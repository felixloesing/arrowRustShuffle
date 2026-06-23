# Arrow Rust Shuffle Service

A high-performance, **external shuffle service** written in Rust, leveraging **Apache Arrow Flight** (built on gRPC/Tonic) for zero-copy data transport.

Inspired by modern push-based shuffle systems like [Apache Celeborn](https://github.com/apache/celeborn), this service aggregates incoming mapper data **on-the-fly** by Reducer ID. This allows reducers to fetch a single consolidated stream directly from the server, eliminating the need to query multiple mapper nodes or merge hundreds of small files.

---

## 📖 Architecture & Design

In traditional pull-based shuffles, $M$ mappers write partition files locally on their disk. A job with $M$ mappers and $R$ reducers creates $M \times R$ logical partitions, producing excessive random I/O and small files.

This service utilizes a **push-based shuffle** design:
1. **Push Phase**: Mappers push their `RecordBatch` streams directly to the shuffle service using Arrow Flight's `DoPut` RPC, targeting a specific `(shuffle_id, reducer_id)` partition.
2. **Aggregation & Spilling**: The server aggregates batches for each `reducer_id` in-memory. If a partition's memory footprint exceeds the configured threshold, the server spills the data as a consolidated `.arrow` IPC file to disk.
3. **Global Memory Management**: A central `MemoryManager` tracks global memory usage across all partitions. When usage crosses the high watermark, it triggers background spills of the largest partitions. If usage hits the hard limit, backpressure is applied to mappers until memory drops below the low watermark.
4. **Fetch Phase**: Reducers fetch their aggregated stream using Arrow Flight's `DoGet` RPC, which streams the spilled files followed by any remaining in-memory batches.

```mermaid
graph LR
    M1[Mapper 0] -->|DoPut| S["Shuffle Server<br/>(Arrow Flight)"]
    M2[Mapper 1] -->|DoPut| S
    M3[Mapper N] -->|DoPut| S
    S -->|DoGet| R1[Reducer 0]
    S -->|DoGet| R2[Reducer 1]
    S -->|DoGet| R3[Reducer M]

    subgraph Shuffle Server
        S --> SM[ShuffleManager]
        SM --> MM[MemoryManager]
        SM --> PB0["PartitionBuffer<br/>reducer_0"]
        SM --> PB1["PartitionBuffer<br/>reducer_1"]
        SM --> PBM["PartitionBuffer<br/>reducer_M"]
    end
```

---

## 📂 Project Structure

This workspace contains two crates:

- **shuffle-server**: The core external shuffle service.
- **test-client**: A simulator client and integration test suite.

```
arrowRustShuffle/
├── Cargo.toml                  # Workspace Root
├── shuffle-server/             # Shuffle Server Crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs             # Server entry point & CLI parser
│       ├── lib.rs              # Re-exports for test usage
│       ├── memory_manager.rs   # Global memory tracking & watermarks
│       ├── partition.rs        # PartitionBuffer logic (in-memory buffering + spilling)
│       ├── shuffle_manager.rs  # ShuffleManager (keys routing and management)
│       └── flight_service.rs   # Arrow Flight Service implementation
└── test-client/                # Test Client Crate
    ├── Cargo.toml
    ├── src/
    │   ├── main.rs             # Standalone client/server demo
    │   └── lib.rs              # Simulator helpers (mappers & reducers)
    └── tests/
        └── integration_test.rs # End-to-end integration tests
```

---

## ⚡ Core Components

### 1. PartitionBuffer
Manages buffering and disk spilling for a single `(shuffle_id, reducer_id)` partition.
- Accumulates `RecordBatch`es in memory.
- Dynamically tracks buffer memory footprint via `RecordBatch::get_array_memory_size`.
- Spills to `.arrow` IPC files in the directory `spill_dir/{shuffle_id}/reducer_{id}/spill_{epoch}.arrow` when the memory threshold is reached.
- Streams back merged data in sequential order (spill files first, then remaining memory buffer).
- Reports memory usage to the global `MemoryManager` on push, spill, and drop.

### 2. MemoryManager
A centralized, lock-free memory tracker shared across all partitions.
- Uses `AtomicUsize` for contention-free global memory accounting.
- Three configurable watermark thresholds control behavior:
  - **High watermark** (default 85%): Triggers background spills of the largest partitions.
  - **Low watermark** (default 60%): Spilling continues until usage drops below this level.
  - **Hard limit** (default 95%): Applies backpressure — mapper `DoPut` calls are paused until memory is freed.
- Prevents OOM by ensuring total memory usage stays bounded regardless of the number of active partitions.

### 3. ShuffleManager
A thread-safe entry point that maps and routes incoming `RecordBatch` streams to the corresponding [PartitionBuffer](shuffle-server/src/partition.rs) based on `(shuffle_id, reducer_id)`.
- Utilizes fine-grained concurrency with per-partition `Arc<RwLock<PartitionBuffer>>` locking.
- Employs a double-checked locking pattern for high-performance concurrent writes, allowing parallel mapper pushes and ensuring that disk spilling on one partition does not block writes or reads on other partitions.
- Enforces global memory limits before each push by delegating to `MemoryManager`.

### 4. ShuffleFlightService
Implements the gRPC-based `FlightService` from the `arrow-flight` crate:

| RPC Method | Descriptor/Ticket Format | Purpose |
| :--- | :--- | :--- |
| `DoPut` | Path: `[shuffle_id, reducer_id]` | Mappers upload partition batches |
| `DoGet` | Ticket: `"{shuffle_id}/{reducer_id}"` | Reducers stream merged partition data |
| `GetFlightInfo` | Path: `[shuffle_id, reducer_id]` | Query partition metadata and endpoint info |
| `ListFlights` | None | Listing all registered shuffle partitions |

---

## 🚀 Getting Started

### Prerequisites

Ensure you have Rust and Cargo installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Running the Shuffle Server

To start the standalone shuffle service:
```bash
cargo run -p shuffle-server -- --port 50051 --spill-dir /tmp/shuffle-spill --spill-threshold-mb 64
```

#### CLI Configuration Options:
| Flag | Description | Default |
| :--- | :--- | :--- |
| `--port` | Port to run the server on | `50051` |
| `--spill-dir` | Location where spilled `.arrow` files will be written | `/tmp/shuffle-spill` |
| `--spill-threshold-mb` | Max memory (MB) for a single partition before spilling to disk | `64` |
| `--global-memory-limit-mb` | Total memory budget (MB) for all partition buffers combined | `1024` |
| `--global-memory-high-watermark-pct` | % of global limit that triggers background spills | `85` |
| `--global-memory-low-watermark-pct` | % of global limit below which spilling stops | `60` |
| `--global-memory-hard-limit-pct` | % of global limit that triggers mapper backpressure | `95` |

#### Example with memory limits:
```bash
cargo run -p shuffle-server -- \
  --port 50051 \
  --spill-dir /tmp/shuffle-spill \
  --spill-threshold-mb 64 \
  --global-memory-limit-mb 2048 \
  --global-memory-high-watermark-pct 80 \
  --global-memory-low-watermark-pct 50 \
  --global-memory-hard-limit-pct 90
```

### Running the Demo Client

The demo client starts an embedded shuffle server with a very low spill threshold (`4 KB`) to show the transition between in-memory buffering and disk spilling, runs 4 simulated mappers pushing data, and 3 simulated reducers fetching it back:
```bash
cargo run -p test-client
```

### Running Tests

To run all unit and integration tests across the workspace:
```bash
cargo test --workspace -- --nocapture
```

The tests cover:
- In-memory partition buffer lifecycle.
- Spill-to-disk verification and file path checks.
- End-to-end mapper-to-reducer Arrow Flight integration.
- Global memory manager cross-partition spill triggering.

