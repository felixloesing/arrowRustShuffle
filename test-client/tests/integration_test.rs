use tempfile::TempDir;

use test_client::{connect_client, run_mapper, run_reducer, start_test_server};

/// Full integration test:
///   1. Start an in-process server with a low spill threshold (4 KB)
///   2. 4 mappers each push 3 batches of 100 rows to 3 reducers
///   3. Each reducer fetches and validates its partition
///   4. Assert row counts and that spill files were created
#[tokio::test]
async fn test_full_shuffle_round_trip() {
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let spill_threshold = 4 * 1024; // 4 KB to force spilling

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None).await;

    let shuffle_id = "test_shuffle_1";
    let num_mappers = 4u32;
    let num_reducers = 3u32;
    let rows_per_batch = 100;
    let batches_per_reducer = 3;

    // --- Mapper phase ---
    let mut total_rows_pushed = 0u64;
    for mapper_id in 0..num_mappers {
        let mut client = connect_client(port).await;
        let rows = run_mapper(
            &mut client,
            shuffle_id,
            mapper_id,
            num_reducers,
            rows_per_batch,
            batches_per_reducer,
        )
        .await;
        total_rows_pushed += rows;
    }

    let expected_total = (num_mappers as u64)
        * (num_reducers as u64)
        * (batches_per_reducer as u64)
        * (rows_per_batch as u64);
    assert_eq!(total_rows_pushed, expected_total);

    // --- Reducer phase ---
    let expected_per_reducer =
        (num_mappers as usize) * batches_per_reducer * rows_per_batch;
    let mut total_rows_received = 0usize;

    for reducer_id in 0..num_reducers {
        let mut client = connect_client(port).await;
        let (num_batches, num_rows) =
            run_reducer(&mut client, shuffle_id, reducer_id).await;

        assert_eq!(
            num_rows, expected_per_reducer,
            "Reducer {reducer_id}: expected {expected_per_reducer} rows, got {num_rows}"
        );
        assert!(
            num_batches > 0,
            "Reducer {reducer_id}: expected at least one batch"
        );

        total_rows_received += num_rows;
    }

    assert_eq!(total_rows_received, total_rows_pushed as usize);

    // --- Verify spill files exist ---
    // With a 4 KB threshold and 100-row batches, spilling should have occurred
    let spill_base = tmp_dir.path().join(shuffle_id);
    assert!(
        spill_base.exists(),
        "Spill directory should exist at {:?}",
        spill_base
    );

    let mut arrow_file_count = 0;
    for reducer_id in 0..num_reducers {
        let reducer_dir = spill_base.join(format!("reducer_{reducer_id}"));
        if reducer_dir.exists() {
            for entry in std::fs::read_dir(&reducer_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().extension().map_or(false, |e| e == "arrow") {
                    arrow_file_count += 1;
                }
            }
        }
    }

    assert!(
        arrow_file_count > 0,
        "Expected at least one .arrow spill file, found {arrow_file_count}"
    );
    println!(
        "Found {arrow_file_count} .arrow spill files across all reducer partitions"
    );
}

/// Test that fetching a non-existent partition returns an error.
#[tokio::test]
async fn test_get_nonexistent_partition() {
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let port = start_test_server(1024 * 1024, tmp_dir.path().to_path_buf(), None).await;

    let mut client = connect_client(port).await;
    let ticket = arrow_flight::Ticket::new("nonexistent/42".to_string());
    let result = client.do_get(ticket).await;

    assert!(result.is_err(), "Expected error for nonexistent partition");
}

/// Test with a large spill threshold (no spilling should occur).
#[tokio::test]
async fn test_no_spill_when_threshold_large() {
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let spill_threshold = 100 * 1024 * 1024; // 100 MB

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None).await;

    let shuffle_id = "no_spill_shuffle";
    let mut client = connect_client(port).await;

    run_mapper(&mut client, shuffle_id, 0, 2, 50, 2).await;

    // Verify no spill files were created
    let spill_base = tmp_dir.path().join(shuffle_id);
    assert!(
        !spill_base.exists(),
        "No spill directory should exist with large threshold"
    );

    // But data should still be readable
    for reducer_id in 0..2u32 {
        let mut client = connect_client(port).await;
        let (num_batches, num_rows) =
            run_reducer(&mut client, shuffle_id, reducer_id).await;
        assert_eq!(num_rows, 100); // 1 mapper × 2 batches × 50 rows
        assert_eq!(num_batches, 2);
    }
}

/// Test the global memory manager's ability to trigger cross-partition spills.
///
/// Strategy: use a very high per-partition spill threshold (so partitions
/// do NOT spill individually), but a tiny global memory limit (e.g. 8 KB)
/// so the MemoryManager is forced to trigger global spills across partitions.
/// Then verify that all data is still correctly transmitted to reducers.
#[tokio::test]
async fn test_global_memory_manager_spill() {
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    // Per-partition threshold very high — partitions won't self-spill
    let spill_threshold = 100 * 1024 * 1024; // 100 MB
    // Global limit very low — forces the memory manager to trigger spills
    let global_memory_limit = Some(8 * 1024); // 8 KB

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), global_memory_limit).await;

    let shuffle_id = "global_mem_test";
    let num_mappers = 2u32;
    let num_reducers = 3u32;
    let rows_per_batch = 100;
    let batches_per_reducer = 3;

    // --- Mapper phase ---
    let mut total_rows_pushed = 0u64;
    for mapper_id in 0..num_mappers {
        let mut client = connect_client(port).await;
        let rows = run_mapper(
            &mut client,
            shuffle_id,
            mapper_id,
            num_reducers,
            rows_per_batch,
            batches_per_reducer,
        )
        .await;
        total_rows_pushed += rows;
    }

    let expected_total = (num_mappers as u64)
        * (num_reducers as u64)
        * (batches_per_reducer as u64)
        * (rows_per_batch as u64);
    assert_eq!(total_rows_pushed, expected_total);

    // --- Reducer phase: verify all data is retrievable despite global spills ---
    let expected_per_reducer =
        (num_mappers as usize) * batches_per_reducer * rows_per_batch;
    let mut total_rows_received = 0usize;

    for reducer_id in 0..num_reducers {
        let mut client = connect_client(port).await;
        let (num_batches, num_rows) =
            run_reducer(&mut client, shuffle_id, reducer_id).await;

        assert_eq!(
            num_rows, expected_per_reducer,
            "Reducer {reducer_id}: expected {expected_per_reducer} rows, got {num_rows}"
        );
        assert!(
            num_batches > 0,
            "Reducer {reducer_id}: expected at least one batch"
        );

        total_rows_received += num_rows;
    }

    assert_eq!(total_rows_received, total_rows_pushed as usize);

    // --- Verify that global spills occurred ---
    // With only 8 KB global limit, spill files must have been created
    let spill_base = tmp_dir.path().join(shuffle_id);
    assert!(
        spill_base.exists(),
        "Spill directory should exist due to global memory pressure: {:?}",
        spill_base
    );

    let mut arrow_file_count = 0;
    for reducer_id in 0..num_reducers {
        let reducer_dir = spill_base.join(format!("reducer_{reducer_id}"));
        if reducer_dir.exists() {
            for entry in std::fs::read_dir(&reducer_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().extension().map_or(false, |e| e == "arrow") {
                    arrow_file_count += 1;
                }
            }
        }
    }

    assert!(
        arrow_file_count > 0,
        "Expected global memory manager to trigger spills, found {arrow_file_count} .arrow files"
    );
    println!(
        "Global memory manager test: {arrow_file_count} spill files created across {num_reducers} reducer partitions"
    );
}

