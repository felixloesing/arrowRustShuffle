use tempfile::TempDir;

use test_client::{connect_client, run_mapper, run_reducer, start_test_server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Arrow Flight Shuffle Service Demo ===\n");

    let tmp_dir = TempDir::new()?;
    println!("Spill directory: {}", tmp_dir.path().display());
    let spill_threshold = 4 * 1024; // 4 KB — low threshold to demonstrate spilling

    // Start the embedded shuffle server
    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf()).await;
    println!("Server started on port {port}");

    let shuffle_id = "demo_shuffle_1";
    let num_mappers = 4u32;
    let num_reducers = 3u32;
    let rows_per_batch = 100;
    let batches_per_reducer = 3;

    // Phase 1: Mappers push data
    println!("\n--- Phase 1: Mappers pushing data ---");
    let mut total_rows_sent: u64 = 0;
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
        println!(
            "  Mapper {mapper_id}: sent {rows} rows across {num_reducers} reducers"
        );
        total_rows_sent += rows;
    }
    println!("Total rows sent by all mappers: {total_rows_sent}");

    // Phase 2: Reducers fetch data
    println!("\n--- Phase 2: Reducers fetching data ---");
    let expected_rows_per_reducer =
        (num_mappers as usize) * batches_per_reducer * rows_per_batch;
    let mut total_rows_received = 0usize;

    for reducer_id in 0..num_reducers {
        let mut client = connect_client(port).await;
        let (num_batches, num_rows) =
            run_reducer(&mut client, shuffle_id, reducer_id).await;
        println!(
            "  Reducer {reducer_id}: received {num_rows} rows in {num_batches} batches"
        );
        total_rows_received += num_rows;

        assert_eq!(
            num_rows, expected_rows_per_reducer,
            "Reducer {reducer_id} row count mismatch!"
        );
    }

    // Validation
    println!("\n--- Validation ---");
    assert_eq!(
        total_rows_received,
        total_rows_sent as usize,
        "Total rows mismatch!"
    );
    println!("✓ Total rows match: {total_rows_received}");
    println!(
        "✓ Each reducer got exactly {expected_rows_per_reducer} rows"
    );
    println!("\n=== PASS ===");

    Ok(())
}
