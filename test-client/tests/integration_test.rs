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

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None, None, None).await;

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
    let port = start_test_server(1024 * 1024, tmp_dir.path().to_path_buf(), None, None, None).await;

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

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None, None, None).await;

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

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), global_memory_limit, None, None).await;

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

/// Test the CleanShuffle DoAction explicitly.
#[tokio::test]
async fn test_explicit_clean_shuffle_action() {
    use futures::{StreamExt, TryStreamExt};
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let spill_threshold = 4 * 1024; // 4 KB to force spilling

    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None, None, None).await;

    let shuffle_id = "clean_test_shuffle";

    // Push some data to trigger partition buffer creation and disk spilling
    let mut client = connect_client(port).await;
    run_mapper(&mut client, shuffle_id, 0, 2, 100, 2).await;

    // Verify spill directories exist
    let spill_base = tmp_dir.path().join(shuffle_id);
    assert!(spill_base.exists(), "Spill directory should exist before clean");

    // Send CleanShuffle action
    let action = arrow_flight::Action {
        r#type: "CleanShuffle".to_string(),
        body: shuffle_id.as_bytes().to_vec().into(),
    };

    let response = client.do_action(action).await.unwrap();
    let mut stream = response.into_inner();

    // Read response from action
    let mut results = Vec::new();
    while let Some(res) = stream.next().await {
        let action_res = res.unwrap();
        results.push(String::from_utf8_lossy(&action_res.body).into_owned());
    }

    assert_eq!(results.len(), 1);
    assert!(results[0].contains("Successfully cleaned shuffle"));

    // Verify spill directories are cleaned up
    assert!(!spill_base.exists(), "Spill directory should have been deleted");

    // Verify that retrieving the partition now returns empty or nonexistent
    // Since the partitions map was cleared, getting schema or get_flight_info will fail/not find it.
    let list_response = client.list_flights(arrow_flight::Criteria::default()).await.unwrap();
    let flights: Vec<_> = list_response.into_inner().try_collect().await.unwrap();

    // Verify none of the flights have this shuffle_id
    for flight in flights {
        let descriptor = flight.flight_descriptor.unwrap();
        assert_ne!(descriptor.path[0], shuffle_id);
    }
}

/// Test the background TTL cleanup task.
#[tokio::test]
async fn test_background_ttl_cleanup() {
    use futures::TryStreamExt;
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let spill_threshold = 4 * 1024; // 4 KB to force spilling

    // Server started with a 1 second TTL and a 1 second cleanup check interval
    let port = start_test_server(spill_threshold, tmp_dir.path().to_path_buf(), None, Some(1), Some(1)).await;

    let shuffle_id = "ttl_test_shuffle";

    // Push some data to trigger partition buffer creation and disk spilling
    let mut client = connect_client(port).await;
    run_mapper(&mut client, shuffle_id, 0, 2, 100, 2).await;

    // Verify spill directories exist
    let spill_base = tmp_dir.path().join(shuffle_id);
    assert!(spill_base.exists(), "Spill directory should exist before expiration");

    // Wait for TTL (1 sec) + check interval (1 sec) to guarantee expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(2500)).await;

    // Verify spill directories are cleaned up automatically
    assert!(!spill_base.exists(), "Spill directory should have been cleaned up by TTL");

    // Verify list_flights is empty for this shuffle
    let list_response = client.list_flights(arrow_flight::Criteria::default()).await.unwrap();
    let flights: Vec<_> = list_response.into_inner().try_collect().await.unwrap();
    for flight in flights {
        let descriptor = flight.flight_descriptor.unwrap();
        assert_ne!(descriptor.path[0], shuffle_id);
    }
}

/// Test that pushing a RecordBatch with a mismatched schema fails with InvalidArgument status.
#[tokio::test]
async fn test_schema_compatibility_mismatch() {
    use futures::StreamExt;
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = TempDir::new().unwrap();
    let port = start_test_server(1024 * 1024, tmp_dir.path().to_path_buf(), None, None, None).await;

    let shuffle_id = "schema_mismatch_shuffle";
    let reducer_id = 0u32;

    let mut client = connect_client(port).await;

    // 1. Push a valid batch with the standard test schema
    let batch1 = test_client::generate_test_batch(reducer_id as i32, 10);
    let descriptor = arrow_flight::FlightDescriptor::new_path(vec![
        shuffle_id.to_string(),
        reducer_id.to_string(),
    ]);

    let schema1 = test_client::test_schema();
    let flight_data_stream1 = arrow_flight::encode::FlightDataEncoderBuilder::new()
        .with_schema(schema1)
        .with_flight_descriptor(Some(descriptor.clone()))
        .build(futures::stream::iter(vec![Ok(batch1)]))
        .map(|r| r.unwrap());

    let put_res = client.do_put(flight_data_stream1).await;
    assert!(put_res.is_ok(), "First push with valid schema should succeed");

    // 2. Build a mismatched RecordBatch schema (e.g., key column is Utf8 instead of Int32)
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let mismatched_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false), // different type
        Field::new("value", DataType::Utf8, false),
        Field::new("partition_id", DataType::Int32, false),
    ]));

    let keys = StringArray::from(vec!["not_an_int"; 10]);
    let values = StringArray::from(vec!["val"; 10]);
    let partitions = arrow::array::Int32Array::from(vec![reducer_id as i32; 10]);

    let batch2 = arrow::record_batch::RecordBatch::try_new(
        mismatched_schema.clone(),
        vec![
            Arc::new(keys),
            Arc::new(values),
            Arc::new(partitions),
        ],
    )
    .unwrap();

    let flight_data_stream2 = arrow_flight::encode::FlightDataEncoderBuilder::new()
        .with_schema(mismatched_schema)
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::iter(vec![Ok(batch2)]))
        .map(|r| r.unwrap());

    // 3. Attempt to push the mismatched batch and verify it fails with InvalidArgument
    let response_err = client.do_put(flight_data_stream2).await.unwrap_err();
    assert_eq!(response_err.code(), tonic::Code::InvalidArgument);
    assert!(
        response_err.message().contains("Schema mismatch"),
        "Expected error message to contain 'Schema mismatch', got '{}'",
        response_err.message()
    );
}

