use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use tracing::{debug, info};

const PREFETCH_BUFFER_SIZE: usize = 8;

use crate::partition::PartitionBuffer;

/// Key for partition lookup: (shuffle_id, reducer_id).
type PartitionKey = (String, u32);

/// Central coordinator that routes incoming batches to per-reducer partition
/// buffers. Thread-safe via fine-grained per-partition `RwLock`.
pub struct ShuffleManager {
    partitions: RwLock<HashMap<PartitionKey, Arc<RwLock<PartitionBuffer>>>>,
    spill_threshold_bytes: usize,
    spill_dir: PathBuf,
}

impl ShuffleManager {
    pub fn new(spill_threshold_bytes: usize, spill_dir: PathBuf) -> Self {
        info!(
            spill_threshold_bytes,
            spill_dir = %spill_dir.display(),
            "ShuffleManager created"
        );
        Self {
            partitions: RwLock::new(HashMap::new()),
            spill_threshold_bytes,
            spill_dir,
        }
    }

    /// Push a RecordBatch for a given (shuffle_id, reducer_id).
    /// Creates the partition buffer on first access.
    pub fn push_batch(
        &self,
        shuffle_id: &str,
        reducer_id: u32,
        batch: RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = (shuffle_id.to_string(), reducer_id);

        let buf = {
            // Try read lock first to check existence
            let partitions = self.partitions.read().map_err(|e| format!("Lock error: {e}"))?;
            if let Some(buf) = partitions.get(&key) {
                buf.clone()
            } else {
                drop(partitions);
                // Acquire write lock to insert new Arc
                let mut partitions = self.partitions.write().map_err(|e| format!("Lock error: {e}"))?;
                // Double-check under write lock
                partitions
                    .entry(key.clone())
                    .or_insert_with(|| {
                        debug!(shuffle_id, reducer_id, "Creating new partition buffer");
                        Arc::new(RwLock::new(PartitionBuffer::new(
                            shuffle_id.to_string(),
                            reducer_id,
                            self.spill_threshold_bytes,
                            self.spill_dir.clone(),
                        )))
                    })
                    .clone()
            }
        };

        // Lock only this partition buffer for writing and push
        let mut buf_guard = buf.write().map_err(|e| format!("Partition lock error: {e}"))?;
        buf_guard.push_batch(batch).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Push failed: {e}").into()
        })?;
        Ok(())
    }

    /// Read all batches for a (shuffle_id, reducer_id) partition.
    /// Returns spilled + in-memory batches in order.
    pub fn read_partition(
        &self,
        shuffle_id: &str,
        reducer_id: u32,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let key = (shuffle_id.to_string(), reducer_id);
        
        let buf = {
            let partitions = self.partitions.read().map_err(|e| format!("Lock error: {e}"))?;
            partitions.get(&key).cloned()
        };

        match buf {
            Some(buf) => {
                let buf_guard = buf.read().map_err(|e| format!("Partition lock error: {e}"))?;
                let batches = buf_guard.read_all().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("Read failed: {e}").into()
                })?;
                info!(
                    shuffle_id,
                    reducer_id,
                    num_batches = batches.len(),
                    total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    "Partition read complete"
                );
                Ok(batches)
            }
            None => {
                debug!(shuffle_id, reducer_id, "Partition not found, returning empty");
                Ok(Vec::new())
            }
        }
    }

    /// Read all batches for a (shuffle_id, reducer_id) partition lazily.
    /// Returns the schema and a stream of FlightData-compatible batches.
    pub fn stream_partition(
        &self,
        shuffle_id: &str,
        reducer_id: u32,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        RxStream<Result<RecordBatch, arrow_flight::error::FlightError>>,
    ), tonic::Status> {
        let key = (shuffle_id.to_string(), reducer_id);

        let buf = {
            let partitions = self.partitions.read().map_err(|e| {
                tonic::Status::internal(format!("Lock error: {e}"))
            })?;
            partitions
                .get(&key)
                .cloned()
                .ok_or_else(|| tonic::Status::not_found("Partition not found"))?
        };

        let buf_guard = buf.read().map_err(|e| {
            tonic::Status::internal(format!("Partition lock error: {e}"))
        })?;

        let schema = buf_guard
            .schema()
            .ok_or_else(|| tonic::Status::not_found("Partition has no schema/data"))?;

        let spill_files = buf_guard.spill_paths().map_err(|e| {
            tonic::Status::internal(format!("Failed to list spill files: {e}"))
        })?;
        let in_memory = buf_guard.in_memory_batches();

        drop(buf_guard);

        let (tx, rx) = tokio::sync::mpsc::channel(PREFETCH_BUFFER_SIZE);

        tokio::task::spawn_blocking(move || {
            // Phase 1: Stream spill files from disk
            for path in spill_files {
                let file = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        let arrow_err = arrow::error::ArrowError::IoError(
                            format!("Failed to open spill file: {e}"),
                            e,
                        );
                        let _ = tx.blocking_send(Err(arrow_flight::error::FlightError::from(arrow_err)));
                        return;
                    }
                };
                let reader = match arrow_ipc::reader::FileReader::try_new(file, None) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(arrow_flight::error::FlightError::from(e)));
                        return;
                    }
                };
                for batch_result in reader {
                    match batch_result {
                        Ok(batch) => {
                            if tx.blocking_send(Ok(batch)).is_err() {
                                return; // channel closed
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(arrow_flight::error::FlightError::from(e)));
                            return;
                        }
                    }
                }
            }

            // Phase 2: Stream in-memory batches
            for batch in in_memory {
                if tx.blocking_send(Ok(batch)).is_err() {
                    return; // channel closed
                }
            }
        });

        Ok((schema, RxStream::new(rx)))
    }

    /// Get the schema for a partition (if it exists and has data).
    pub fn get_schema(
        &self,
        shuffle_id: &str,
        reducer_id: u32,
    ) -> Option<arrow::datatypes::SchemaRef> {
        let key = (shuffle_id.to_string(), reducer_id);
        let buf = {
            let partitions = self.partitions.read().ok()?;
            partitions.get(&key).cloned()?
        };
        let buf_guard = buf.read().ok()?;
        buf_guard.schema()
    }

    /// List all registered (shuffle_id, reducer_id) pairs.
    pub fn list_partitions(&self) -> Vec<(String, u32)> {
        let partitions = self.partitions.read().unwrap();
        partitions.keys().cloned().collect()
    }

    /// Clean up all partitions for a given shuffle_id.
    pub fn cleanup_shuffle(&self, shuffle_id: &str) {
        let keys_to_remove: Vec<PartitionKey> = {
            let partitions = self.partitions.read().unwrap();
            partitions
                .keys()
                .filter(|(sid, _)| sid == shuffle_id)
                .cloned()
                .collect()
        };

        for key in &keys_to_remove {
            let buf = {
                let partitions = self.partitions.read().unwrap();
                partitions.get(key).cloned()
            };
            if let Some(buf) = buf {
                let buf_guard = buf.read().unwrap();
                let _ = buf_guard.cleanup();
            }
        }

        let mut partitions = self.partitions.write().unwrap();
        for key in keys_to_remove {
            partitions.remove(&key);
        }
        info!(shuffle_id, "Shuffle cleaned up");
    }
}

/// A helper stream that wraps a Tokio MPSC receiver.
pub struct RxStream<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> RxStream<T> {
    pub fn new(rx: tokio::sync::mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T> futures::Stream for RxStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_batch(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![n])),
                Arc::new(StringArray::from(vec![format!("v{n}")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_push_and_read() {
        let tmp = TempDir::new().unwrap();
        let mgr = ShuffleManager::new(1024 * 1024, tmp.path().to_path_buf());

        mgr.push_batch("s1", 0, test_batch(1)).unwrap();
        mgr.push_batch("s1", 0, test_batch(2)).unwrap();
        mgr.push_batch("s1", 1, test_batch(3)).unwrap();

        let batches_0 = mgr.read_partition("s1", 0).unwrap();
        assert_eq!(batches_0.len(), 2);

        let batches_1 = mgr.read_partition("s1", 1).unwrap();
        assert_eq!(batches_1.len(), 1);

        let batches_empty = mgr.read_partition("s1", 99).unwrap();
        assert!(batches_empty.is_empty());
    }

    #[test]
    fn test_list_partitions() {
        let tmp = TempDir::new().unwrap();
        let mgr = ShuffleManager::new(1024 * 1024, tmp.path().to_path_buf());

        mgr.push_batch("s1", 0, test_batch(1)).unwrap();
        mgr.push_batch("s1", 1, test_batch(2)).unwrap();
        mgr.push_batch("s2", 0, test_batch(3)).unwrap();

        let mut parts = mgr.list_partitions();
        parts.sort();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_concurrent_pushes() {
        let tmp = TempDir::new().unwrap();
        let mgr = Arc::new(ShuffleManager::new(1024 * 1024, tmp.path().to_path_buf()));
        let mut handles = vec![];

        // Spawn 10 threads, each pushing to its own reducer partition
        for i in 0..10 {
            let mgr_clone = mgr.clone();
            let handle = std::thread::spawn(move || {
                for batch_idx in 0..5 {
                    mgr_clone.push_batch("s1", i, test_batch(batch_idx)).unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all partitions have exactly 5 batches
        for i in 0..10 {
            let batches = mgr.read_partition("s1", i).unwrap();
            assert_eq!(batches.len(), 5);
        }
    }

    #[tokio::test]
    async fn test_lazy_stream() {
        use futures::StreamExt;
        let tmp = TempDir::new().unwrap();
        // 1 byte threshold to force spilling
        let mgr = ShuffleManager::new(1, tmp.path().to_path_buf());

        mgr.push_batch("s1", 0, test_batch(1)).unwrap();
        mgr.push_batch("s1", 0, test_batch(2)).unwrap();

        let (schema, mut stream) = mgr.stream_partition("s1", 0).unwrap();
        assert_eq!(schema.fields().len(), 2);

        let mut results = vec![];
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            results.push(batch);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(results[1].num_rows(), 1);
    }
}
