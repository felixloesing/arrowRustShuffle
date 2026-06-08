use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use arrow::array::RecordBatch;
use tracing::{debug, info};

use crate::partition::PartitionBuffer;

/// Key for partition lookup: (shuffle_id, reducer_id).
type PartitionKey = (String, u32);

/// Central coordinator that routes incoming batches to per-reducer partition
/// buffers. Thread-safe via `RwLock`.
pub struct ShuffleManager {
    partitions: RwLock<HashMap<PartitionKey, PartitionBuffer>>,
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

        // Try read lock first to check existence
        {
            let partitions = self.partitions.read().map_err(|e| format!("Lock error: {e}"))?;
            if let Some(_) = partitions.get(&key) {
                drop(partitions);
                // Acquire write lock to push
                let mut partitions = self.partitions.write().map_err(|e| format!("Lock error: {e}"))?;
                if let Some(buf) = partitions.get_mut(&key) {
                    buf.push_batch(batch).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("Push failed: {e}").into()
                    })?;
                    return Ok(());
                }
            }
        }

        // Key doesn't exist — create new buffer under write lock
        let mut partitions = self.partitions.write().map_err(|e| format!("Lock error: {e}"))?;

        // Double-check after acquiring write lock
        if !partitions.contains_key(&key) {
            debug!(shuffle_id, reducer_id, "Creating new partition buffer");
            let buf = PartitionBuffer::new(
                shuffle_id.to_string(),
                reducer_id,
                self.spill_threshold_bytes,
                self.spill_dir.clone(),
            );
            partitions.insert(key.clone(), buf);
        }

        let buf = partitions.get_mut(&key).unwrap();
        buf.push_batch(batch).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
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
        let partitions = self.partitions.read().map_err(|e| format!("Lock error: {e}"))?;

        match partitions.get(&key) {
            Some(buf) => {
                let batches = buf.read_all().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
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

    /// Get the schema for a partition (if it exists and has data).
    pub fn get_schema(
        &self,
        shuffle_id: &str,
        reducer_id: u32,
    ) -> Option<arrow::datatypes::SchemaRef> {
        let key = (shuffle_id.to_string(), reducer_id);
        let partitions = self.partitions.read().ok()?;
        partitions.get(&key)?.schema()
    }

    /// List all registered (shuffle_id, reducer_id) pairs.
    pub fn list_partitions(&self) -> Vec<(String, u32)> {
        let partitions = self.partitions.read().unwrap();
        partitions.keys().cloned().collect()
    }

    /// Clean up all partitions for a given shuffle_id.
    pub fn cleanup_shuffle(&self, shuffle_id: &str) {
        let mut partitions = self.partitions.write().unwrap();
        let keys_to_remove: Vec<PartitionKey> = partitions
            .keys()
            .filter(|(sid, _)| sid == shuffle_id)
            .cloned()
            .collect();

        for key in &keys_to_remove {
            if let Some(buf) = partitions.get(key) {
                let _ = buf.cleanup();
            }
        }
        for key in keys_to_remove {
            partitions.remove(&key);
        }
        info!(shuffle_id, "Shuffle cleaned up");
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
}
