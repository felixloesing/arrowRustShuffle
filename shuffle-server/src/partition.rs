use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use tracing::{debug, info};

use crate::memory_manager::MemoryManager;

/// A buffer for a single (shuffle_id, reducer_id) partition.
///
/// Incoming RecordBatches are held in memory. When the memory footprint
/// exceeds `spill_threshold_bytes`, the buffer is flushed to an Arrow IPC
/// file on disk. On read, spilled files are streamed first (in epoch order),
/// followed by any remaining in-memory batches.
pub struct PartitionBuffer {
    pub shuffle_id: String,
    pub reducer_id: u32,
    schema: Option<SchemaRef>,
    in_memory: Vec<RecordBatch>,
    pub memory_bytes: usize,
    spill_threshold_bytes: usize,
    spill_dir: PathBuf,
    epoch: AtomicUsize,
    memory_manager: Arc<MemoryManager>,
}

impl PartitionBuffer {
    pub fn new(
        shuffle_id: String,
        reducer_id: u32,
        spill_threshold_bytes: usize,
        spill_dir: PathBuf,
        memory_manager: Arc<MemoryManager>,
    ) -> Self {
        Self {
            shuffle_id,
            reducer_id,
            schema: None,
            in_memory: Vec::new(),
            memory_bytes: 0,
            spill_threshold_bytes,
            spill_dir,
            epoch: AtomicUsize::new(0),
            memory_manager,
        }
    }

    /// Returns the spill directory for this specific partition.
    fn partition_spill_dir(&self) -> PathBuf {
        self.spill_dir
            .join(&self.shuffle_id)
            .join(format!("reducer_{}", self.reducer_id))
    }

    /// Push a RecordBatch into this partition buffer.
    /// If the memory threshold is exceeded after push, spill to disk.
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        // Validate schema compatibility if a schema is already set
        if let Some(ref schema) = self.schema {
            if schema.fields() != batch.schema().fields() {
                return Err("Schema mismatch: incoming batch fields do not match partition schema".into());
            }
        } else {
            // Capture schema from first batch
            self.schema = Some(batch.schema());
        }

        let batch_size = batch.get_array_memory_size();
        self.memory_bytes += batch_size;
        self.in_memory.push(batch);

        // Reserve globally
        self.memory_manager.reserve(batch_size);

        debug!(
            shuffle_id = %self.shuffle_id,
            reducer_id = self.reducer_id,
            memory_bytes = self.memory_bytes,
            threshold = self.spill_threshold_bytes,
            global_used = self.memory_manager.used_bytes(),
            "Pushed batch"
        );

        if self.memory_bytes >= self.spill_threshold_bytes {
            self.spill_to_disk()?;
        }

        Ok(())
    }

    /// Spill current in-memory batches to an Arrow IPC file.
    pub fn spill_to_disk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.in_memory.is_empty() {
            return Ok(());
        }

        let schema = self
            .schema
            .as_ref()
            .ok_or("No schema available for spill")?
            .clone();

        let spill_dir = self.partition_spill_dir();
        fs::create_dir_all(&spill_dir)?;

        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst);
        let spill_path = spill_dir.join(format!("spill_{epoch}.arrow"));

        info!(
            path = %spill_path.display(),
            batches = self.in_memory.len(),
            bytes = self.memory_bytes,
            "Spilling partition to disk"
        );

        let file = File::create(&spill_path)?;
        let buf_writer = BufWriter::new(file);
        let mut writer = FileWriter::try_new(buf_writer, &schema)?;

        for batch in self.in_memory.drain(..) {
            writer.write(&batch)?;
        }
        writer.finish()?;

        // Release globally
        self.memory_manager.release(self.memory_bytes);
        self.memory_bytes = 0;
        Ok(())
    }

    /// Returns a list of all spilled files on disk in epoch order.
    pub fn spill_paths(&self) -> Result<Vec<PathBuf>, std::io::Error> {
        let spill_dir = self.partition_spill_dir();
        if spill_dir.exists() {
            let mut spill_files: Vec<PathBuf> = fs::read_dir(&spill_dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map_or(false, |ext| ext == "arrow"))
                .collect();
            spill_files.sort();
            Ok(spill_files)
        } else {
            Ok(Vec::new())
        }
    }

    /// Returns a copy of the in-memory record batches.
    pub fn in_memory_batches(&self) -> Vec<RecordBatch> {
        self.in_memory.clone()
    }

    /// Read all data for this partition: spilled files first, then in-memory.
    /// Returns batches in the order they were received.
    pub fn read_all(&self) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let mut all_batches = Vec::new();

        // Read spilled files in epoch order
        let spill_files = self.spill_paths()?;
        for spill_path in spill_files {
            debug!(path = %spill_path.display(), "Reading spill file");
            let file = File::open(&spill_path)?;
            let reader = FileReader::try_new(file, None)?;
            for batch_result in reader {
                all_batches.push(batch_result?);
            }
        }

        // Then append in-memory batches
        all_batches.extend(self.in_memory_batches());

        Ok(all_batches)
    }

    /// Get the schema for this partition (if any data has been pushed).
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    /// Get the number of spill files on disk.
    pub fn spill_count(&self) -> usize {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Clean up spill files for this partition.
    pub fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let spill_dir = self.partition_spill_dir();
        if spill_dir.exists() {
            fs::remove_dir_all(&spill_dir)?;
        }
        Ok(())
    }
}

impl Drop for PartitionBuffer {
    fn drop(&mut self) {
        if self.memory_bytes > 0 {
            self.memory_manager.release(self.memory_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef, rows: i32) -> RecordBatch {
        let keys: Vec<i32> = (0..rows).collect();
        let values: Vec<String> = (0..rows).map(|i| format!("val_{i}")).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_push_and_read_in_memory() {
        let tmp = TempDir::new().unwrap();
        let schema = test_schema();
        let mem_mgr = Arc::new(MemoryManager::new(100 * 1024 * 1024, 85, 60, 95));
        let mut buf = PartitionBuffer::new(
            "shuffle_1".into(),
            0,
            1024 * 1024, // 1MB threshold — won't spill
            tmp.path().to_path_buf(),
            mem_mgr,
        );

        buf.push_batch(test_batch(&schema, 10)).unwrap();
        buf.push_batch(test_batch(&schema, 20)).unwrap();

        let batches = buf.read_all().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[1].num_rows(), 20);
        assert_eq!(buf.spill_count(), 0);
    }

    #[test]
    fn test_spill_to_disk() {
        let tmp = TempDir::new().unwrap();
        let schema = test_schema();
        let mem_mgr = Arc::new(MemoryManager::new(100 * 1024 * 1024, 85, 60, 95));
        // Very low threshold to force spilling
        let mut buf = PartitionBuffer::new(
            "shuffle_1".into(),
            0,
            1, // 1 byte threshold — will spill immediately
            tmp.path().to_path_buf(),
            mem_mgr,
        );

        buf.push_batch(test_batch(&schema, 100)).unwrap();
        assert_eq!(buf.spill_count(), 1);
        assert!(buf.in_memory.is_empty());

        buf.push_batch(test_batch(&schema, 50)).unwrap();
        assert_eq!(buf.spill_count(), 2);

        let batches = buf.read_all().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 100);
        assert_eq!(batches[1].num_rows(), 50);
    }
}
