use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use shuffle_server::{MemoryManager, ShuffleFlightService, ShuffleManager};

#[derive(Parser, Debug)]
#[command(name = "shuffle-server")]
#[command(about = "Arrow Flight shuffle service for Apache Spark")]
struct Args {
    /// Port to listen on
    #[arg(long, default_value_t = 50051)]
    port: u16,

    /// Directory for spill files
    #[arg(long, default_value = "/tmp/shuffle-spill")]
    spill_dir: PathBuf,

    /// Memory threshold per partition before spilling to disk (in MB)
    #[arg(long, default_value_t = 64)]
    spill_threshold_mb: usize,

    /// Global memory limit for all partition buffers (in MB)
    #[arg(long, default_value_t = 1024)]
    global_memory_limit_mb: usize,

    /// High watermark percentage of global memory limit; triggers background spills
    #[arg(long, default_value_t = 85)]
    global_memory_high_watermark_pct: u8,

    /// Low watermark percentage of global memory limit; spilling stops when usage falls below this
    #[arg(long, default_value_t = 60)]
    global_memory_low_watermark_pct: u8,

    /// Hard limit percentage of global memory limit; applies backpressure to mappers
    #[arg(long, default_value_t = 95)]
    global_memory_hard_limit_pct: u8,

    /// Time-to-live for a shuffle's data in seconds before automatic cleanup (0 to disable)
    #[arg(long, default_value_t = 7200)]
    shuffle_ttl_secs: u64,

    /// How frequently to check for expired shuffles (in seconds)
    #[arg(long, default_value_t = 60)]
    shuffle_cleanup_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let spill_threshold_bytes = args.spill_threshold_mb * 1024 * 1024;
    let global_memory_limit_bytes = args.global_memory_limit_mb * 1024 * 1024;

    let memory_manager = Arc::new(MemoryManager::new(
        global_memory_limit_bytes,
        args.global_memory_high_watermark_pct,
        args.global_memory_low_watermark_pct,
        args.global_memory_hard_limit_pct,
    ));

    info!(
        global_memory_limit_mb = args.global_memory_limit_mb,
        high_watermark_pct = args.global_memory_high_watermark_pct,
        low_watermark_pct = args.global_memory_low_watermark_pct,
        hard_limit_pct = args.global_memory_hard_limit_pct,
        "Global memory manager configured"
    );

    let manager = Arc::new(ShuffleManager::new(
        spill_threshold_bytes,
        args.spill_dir,
        memory_manager,
    ));

    // Spawn the background TTL cleanup task if TTL is enabled (>0)
    if args.shuffle_ttl_secs > 0 {
        let manager_clone = manager.clone();
        let ttl = std::time::Duration::from_secs(args.shuffle_ttl_secs);
        let interval = std::time::Duration::from_secs(args.shuffle_cleanup_interval_secs);
        info!(
            ttl_secs = args.shuffle_ttl_secs,
            interval_secs = args.shuffle_cleanup_interval_secs,
            "Spawning background shuffle TTL cleanup task"
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                manager_clone.cleanup_expired_shuffles(ttl);
            }
        });
    }

    let service = ShuffleFlightService::new(manager);
    let svc = FlightServiceServer::new(service);

    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    info!("Shuffle server listening on {addr}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
