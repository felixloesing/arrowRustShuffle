use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use shuffle_server::{ShuffleFlightService, ShuffleManager};

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let spill_threshold_bytes = args.spill_threshold_mb * 1024 * 1024;

    let manager = Arc::new(ShuffleManager::new(spill_threshold_bytes, args.spill_dir));

    let service = ShuffleFlightService::new(manager);
    let svc = FlightServiceServer::new(service);

    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    info!("Shuffle server listening on {addr}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
