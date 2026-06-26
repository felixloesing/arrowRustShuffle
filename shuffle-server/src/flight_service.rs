use std::sync::Arc;

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, Ticket,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::shuffle_manager::ShuffleManager;

/// Arrow Flight implementation for the shuffle service.
///
/// - `DoPut`: mappers push RecordBatches keyed by (shuffle_id, reducer_id)
/// - `DoGet`: reducers fetch the merged stream for their partition
/// - `GetFlightInfo`: query metadata for a partition
/// - `ListFlights`: list all registered partitions
pub struct ShuffleFlightService {
    manager: Arc<ShuffleManager>,
}

impl ShuffleFlightService {
    pub fn new(manager: Arc<ShuffleManager>) -> Self {
        Self { manager }
    }
}

/// Parse shuffle_id and reducer_id from a FlightDescriptor path.
/// Expects path = ["shuffle_id", "reducer_id"].
fn parse_descriptor(descriptor: &FlightDescriptor) -> Result<(String, u32), Status> {
    if descriptor.path.len() != 2 {
        return Err(Status::invalid_argument(
            "FlightDescriptor path must be [shuffle_id, reducer_id]",
        ));
    }
    let shuffle_id = descriptor.path[0].clone();
    let reducer_id: u32 = descriptor.path[1]
        .parse()
        .map_err(|_| Status::invalid_argument("reducer_id must be a u32"))?;
    Ok((shuffle_id, reducer_id))
}

/// Parse shuffle_id and reducer_id from a Ticket.
/// Expects ticket bytes as UTF-8 string "shuffle_id/reducer_id".
fn parse_ticket(ticket: &Ticket) -> Result<(String, u32), Status> {
    let s = std::str::from_utf8(&ticket.ticket)
        .map_err(|_| Status::invalid_argument("Ticket must be valid UTF-8"))?;
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(Status::invalid_argument(
            "Ticket must be 'shuffle_id/reducer_id'",
        ));
    }
    let shuffle_id = parts[0].to_string();
    let reducer_id: u32 = parts[1]
        .parse()
        .map_err(|_| Status::invalid_argument("reducer_id must be a u32"))?;
    Ok((shuffle_id, reducer_id))
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Handshake — not used, return empty stream.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }

    /// List all registered (shuffle_id, reducer_id) partitions.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let partitions = self.manager.list_partitions();
        let flight_infos: Vec<Result<FlightInfo, Status>> = partitions
            .into_iter()
            .filter_map(|(shuffle_id, reducer_id)| {
                let descriptor = FlightDescriptor::new_path(vec![
                    shuffle_id.clone(),
                    reducer_id.to_string(),
                ]);

                let schema = self.manager.get_schema(&shuffle_id, reducer_id)?;

                Some(Ok(FlightInfo::new()
                    .with_descriptor(descriptor)
                    .try_with_schema(&schema)
                    .ok()?))
            })
            .collect();

        let stream = stream::iter(flight_infos);
        Ok(Response::new(stream.boxed()))
    }

    /// Get metadata about a specific partition.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let (shuffle_id, reducer_id) = parse_descriptor(&descriptor)?;

        let schema = self
            .manager
            .get_schema(&shuffle_id, reducer_id)
            .ok_or_else(|| Status::not_found("Partition not found"))?;

        let info = FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_path(vec![
                shuffle_id,
                reducer_id.to_string(),
            ]))
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema encoding error: {e}")))?;

        Ok(Response::new(info))
    }

    /// Reducer fetches merged data for its partition.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (shuffle_id, reducer_id) = parse_ticket(&ticket)?;

        info!(shuffle_id = %shuffle_id, reducer_id, "DoGet: reducer fetching partition");

        let (schema, batch_stream) = self
            .manager
            .stream_partition(&shuffle_id, reducer_id)?;

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Encode error: {e}")));

        Ok(Response::new(flight_stream.boxed()))
    }

    /// Mapper pushes RecordBatches for a (shuffle_id, reducer_id).
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        // The first message should contain the FlightDescriptor
        let first_msg = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Empty DoPut stream"))?;

        let descriptor = first_msg
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("First message must have FlightDescriptor"))?;

        let (shuffle_id, reducer_id) = parse_descriptor(descriptor)?;

        info!(
            shuffle_id = %shuffle_id,
            reducer_id,
            "DoPut: mapper pushing data"
        );

        // Decode the stream of FlightData into RecordBatches
        // Re-inject the first message into the stream
        let first_stream = stream::once(async { Ok(first_msg) });
        let rest_stream = stream.map_err(|e| FlightError::Tonic(e));
        let combined = first_stream.chain(rest_stream);

        let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(combined);

        let mut batch_count = 0u64;
        let mut row_count = 0u64;

        while let Some(batch_result) = decoder.next().await {
            match batch_result {
                Ok(batch) => {
                    row_count += batch.num_rows() as u64;
                    batch_count += 1;
                    let manager = self.manager.clone();
                    let shuffle_id_clone = shuffle_id.clone();
                    tokio::task::spawn_blocking(move || {
                        manager.push_batch(&shuffle_id_clone, reducer_id, batch)
                    })
                    .await
                    .map_err(|e| Status::internal(format!("Join error: {e}")))?
                    .map_err(|e| {
                        let err_msg = e.to_string();
                        if err_msg.contains("Schema mismatch") {
                            Status::invalid_argument(err_msg)
                        } else {
                            Status::internal(format!("Push error: {err_msg}"))
                        }
                    })?;
                }
                Err(e) => {
                    error!("Error decoding flight data: {e}");
                    return Err(Status::internal(format!("Decode error: {e}")));
                }
            }
        }

        info!(
            shuffle_id = %shuffle_id,
            reducer_id,
            batch_count,
            row_count,
            "DoPut complete"
        );

        let result = PutResult {
            app_metadata: format!("OK: {batch_count} batches, {row_count} rows").into(),
        };

        let stream = stream::once(async { Ok(result) });
        Ok(Response::new(stream.boxed()))
    }

    /// DoAction — implements custom administrative actions, e.g. CleanShuffle.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        info!(action_type = %action.r#type, "Received DoAction request");

        if action.r#type == "CleanShuffle" {
            let shuffle_id = std::str::from_utf8(&action.body)
                .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in shuffle_id: {e}")))?;

            if shuffle_id.is_empty() {
                return Err(Status::invalid_argument("shuffle_id cannot be empty"));
            }

            info!(shuffle_id = %shuffle_id, "Explicit shuffle clean action triggered");
            self.manager.cleanup_shuffle(shuffle_id);

            let result = arrow_flight::Result {
                body: format!("Successfully cleaned shuffle: {shuffle_id}").into(),
            };

            let stream = stream::once(async move { Ok(result) });
            Ok(Response::new(stream.boxed()))
        } else {
            Err(Status::unimplemented(format!(
                "Action '{}' is not implemented",
                action.r#type
            )))
        }
    }

    /// ListActions — returns supported action types.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let action_types = vec![Ok(ActionType {
            r#type: "CleanShuffle".to_string(),
            description: "Cleans up all cached metadata and spill files on disk for a given shuffle_id".to_string(),
        })];
        let stream = stream::iter(action_types);
        Ok(Response::new(stream.boxed()))
    }

    /// PollFlightInfo — not implemented.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }

    /// DoExchange — not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }

    /// GetSchema — returns schema for a partition.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let (shuffle_id, reducer_id) = parse_descriptor(&descriptor)?;

        let schema = self
            .manager
            .get_schema(&shuffle_id, reducer_id)
            .ok_or_else(|| Status::not_found("Partition not found"))?;

        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result: arrow_flight::SchemaResult = schema_ipc
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(format!("Schema error: {e}")))?;

        Ok(Response::new(schema_result))
    }
}
