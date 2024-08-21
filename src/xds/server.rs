use std::{
    pin::Pin,
    time::{Duration, Instant},
};

use enum_map::EnumMap;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use xds_api::pb::envoy::service::discovery::v3::{
    aggregated_discovery_service_server::AggregatedDiscoveryService, DeltaDiscoveryRequest,
    DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};

use crate::xds::{AdsConnection, ResourceType, Snapshot};

pub(crate) struct AdsServer {
    snapshot: Snapshot,
}

impl AdsServer {
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self { snapshot }
    }
}

type AggregatedResourcesStream =
    Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, Status>> + Send>>;

type DeltaAggregatedResourcesStream =
    Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, Status>> + Send>>;

#[tonic::async_trait]
impl AggregatedDiscoveryService for AdsServer {
    type StreamAggregatedResourcesStream = AggregatedResourcesStream;
    type DeltaAggregatedResourcesStream = DeltaAggregatedResourcesStream;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let requests = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(stream_ads(requests, tx, self.snapshot.clone()));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn delta_aggregated_resources(
        &self,
        _request: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> std::result::Result<tonic::Response<Self::DeltaAggregatedResourcesStream>, Status> {
        return Err(Status::unimplemented(
            "ezbake does not support Incremental ADS",
        ));
    }
}

macro_rules! try_send {
    ($ch:expr, $value:expr) => {
        if let Err(_) = $ch.send($value).await {
            tracing::debug!("channel closed unexpectedly");
            return;
        }
    };
}

macro_rules! debug_xds_discovery_request {
    ($node:expr, $request:expr) => {
        tracing::debug!(
            nack = crate::xds::connection::is_nack(&$request),
            "DiscoveryRequest(v={:?}, n={:?}, ty={:?}, r={:?})",
            $request.version_info,
            $request.response_nonce,
            $request.type_url,
            $request.resource_names,
        );
    };
}

macro_rules! debug_xds_discovery_response {
    ($node:expr, $response:expr) => {
        tracing::debug!(
            "DiscoveryResponse(v={:?}, n={:?}, ty={:?}, r_count={:?})",
            $response.version_info,
            $response.nonce,
            $response.type_url,
            $response.resources.len(),
        );
    };
}

async fn stream_ads(
    mut requests: Streaming<DiscoveryRequest>,
    send_response: tokio::sync::mpsc::Sender<Result<DiscoveryResponse, Status>>,
    snapshot: Snapshot,
) {
    let initial_request = match requests.message().await {
        Ok(Some(message)) => message,
        Ok(None) => return,
        Err(e) => todo!("handle message errors: {e}"),
    };
    debug_xds_discovery_request!(None, &initial_request);

    let mut timer = CacheTimer::new(Duration::from_millis(500));
    let (mut conn, resource_type, responses) =
        match AdsConnection::from_initial_request(initial_request, snapshot) {
            Ok((conn, rtype, responses)) => (conn, rtype, responses),
            Err(_) => todo!(),
        };

    if let Some(rtype) = resource_type {
        timer.touch(rtype, Instant::now())
    }
    for response in responses {
        debug_xds_discovery_response!(&conn.node, &response);
        try_send!(send_response, Ok(response));
    }

    loop {
        let (rtype, responses) = tokio::select! {
            resource_type = timer.wait() => {
                // TODO: skip the update if the last version is identical to this version
                (Some(resource_type), conn.handle_snapshot_update(resource_type))
            },
            request = requests.message() => {
                let message = match request {
                    Ok(Some(msg)) => msg,
                    Ok(None) => return,
                    Err(e) => todo!("handle message errors: {e}"),
                };
                debug_xds_discovery_request!(&conn.node, &message);
                conn.handle_ads_request(message)
            },
        };

        if let Some(rtype) = rtype {
            timer.touch(rtype, Instant::now())
        }
        for response in responses {
            debug_xds_discovery_response!(&conn.node, &response);
            try_send!(send_response, Ok(response));
        }
    }
}

struct CacheTimer {
    interval: Duration,
    timers: EnumMap<ResourceType, Option<Instant>>,
}

impl CacheTimer {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            timers: Default::default(),
        }
    }

    fn touch(&mut self, resource_type: ResourceType, now: Instant) {
        self.timers[resource_type] = Some(now + self.interval)
    }

    async fn wait(&mut self) -> ResourceType {
        let min_entry = self
            .timers
            .iter()
            .filter_map(|(rtype, deadline)| Option::zip(Some(rtype), *deadline))
            .min_by_key(|(_rtype, deadline)| *deadline);

        if let Some((rtype, deadline)) = min_entry {
            tokio::time::sleep_until(deadline.into()).await;
            rtype
        } else {
            futures::future::pending().await
        }
    }
}
