use std::{
    pin::Pin,
    time::{Duration, Instant},
};

use enum_map::EnumMap;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, trace, warn};
use xds_api::pb::envoy::service::{
    cluster::v3::cluster_discovery_service_server::ClusterDiscoveryService,
    discovery::v3::{
        aggregated_discovery_service_server::AggregatedDiscoveryService, DeltaDiscoveryRequest,
        DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    },
    endpoint::v3::endpoint_discovery_service_server::EndpointDiscoveryService,
    listener::v3::listener_discovery_service_server::ListenerDiscoveryService,
    route::v3::route_discovery_service_server::RouteDiscoveryService,
};

use crate::xds::{AdsConnection, ResourceType, Snapshot};

#[derive(Clone)]
pub(crate) struct AdsServer {
    snapshot: Snapshot,
}

impl AdsServer {
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self { snapshot }
    }

    fn fetch(
        &self,
        resource_type: ResourceType,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        let request = request.into_inner();

        let Some(snapshot_version) = self.snapshot.version(resource_type) else {
            return Err(Status::unavailable("no snapshot available"));
        };

        let request_version = request.version_info.parse().ok();
        if request_version == Some(snapshot_version) {
            // TODO: delay/long-poll here? this is what go-control-plane does, but it's odd
            return Err(Status::cancelled("already up to date"));
        }

        let mut resources = Vec::with_capacity(request.resource_names.len());
        if request.resource_names.is_empty() {
            for e in self.snapshot.iter(resource_type) {
                resources.push(e.value().proto.clone());
            }
        } else {
            for name in &request.resource_names {
                if let Some(e) = self.snapshot.get(resource_type, name) {
                    resources.push(e.value().proto.clone())
                }
            }
        };

        Ok(Response::new(DiscoveryResponse {
            version_info: snapshot_version.to_string(),
            resources,
            ..Default::default()
        }))
    }
}

type SotwResponseStream = Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, Status>> + Send>>;
type DeltaResponseStream =
    Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, Status>> + Send>>;

#[tonic::async_trait]
impl AggregatedDiscoveryService for AdsServer {
    type StreamAggregatedResourcesStream = SotwResponseStream;
    type DeltaAggregatedResourcesStream = DeltaResponseStream;

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
    macro_rules! handle_message {
        ($message:expr) => {
            match $message {
                Ok(Some(msg)) => msg,
                // the stream has ended
                Ok(None) => return,
                // the connection is hosed, just bail
                Err(e) if io_source(&e).is_some() => {
                    trace!(err = %e, "closing connection: ignoring io error");
                    return;
                },
                // something actually went wrong!
                Err(e) => {
                    warn!(err = %e, "an unexpected error occurred, closing the connection");
                    return;
                },
            }
        }
    }

    let initial_request = handle_message!(requests.message().await);
    debug_xds_discovery_request!(None, &initial_request);

    let mut timer = CacheTimer::new(Duration::from_millis(500));
    let (mut conn, resource_type, responses) =
        match AdsConnection::from_initial_request(initial_request, snapshot) {
            Ok((conn, rtype, responses)) => (conn, rtype, responses),
            Err(e) => {
                info!(err = %e, "refusing connection: invalid initial request");
                try_send!(send_response, Err(e.into_status()));
                return;
            }
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
                (Some(resource_type), conn.handle_snapshot_update(resource_type))
            },
            request = requests.message() => {
                let message = handle_message!(request);
                debug_xds_discovery_request!(&conn.node, &message);

                match conn.handle_ads_request(message) {
                    Ok((rty, res)) => (rty, res),
                    Err(e) => {
                        info!(node = ?conn.node(), err = %e, "closing connection: invalid request");
                        try_send!(send_response, Err(e.into_status()));
                        return;
                    },
                }
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

fn io_source(status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn std::error::Error + 'static) = status;

    loop {
        if let Some(e) = err.downcast_ref::<std::io::Error>() {
            return Some(e);
        }

        if let Some(e) = err.downcast_ref::<h2::Error>().and_then(|e| e.get_io()) {
            return Some(e);
        }

        err = err.source()?;
    }
}

/// A debouncing timer, for sending cache updates. The timer fires `interval`
/// after the first time it's touched, and ignores all touches until after
/// it fires again.
///
/// The timer is keyed by [ResourceType]. It could be generic, but there's no
/// reason to do that.
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
        self.timers[resource_type].get_or_insert(now + self.interval);
    }

    fn next_deadline(&mut self) -> Option<(ResourceType, Instant)> {
        let min_entry = self
            .timers
            .iter()
            .filter_map(|(rtype, deadline)| Option::zip(Some(rtype), *deadline))
            .min_by_key(|(_rtype, deadline)| *deadline);

        if let Some((rtype, _)) = min_entry.as_ref() {
            self.timers[*rtype] = None;
        }

        min_entry
    }

    async fn wait(&mut self) -> ResourceType {
        match self.next_deadline() {
            Some((rtype, d)) => {
                tokio::time::sleep_until(d.into()).await;
                rtype
            }
            None => futures::future::pending().await,
        }
    }
}

#[cfg(test)]
mod test_timer {
    use std::time::{Duration, Instant};

    use crate::xds::ResourceType;

    use super::CacheTimer;

    #[test]
    fn test_touch_one() {
        let now = Instant::now();
        let mut t = CacheTimer::new(Duration::from_secs(1));

        // touching once sets the deadline.
        t.touch(ResourceType::Cluster, now);
        assert_eq!(
            t.next_deadline(),
            Some((ResourceType::Cluster, now + t.interval))
        );
        assert_eq!(t.next_deadline(), None);

        // touching twice has no effect
        t.touch(ResourceType::Cluster, now);
        t.touch(ResourceType::Cluster, now);
        assert_eq!(
            t.next_deadline(),
            Some((ResourceType::Cluster, now + t.interval))
        );
        assert_eq!(t.next_deadline(), None);
    }

    #[test]
    fn test_touch_many() {
        let now = Instant::now();
        let delta = Duration::from_millis(250);
        let mut t = CacheTimer::new(Duration::from_secs(1));

        // touch two in sequence
        t.touch(ResourceType::Cluster, now);
        t.touch(ResourceType::ClusterLoadAssignment, now + delta);
        assert_eq!(
            t.next_deadline(),
            Some((ResourceType::Cluster, now + t.interval))
        );
        assert_eq!(
            t.next_deadline(),
            Some((
                ResourceType::ClusterLoadAssignment,
                now + delta + t.interval
            ))
        );
        assert_eq!(t.next_deadline(), None);

        // touch two, multiple touches don't reset things
        t.touch(ResourceType::Cluster, now);
        t.touch(ResourceType::Cluster, now + delta);
        t.touch(ResourceType::ClusterLoadAssignment, now + delta);
        assert_eq!(
            t.next_deadline(),
            Some((ResourceType::Cluster, now + t.interval))
        );
        assert_eq!(
            t.next_deadline(),
            Some((
                ResourceType::ClusterLoadAssignment,
                now + delta + t.interval
            ))
        );
        assert_eq!(t.next_deadline(), None);
    }
}

macro_rules! impl_fetch_api {
    (impl $trait:ty => $resource_type:ident { type $sotw_stream:ident; type $delta_stream:ident; fn $fetch:ident; fn $stream:ident; fn $delta:ident;}) => {
        #[tonic::async_trait]
        impl $trait for AdsServer {
            type $sotw_stream = SotwResponseStream;
            type $delta_stream = DeltaResponseStream;

            async fn $fetch(
                &self,
                request: Request<DiscoveryRequest>,
            ) -> Result<Response<DiscoveryResponse>, Status> {
                self.fetch(ResourceType::$resource_type, request)
            }

            async fn $stream(
                &self,
                _request: Request<Streaming<DiscoveryRequest>>,
            ) -> Result<Response<Self::$sotw_stream>, Status> {
                return Err(Status::unimplemented(
                    "ezbake does not support streaming EDS. please use ADS",
                ));
            }

            async fn $delta(
                &self,
                _request: Request<Streaming<DeltaDiscoveryRequest>>,
            ) -> std::result::Result<tonic::Response<Self::$delta_stream>, Status> {
                return Err(Status::unimplemented(
                    "ezbake does not support Incremental EDS",
                ));
            }
        }
    };
}

impl_fetch_api! {
    impl ListenerDiscoveryService => Listener {
        type StreamListenersStream;
        type DeltaListenersStream;

        fn fetch_listeners;
        fn stream_listeners;
        fn delta_listeners;
    }
}

impl_fetch_api! {
    impl RouteDiscoveryService => RouteConfiguration {
        type StreamRoutesStream;
        type DeltaRoutesStream;

        fn fetch_routes;
        fn stream_routes;
        fn delta_routes;
    }
}

impl_fetch_api! {
    impl ClusterDiscoveryService => Cluster {
        type StreamClustersStream;
        type DeltaClustersStream;

        fn fetch_clusters;
        fn stream_clusters;
        fn delta_clusters;
    }
}

impl_fetch_api! {
    impl EndpointDiscoveryService => ClusterLoadAssignment {

        type StreamEndpointsStream;
        type DeltaEndpointsStream;

        fn fetch_endpoints;
        fn stream_endpoints;
        fn delta_endpoints;
    }
}
