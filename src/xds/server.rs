use std::{
    net::SocketAddr,
    pin::Pin,
    time::{Duration, Instant},
};

use enum_map::EnumMap;
use futures::Stream;
use metrics::counter;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, trace, warn, Span};
use xds_api::pb::envoy::service::{
    cluster::v3::cluster_discovery_service_server::ClusterDiscoveryService,
    discovery::v3::{
        aggregated_discovery_service_server::AggregatedDiscoveryService, DeltaDiscoveryRequest,
        DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    },
    endpoint::v3::endpoint_discovery_service_server::EndpointDiscoveryService,
    listener::v3::listener_discovery_service_server::ListenerDiscoveryService,
    route::v3::route_discovery_service_server::RouteDiscoveryService,
    status::v3::{
        client_config::GenericXdsConfig,
        client_status_discovery_service_server::ClientStatusDiscoveryService, ClientConfig,
        ClientStatusRequest, ClientStatusResponse, ConfigStatus,
    },
};

use crate::{
    grpc_access,
    xds::{AdsConnection, ResourceType, SnapshotCache},
};

use super::connection::ConnectionSnapshot;

#[derive(Clone)]
pub(crate) struct AdsServer {
    cache: SnapshotCache,
    clients: ConnectionSnapshot,
}

impl AdsServer {
    pub(crate) fn new(cache: SnapshotCache) -> Self {
        Self {
            cache,
            clients: Default::default(),
        }
    }

    fn fetch(
        &self,
        resource_type: ResourceType,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        let request = request.into_inner();

        grpc_access::xds_discovery_request(&request);

        let Some(snapshot_version) = self.cache.version(resource_type) else {
            return Err(Status::unavailable("no snapshot available"));
        };

        let request_version = request.version_info.parse().ok();
        if request_version == Some(snapshot_version) {
            // TODO: delay/long-poll here? this is what go-control-plane does, but it's odd
            return Err(Status::cancelled("already up to date"));
        }

        let mut resources = Vec::with_capacity(request.resource_names.len());
        if request.resource_names.is_empty() {
            for e in self.cache.iter(resource_type) {
                resources.push(e.value().proto.clone());
            }
        } else {
            for name in &request.resource_names {
                if let Some(e) = self.cache.get(resource_type, name) {
                    resources.push(e.value().proto.clone())
                }
            }
        };

        let response = DiscoveryResponse {
            version_info: snapshot_version.to_string(),
            resources,
            ..Default::default()
        };
        grpc_access::xds_discovery_response(&response);

        Ok(Response::new(response))
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

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        remote_addr = tracing::field::Empty,
        node_id = tracing::field::Empty,
        node_cluster = tracing::field::Empty,
    )
)]
async fn stream_ads(
    snapshot: SnapshotCache,
    clients: ConnectionSnapshot,
    remote_addr: Option<SocketAddr>,
    mut requests: Streaming<DiscoveryRequest>,
    send_response: tokio::sync::mpsc::Sender<Result<DiscoveryResponse, Status>>,
) {
    let _conn_active = crate::metrics::scoped_gauge!("ads.active_connections", 1);

    // ?remote_addr shows us Some(_) when an addr is present and %remote_addr
    // doesn't compile. this is annoying but do it anyway.
    if let Some(addr) = remote_addr {
        Span::current().record("remote_addr", addr.to_string());
    }

    macro_rules! send_xds {
        ($chan:expr, $message:expr) => {
            grpc_access::xds_discovery_response(&$message);
            try_send!($chan, Ok($message));
            counter!("ads.tx").increment(1);
        };
    }

    macro_rules! recv_xds {
        ($message:expr) => {
            match $message {
                Ok(Some(msg)) => {
                    grpc_access::xds_discovery_request(&msg);
                    counter!("ads.rx").increment(1);
                    msg
                },
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

    // pull the Node out of the initial request and add the current node info to
    // the current span so we can forget about it for the rest of the stream.
    let mut initial_request = recv_xds!(requests.message().await);
    let mut conn = match AdsConnection::from_initial_request(&mut initial_request, snapshot) {
        Ok(conn) => conn,
        Err(e) => {
            info!(err = %e, "refusing connection: invalid initial request");
            try_send!(send_response, Err(e.into_status()));
            return;
        }
    };

    let node = conn.node();
    let current_span = Span::current();
    current_span.record("node_id", &node.id);
    current_span.record("node_cluster", &node.cluster);

    // first round of message handling.
    //
    // this is *almost* identical to handling any subsequent message, but there
    // are no interrupts from snapshot updates that we might have to handle yet.
    let mut timer = CacheTimer::new(Duration::from_millis(500));
    let (resource_type, responses) = match conn.handle_ads_request(initial_request) {
        Ok((rty, res)) => (rty, res),
        Err(e) => {
            info!(node = ?conn.node(), err = %e, "closing connection: invalid request");
            try_send!(send_response, Err(e.into_status()));
            return;
        }
    };
    if let Some(rtype) = resource_type {
        timer.touch(rtype, Instant::now())
    }
    for response in responses {
        send_xds!(send_response, response);
    }
    clients.update(&conn, remote_addr);

    // respond to either an incoming request or a snapshot update until the client
    // goes away.
    loop {
        let (rtype, responses) = tokio::select! {
            resource_type = timer.wait() => {
                (Some(resource_type), conn.handle_snapshot_update(resource_type))
            },
            request = requests.message() => {
                let message = recv_xds!(request);

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
            send_xds!(send_response, response);
        }
        clients.update(&conn, remote_addr);
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
        let remote_addr = request.remote_addr();

        let requests = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(stream_ads(
            self.cache.clone(),
            self.clients.clone(),
            remote_addr,
            requests,
            tx,
        ));
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

type ClientStatusResponsestream =
    Pin<Box<dyn Stream<Item = Result<ClientStatusResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ClientStatusDiscoveryService for AdsServer {
    type StreamClientStatusStream = ClientStatusResponsestream;

    async fn stream_client_status(
        &self,
        _request: Request<Streaming<ClientStatusRequest>>,
    ) -> Result<Response<Self::StreamClientStatusStream>, Status> {
        return Err(Status::unimplemented(
            "streaming client status is not supported",
        ));
    }

    async fn fetch_client_status(
        &self,
        request: Request<ClientStatusRequest>,
    ) -> Result<Response<ClientStatusResponse>, Status> {
        let request = request.into_inner();
        if !request.node_matchers.is_empty() {
            return Err(Status::invalid_argument("node_matchers are unsupported"));
        }

        let mut config = vec![];
        for conn_snapshot in self.clients.iter() {
            let mut generic_xds_configs = vec![];

            let node = conn_snapshot.node().clone();
            for (rtype, sub_state) in conn_snapshot.subscriptions() {
                let type_url = rtype.type_url();
                let config_status = if sub_state.applied {
                    ConfigStatus::Synced
                } else {
                    ConfigStatus::Error
                };

                for (name, version) in &sub_state.sent {
                    generic_xds_configs.push(GenericXdsConfig {
                        type_url: type_url.to_string(),
                        name: name.to_string(),
                        version_info: version.to_string(),
                        config_status: config_status.into(),
                        ..Default::default()
                    });
                }
            }

            config.push(ClientConfig {
                node: Some(node),
                generic_xds_configs,
                ..Default::default()
            });
        }

        Ok(Response::new(ClientStatusResponse { config }))
    }
}
