use std::collections::BTreeMap;
use std::{net::SocketAddr, pin::Pin, sync::Arc};

use crossbeam_skiplist::SkipMap;
use enum_map::EnumMap;
use futures::Stream;
use metrics::counter;
use smol_str::SmolStr;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, trace, warn, Span};
use xds_api::pb::envoy::config::core::v3 as xds_node;
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
    xds::{ResourceType, SnapshotCache},
};

use super::{delta, sotw};

#[derive(Clone)]
pub(crate) struct AdsServer {
    cache: SnapshotCache,
    stats: ConnectionSnapshot,
}

impl AdsServer {
    pub(crate) fn new(cache: SnapshotCache) -> Self {
        Self {
            cache,
            stats: Default::default(),
        }
    }

    fn fetch(
        &self,
        resource_type: ResourceType,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        let request = request.into_inner();

        grpc_access::xds_discovery_request(&request);

        let snapshot_version = self.cache.version(resource_type);
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

#[allow(unused, dead_code)]
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        remote_addr = tracing::field::Empty,
        node_id = tracing::field::Empty,
        node_cluster = tracing::field::Empty,
    )
)]
async fn stream_delta_ads(
    snapshot: SnapshotCache,
    conn_stats: ConnectionStats,
    mut requests: Streaming<DeltaDiscoveryRequest>,
    send_response: tokio::sync::mpsc::Sender<Result<DeltaDiscoveryResponse, Status>>,
) {
    let _conn_active = crate::metrics::scoped_gauge!("delta_ads.active_connections", 1);

    // ?remote_addr shows us Some(_) when an addr is present and %remote_addr
    // doesn't compile. this is annoying but do it anyway.
    if let Some(addr) = &conn_stats.key.remote_addr {
        Span::current().record("remote_addr", addr.to_string());
    }

    // save a handle to the snapshot to watch for changes
    let mut snapshot_changes = snapshot.changes();

    macro_rules! send_xds {
        ($chan:expr, $message:expr) => {
            grpc_access::xds_delta_discovery_response(&$message);
            try_send!($chan, Ok($message));
            counter!("delta_ads.tx").increment(1);
        };
    }

    macro_rules! recv_xds {
        ($message:expr) => {
            match $message {
                Ok(Some(msg)) => {
                    grpc_access::xds_delta_discovery_request(&msg);
                    counter!("delta_ads.rx").increment(1);
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

    let mut initial_request = recv_xds!(requests.message().await);
    let mut conn = match delta::AdsConnection::from_initial_request(&mut initial_request, snapshot)
    {
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
    conn_stats.update_node(node);

    conn.handle_ads_request(initial_request);

    let responses = conn.ads_responses();
    for response in responses {
        send_xds!(send_response, response);
    }
    conn_stats.update_subs(conn.sent());

    loop {
        // TODO: figure out how to coalesce multiple messages in a reasonable
        // way here. the right thing is something like "await the first one and
        // then try to pull out the same events a second-nth time", and to not
        // use a timer since the timer granularity is so low (like 1ms on
        // tokio?).
        tokio::select! {
            biased;

            resource_type = snapshot_changes.changed() => {
                conn.handle_snapshot_update(resource_type);
            },
            request = requests.message() => {
                let request = recv_xds!(request);
                if let Err(e) = conn.handle_ads_request(request) {
                    info!(node = ?conn.node(), err = %e, "closing connection: invalid request");
                    try_send!(send_response, Err(e.into_status()));
                    return;
                }
            }
        }

        let responses = conn.ads_responses();
        if !responses.is_empty() {
            for response in responses {
                send_xds!(send_response, response);
            }
            conn_stats.update_subs(conn.sent());
        }
    }
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
    conn_stats: ConnectionStats,
    mut requests: Streaming<DiscoveryRequest>,
    send_response: tokio::sync::mpsc::Sender<Result<DiscoveryResponse, Status>>,
) {
    let _conn_active = crate::metrics::scoped_gauge!("ads.active_connections", 1);

    // ?remote_addr shows us Some(_) when an addr is present and %remote_addr
    // doesn't compile. this is annoying but do it anyway.
    if let Some(addr) = &conn_stats.key.remote_addr {
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

    // save a handle to the snapshot to watch for changes
    let mut snapshot_changes = snapshot.changes();

    // pull the Node out of the initial request and add the current node info to
    // the current span so we can forget about it for the rest of the stream.
    let mut initial_request = recv_xds!(requests.message().await);
    let mut conn = match sotw::AdsConnection::from_initial_request(&mut initial_request, snapshot) {
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
    let (_, responses) = match conn.handle_ads_request(initial_request) {
        Ok((rty, res)) => (rty, res),
        Err(e) => {
            info!(node = ?conn.node(), err = %e, "closing connection: invalid request");
            try_send!(send_response, Err(e.into_status()));
            return;
        }
    };
    for response in responses {
        send_xds!(send_response, response);
    }
    conn_stats.update_subs(conn.sent());

    // respond to either an incoming request or a snapshot update until the client
    // goes away.
    loop {
        let (_, responses) = tokio::select! {
            resource_type = snapshot_changes.changed() => {
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

        for response in responses {
            send_xds!(send_response, response);
        }
        conn_stats.update_subs(conn.sent());
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
        let local_addr = request.local_addr();
        let conn_stats = self.stats.new_connection("sotw", remote_addr, local_addr);

        let requests = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(stream_ads(self.cache.clone(), conn_stats, requests, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn delta_aggregated_resources(
        &self,
        request: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> std::result::Result<tonic::Response<Self::DeltaAggregatedResourcesStream>, Status> {
        let remote_addr = request.remote_addr();
        let local_addr = request.local_addr();
        let conn_stats = self.stats.new_connection("delta", remote_addr, local_addr);

        let requests = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(stream_delta_ads(
            self.cache.clone(),
            conn_stats,
            requests,
            tx,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
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
        for conn_stats in self.stats.iter() {
            let mut generic_xds_configs = vec![];

            let node = conn_stats.node().clone();
            for (rtype, subs) in conn_stats.subscriptions() {
                generic_xds_configs.extend(subs.to_generic_xds_config(rtype));
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

// an Arc'd map of connection stats
#[derive(Clone, Default)]
struct ConnectionSnapshot {
    connections: Arc<SkipMap<ConnKey, ConnInfo>>,
}

impl ConnectionSnapshot {
    fn new_connection(
        &self,
        proto: &'static str,
        remote_addr: Option<SocketAddr>,
        local_addr: Option<SocketAddr>,
    ) -> ConnectionStats {
        ConnectionStats {
            connections: self.connections.clone(),
            key: ConnKey {
                protocol: proto,
                remote_addr,
                local_addr,
            },
        }
    }

    fn iter(&self) -> impl Iterator<Item = ConnectionSnapshotEntry> + '_ {
        self.connections.iter().map(ConnectionSnapshotEntry)
    }
}

pub(crate) struct ConnectionSnapshotEntry<'a>(
    crossbeam_skiplist::map::Entry<'a, ConnKey, ConnInfo>,
);

impl ConnectionSnapshotEntry<'_> {
    pub(crate) fn node(&self) -> &xds_node::Node {
        &self.0.value().node
    }

    pub(crate) fn subscriptions(&self) -> &EnumMap<ResourceType, SubInfo> {
        &self.0.value().subscriptions
    }
}

// an RAII stats counter for an individual connection
struct ConnectionStats {
    connections: Arc<SkipMap<ConnKey, ConnInfo>>,
    key: ConnKey,
}

impl Drop for ConnectionStats {
    fn drop(&mut self) {
        self.connections.remove(&self.key);
    }
}

// a key to uniquely identify a connection
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ConnKey {
    protocol: &'static str,
    remote_addr: Option<SocketAddr>,
    local_addr: Option<SocketAddr>,
}

impl ConnectionStats {
    fn update_node(&self, node: &xds_node::Node) {
        let subscriptions = match self.connections.get(&self.key) {
            Some(e) => e.value().subscriptions.clone(),
            None => Default::default(),
        };

        self.connections.insert(
            self.key.clone(),
            ConnInfo {
                node: node.clone(),
                subscriptions,
            },
        );
    }

    fn update_subs(&self, subscriptions: EnumMap<ResourceType, SubInfo>) {
        let node = self
            .connections
            .get(&self.key)
            .map(|e| e.value().node.clone())
            .unwrap_or_default();

        self.connections.insert(
            self.key.clone(),
            ConnInfo {
                node,
                subscriptions,
            },
        );
    }
}

// protocol-based connection info. agnostic to sotw vs. delta so we can use csds
// for both.
struct ConnInfo {
    node: xds_node::Node,
    subscriptions: EnumMap<ResourceType, SubInfo>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SubInfo {
    pub(crate) applied: bool,
    pub(crate) sent: BTreeMap<SmolStr, SmolStr>,
}

impl SubInfo {
    fn to_generic_xds_config(&self, rtype: ResourceType) -> Vec<GenericXdsConfig> {
        let type_url = rtype.type_url();
        let config_status = match self.applied {
            true => ConfigStatus::Synced,
            false => ConfigStatus::Error,
        };
        let configs = self.sent.iter().map(|(name, version)| {
            let name = name.to_string();
            let version_info = version.to_string();
            GenericXdsConfig {
                type_url: type_url.to_string(),
                name,
                version_info,
                config_status: config_status.into(),
                ..Default::default()
            }
        });
        configs.collect()
    }
}
