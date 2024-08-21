use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use gateway_api::apis::standard::httproutes::HTTPRoute;
use k8s_openapi::api::{
    core::v1::Service,
    discovery::v1::{Endpoint, EndpointSlice},
};
use kube::{
    runtime::reflector::{ObjectRef, Store},
    Resource, ResourceExt,
};

use tokio::sync::broadcast;
use tracing::trace;
use xds_api::pb::envoy::{
    config::{
        cluster::v3 as xds_cluster,
        core::v3::{self as xds_core},
        endpoint::v3 as xds_endpoint,
        listener::v3 as xds_listener,
        route::v3 as xds_route,
    },
    extensions::filters::{
        http::router::v3 as xds_http_filter,
        network::http_connection_manager::v3::{self as xds_http, HttpFilter},
    },
};
use xds_api::pb::google::protobuf::Any as ProtoAny;

use crate::{
    k8s::{namespace_and_name, ref_namespace_and_name, ChangedObjects, KubeResource},
    xds::{to_any, SnapshotWriter},
};

// FIXME: what do we do about errors in conversion? just log?

#[derive(Debug, Default)]
struct VersionCounter {
    counter: AtomicU64,
}

impl VersionCounter {
    fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}

/// `Listeners`` listens for changes to either `HTTPRoute`s or `Service`s in
/// kube and converts them into XDS Listener resources. All Listeners and
/// route configuration comes from the local cluster.
///
/// A Service should always generate an XDS Listener, with any number of routes
/// configured for it.
///
/// TODO: How many routes should we look for? Is it ok to only allow one per
/// namespace? What if there are two with conflicting rules?
pub(crate) struct Listeners {
    version_counter: VersionCounter,
    services: Store<Service>,
    // routes: Store<HTTPRoute>,
    writer: SnapshotWriter,
    service_changed: broadcast::Receiver<ChangedObjects<Service>>,
    // routes_changed: Receiver<ChangedObjects<HTTPRoute>>,
}

impl Listeners {
    pub(crate) fn new(
        writer: SnapshotWriter,
        services: Store<Service>,
        service_changed: broadcast::Receiver<ChangedObjects<Service>>,
    ) -> Self {
        Listeners {
            version_counter: VersionCounter::default(),
            services,
            writer,
            service_changed,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                services = self.service_changed.recv() => {
                    let services = services.expect("Listener construction fell behind. Giving up and going away.");

                    let updates = services.iter().map(|svc_ref| self.service_changed(&svc_ref.obj));
                    let version = self.version_counter.next();
                    self.writer.update(version, updates);
                    trace!(version, changed = services.len(), resource_type = ?crate::xds::ResourceType::Listener, "updated snapshot");
                },
            }
        }
    }

    #[allow(unused)]
    fn route_changed(&self, _route_ref: ObjectRef<HTTPRoute>) -> (String, Option<ProtoAny>) {
        todo!("add support for HTTPRoute")
    }

    fn service_changed(&self, svc_ref: &ObjectRef<Service>) -> (String, Option<ProtoAny>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let listener_name = listener_name(namespace, name);

        trace!(svc = %svc_ref, listener = %listener_name, "building Listener");

        let proto = self.services.get(svc_ref).map(|svc| {
            let listener = build_listener(&svc, None);
            to_any(listener).expect("constructed invalid Listener")
        });

        (listener_name, proto)
    }
}

/// `Clusters` listens for changes to Services in Kubernetes and generates XDS Cluster
/// resources. Clusters are always pulled from the local cluster.
pub(crate) struct Clusters {
    version_counter: VersionCounter,
    services: Store<Service>,
    writer: SnapshotWriter,
    service_changed: broadcast::Receiver<ChangedObjects<Service>>,
}

impl Clusters {
    pub(crate) fn new(
        writer: SnapshotWriter,
        services: Store<Service>,
        service_changed: broadcast::Receiver<ChangedObjects<Service>>,
    ) -> Self {
        Self {
            version_counter: VersionCounter::default(),
            services,
            writer,
            service_changed,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let services = self
                .service_changed
                .recv()
                .await
                .expect("Cluster construction fell behind. Giving up and going away.");

            let updates = services
                .iter()
                .map(|svc_ref| self.service_changed(&svc_ref.obj));
            let version = self.version_counter.next();
            self.writer.update(version, updates);
            trace!(version, changed = services.len(), resource_type = ?crate::xds::ResourceType::Cluster, "updated snapshot");
        }
    }

    fn service_changed(&self, svc_ref: &ObjectRef<Service>) -> (String, Option<ProtoAny>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let cluster_name = cluster_name(namespace, name);

        trace!(svc = %svc_ref, cluster = %cluster_name, "building Cluster");

        let proto = self.services.get(svc_ref).map(|svc| {
            let cluster = build_cluster(&svc);
            to_any(cluster).expect("build_cluster: constructed invalid Cluster")
        });

        (cluster_name, proto)
    }
}

pub(crate) struct LoadAssignments {
    version_counter: VersionCounter,
    slices: Store<EndpointSlice>,
    writer: SnapshotWriter,

    slices_changed: broadcast::Receiver<ChangedObjects<EndpointSlice>>,
}

impl LoadAssignments {
    pub(crate) fn new(
        writer: SnapshotWriter,
        slices: Store<EndpointSlice>,
        slices_changed: broadcast::Receiver<ChangedObjects<EndpointSlice>>,
    ) -> Self {
        Self {
            version_counter: VersionCounter::default(),
            slices,
            slices_changed,
            writer,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let slice_refs = self
                .slices_changed
                .recv()
                .await
                .expect("Cluster construction fell behind. Giving up and going away.");

            let mut changed_svcs = HashSet::new();
            for slice_ref in slice_refs.iter() {
                changed_svcs.extend(&slice_ref.parents);
            }

            let mut changed_svcs: HashMap<_, Vec<Arc<EndpointSlice>>> = changed_svcs
                .into_iter()
                .map(|obj_ref| (obj_ref, Vec::new()))
                .collect();

            for slice in self.slices.state() {
                for svc_ref in slice.parent_refs() {
                    if let Some(slices) = changed_svcs.get_mut(&svc_ref) {
                        slices.push(slice.clone());
                    }
                }
            }

            let mut updates = Vec::with_capacity(changed_svcs.len());
            for (svc_ref, slices) in changed_svcs {
                if !slices.is_empty() {
                    trace!(svc = %svc_ref, slice_count = slice_refs.len(), "updating ClusterLoadAssignment");
                    let slice_refs: Vec<_> = slices.iter().map(|rc| rc.as_ref()).collect();
                    updates.push(self.build_cla(svc_ref, slice_refs.into_iter()));
                } else {
                    let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
                    let name = cla_name(namespace, name);
                    trace!(svc = %svc_ref, cla = %name, "deleting ClusterLoadAssignment: service is gone");
                    updates.push((name, None));
                }
            }

            let version = self.version_counter.next();
            let changed = updates.len();
            self.writer.update(version, updates.into_iter());

            trace!(
                version,
                changed,
                resource_type = ?crate::xds::ResourceType::ClusterLoadAssignment,
                "updated snapshot",
            );
        }
    }

    fn build_cla<'s, 'a>(
        &'s self,
        svc_ref: &ObjectRef<Service>,
        slice_refs: impl Iterator<Item = &'a EndpointSlice>,
    ) -> (String, Option<ProtoAny>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let name = cla_name(namespace, name);
        let proto = build_cla(svc_ref, slice_refs);

        trace!(svc = %svc_ref, cla = %name, "building ClusterLoadAssignment");

        (
            name,
            Some(to_any(proto).expect("built invalid ClusterLoadAssignment")),
        )
    }
}

/// Generate a well-known `name.namespace.svc.cluster.local` name for a service.
///
fn listener_name(namespace: &str, name: &str) -> String {
    format!("{name}.{namespace}.svc.cluster.local")
}

fn route_config_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}/routes")
}

fn cluster_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}/cluster")
}

fn cla_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}/endpoints")
}

fn ads_config_source() -> xds_core::ConfigSource {
    xds_core::ConfigSource {
        config_source_specifier: Some(xds_core::config_source::ConfigSourceSpecifier::Ads(
            xds_core::AggregatedConfigSource {},
        )),
        resource_api_version: xds_core::ApiVersion::V3 as i32,
        ..Default::default()
    }
}

fn build_listener(service: &Service, http_route: Option<&HTTPRoute>) -> xds_listener::Listener {
    use xds_http::http_connection_manager::RouteSpecifier;
    use xds_http::http_filter::ConfigType;

    let (namespace, name) = namespace_and_name(service).expect("service missing namespace/name");

    let dns_name = listener_name(namespace, name);
    let virtual_hosts = http_route
        .map(build_vhosts)
        .unwrap_or_else(|| default_vhosts(&dns_name, cluster_name(namespace, name)));

    let route_specifier = Some(RouteSpecifier::RouteConfig(xds_route::RouteConfiguration {
        name: route_config_name(namespace, name),
        virtual_hosts,
        ..Default::default()
    }));

    let http_router_filter = xds_http_filter::Router::default();
    let conn_manager = xds_http::HttpConnectionManager {
        route_specifier,
        http_filters: vec![HttpFilter {
            name: "ezbake".to_string(),
            config_type: Some(ConfigType::TypedConfig(
                to_any(http_router_filter).expect("invalid Router filter"),
            )),
            ..Default::default()
        }],
        ..Default::default()
    };

    let api_listener = Some(to_any(conn_manager).expect("generated invald HttpConnectionManager"));
    xds_listener::Listener {
        name: listener_name(
            service.namespace().as_ref().unwrap(),
            service.meta().name.as_ref().unwrap(),
        ),
        api_listener: Some(xds_listener::ApiListener { api_listener }),
        ..Default::default()
    }
}

fn build_vhosts(_http_route: &HTTPRoute) -> Vec<xds_route::VirtualHost> {
    todo!("implement Gateway API support")
}

fn default_vhosts(dns_name: &str, cluster: String) -> Vec<xds_route::VirtualHost> {
    use xds_route::route::Action;
    use xds_route::route_action::ClusterSpecifier;
    use xds_route::route_match::PathSpecifier;

    let path_specifier = PathSpecifier::Prefix(String::new());
    let action = Action::Route(xds_route::RouteAction {
        cluster_specifier: Some(ClusterSpecifier::Cluster(cluster)),
        ..Default::default()
    });

    let default_route = xds_route::Route {
        r#match: Some(xds_route::RouteMatch {
            path_specifier: Some(path_specifier),
            ..Default::default()
        }),
        action: Some(action),
        ..Default::default()
    };

    vec![xds_route::VirtualHost {
        domains: vec![dns_name.to_string()],
        routes: vec![default_route],
        ..Default::default()
    }]
}

fn build_cluster(svc: &Service) -> xds_cluster::Cluster {
    use xds_cluster::cluster::ClusterDiscoveryType;
    use xds_cluster::cluster::DiscoveryType;
    use xds_cluster::cluster::EdsClusterConfig;
    use xds_cluster::cluster::LbPolicy;

    let (namespace, name) =
        namespace_and_name(svc).expect("build_cluster: service missing namespace and name");

    xds_cluster::Cluster {
        name: cluster_name(namespace, name),
        lb_policy: LbPolicy::RoundRobin.into(),
        cluster_discovery_type: Some(ClusterDiscoveryType::Type(DiscoveryType::Eds.into())),
        eds_cluster_config: Some(EdsClusterConfig {
            eds_config: Some(ads_config_source()),
            service_name: cla_name(namespace, name),
        }),
        ..Default::default()
    }
}

fn build_cla<'a>(
    svc_ref: &ObjectRef<Service>,
    endpoint_slices: impl Iterator<Item = &'a EndpointSlice>,
) -> xds_endpoint::ClusterLoadAssignment {
    use xds_core::address::Address;
    use xds_core::socket_address::PortSpecifier;
    use xds_endpoint::lb_endpoint::HostIdentifier;

    let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
    let mut endpoints_by_zone: BTreeMap<String, Vec<_>> = BTreeMap::new();

    for endpoint_slice in endpoint_slices {
        let ports: Vec<u32> = endpoint_slice_ports(endpoint_slice);
        if ports.is_empty() {
            continue;
        }

        for endpoint in &endpoint_slice.endpoints {
            if !is_endpoint_ready(endpoint) {
                continue;
            }

            let zone = endpoint.zone.clone().unwrap_or_default();
            let endpoints = endpoints_by_zone.entry(zone).or_default();

            for address in &endpoint.addresses {
                for port in &ports {
                    let socket_addr = xds_core::SocketAddress {
                        address: address.clone(),
                        port_specifier: Some(PortSpecifier::PortValue(*port)),
                        ..Default::default()
                    };

                    let endpoint = xds_endpoint::LbEndpoint {
                        health_status: xds_core::HealthStatus::Healthy.into(),
                        host_identifier: Some(HostIdentifier::Endpoint(xds_endpoint::Endpoint {
                            address: Some(xds_core::Address {
                                address: Some(Address::SocketAddress(socket_addr)),
                            }),
                            ..Default::default()
                        })),
                        ..Default::default()
                    };

                    endpoints.push(endpoint);
                }
            }
        }
    }

    let endpoints = endpoints_by_zone
        .into_iter()
        .map(|(zone, lb_endpoints)| xds_endpoint::LocalityLbEndpoints {
            locality: Some(xds_core::Locality {
                zone,
                ..Default::default()
            }),
            lb_endpoints,
            ..Default::default()
        })
        .collect();

    xds_endpoint::ClusterLoadAssignment {
        cluster_name: cla_name(namespace, name),
        endpoints,
        ..Default::default()
    }
}

fn endpoint_slice_ports(slice: &EndpointSlice) -> Vec<u32> {
    let Some(slice_ports) = &slice.ports else {
        return Vec::new();
    };

    let mut ports = Vec::with_capacity(slice_ports.len());
    for port in slice_ports {
        let Some(port_no) = port.port else { continue };
        let Ok(port_no) = port_no.try_into() else {
            continue;
        };

        ports.push(port_no);
    }
    ports
}

fn is_endpoint_ready(endpoint: &Endpoint) -> bool {
    endpoint.conditions.as_ref().map_or(false, |conditions| {
        let ready = conditions.ready.unwrap_or(false);
        conditions.serving.unwrap_or(ready)
    })
}
