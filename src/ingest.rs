use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use gateway_api::apis::standard::httproutes::{
    HTTPRoute, HTTPRouteRules, HTTPRouteRulesBackendRefs, HTTPRouteRulesMatches,
    HTTPRouteRulesMatchesHeaders, HTTPRouteRulesMatchesHeadersType,
};
use k8s_openapi::api::{
    core::v1::Service,
    discovery::v1::{Endpoint, EndpointSlice},
};
use kube::{
    runtime::reflector::{ObjectRef, Store},
    Resource, ResourceExt,
};

use tokio::sync::broadcast;
use tracing::{info, trace};
use xds_api::pb::{
    envoy::{
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
    },
    google::protobuf,
};

use crate::{
    k8s::{
        namespace_and_name, ref_namespace_and_name, ChangedObjects, KubeResource, RefAndParents,
        Watch,
    },
    xds::SnapshotWriter,
};

// FIXME: what do we do about errors in conversion? just log? the current situation is truly terrible
// FIXME: Listeners, Clusters, and Endpoints should all be async fn (...) -> Result<(), Error> instead of objects that get immediately new()'d then run()'d.
// FIXME: tests????

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
    writer: SnapshotWriter,
    services: Store<Service>,
    service_changed: broadcast::Receiver<ChangedObjects<Service>>,
    routes: Store<HTTPRoute>,
    routes_changed: broadcast::Receiver<ChangedObjects<HTTPRoute>>,
}

impl Listeners {
    pub(crate) fn new(
        writer: SnapshotWriter,
        services: &Watch<Service>,
        routes: &Watch<HTTPRoute>,
    ) -> Self {
        Listeners {
            version_counter: VersionCounter::default(),
            writer,
            services: services.store.clone(),
            service_changed: services.changes.subscribe(),
            routes: routes.store.clone(),
            routes_changed: routes.changes.subscribe(),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                services = self.service_changed.recv() => {
                    let objs = services.expect("Listener construction fell behind. Giving up and going away.");

                    let updates = objs.iter().map(|obj_ref| self.service_changed(&obj_ref.obj));
                    let version = self.version_counter.next();
                    self.writer.update(version, updates);
                    trace!(version, changed = objs.len(), resource_type = ?crate::xds::ResourceType::Listener, "updated snapshot");
                },
                routes = self.routes_changed.recv() => {
                    if let Ok(objs) = routes {
                        let updates = objs.iter().flat_map(|obj_ref| self.route_changed(obj_ref));
                        let version = self.version_counter.next();
                        self.writer.update(version, updates);
                        trace!(version, changed = objs.len(), resource_type = ?crate::xds::ResourceType::Listener, "updated snapshot");
                    }
                },
            }
        }
    }

    fn service_changed(&self, svc_ref: &ObjectRef<Service>) -> (String, Option<protobuf::Any>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let listener_name = listener_name(namespace, name);

        trace!(svc = %svc_ref, listener = %listener_name, "building Listener: Service changed");

        let proto = self.svc_and_route(svc_ref).map(|(svc, route)| {
            let listener = build_listener(&svc, route.as_deref());
            protobuf::Any::from_msg(&listener).expect("constructed invalid Listener")
        });

        (listener_name, proto)
    }

    fn route_changed(
        &self,
        route_ref: &RefAndParents<HTTPRoute>,
    ) -> Vec<(String, Option<protobuf::Any>)> {
        let route = self.routes.get(&route_ref.obj);
        let svcs = route_ref
            .parents
            .iter()
            .filter_map(|svc_ref| self.services.get(svc_ref));

        let mut updates = vec![];
        for svc in svcs {
            let (namespace, name) = namespace_and_name(svc.as_ref()).unwrap();
            let listener_name = listener_name(namespace, name);
            trace!(namespace, name, listener = %listener_name, "building Listener: HTTPRoute changed");
            let listener = protobuf::Any::from_msg(&build_listener(&svc, route.as_deref()))
                .expect("constructed invalid Listener");

            updates.push((listener_name, Some(listener)))
        }

        updates
    }

    fn svc_and_route(
        &self,
        svc_ref: &ObjectRef<Service>,
    ) -> Option<(Arc<Service>, Option<Arc<HTTPRoute>>)> {
        let svc = self.services.get(svc_ref)?;

        let mut route = None;

        for r in self.routes.state() {
            if r.parent_refs().contains(svc_ref) && route.replace(r).is_some() {
                info!(svc = %svc_ref, "found multiple HTTPRoutes attached to the same Service. skipping.");
                return None;
            }
        }

        Some((svc, route))
    }
}

/// `Clusters` listens for changes to Services in Kubernetes and generates XDS Cluster
/// resources. Clusters are always pulled from the local cluster.
pub(crate) struct Clusters {
    version_counter: VersionCounter,
    writer: SnapshotWriter,
    services: Store<Service>,
    service_changed: broadcast::Receiver<ChangedObjects<Service>>,
}

impl Clusters {
    pub(crate) fn new(writer: SnapshotWriter, services: &Watch<Service>) -> Self {
        Self {
            version_counter: VersionCounter::default(),
            writer,
            services: services.store.clone(),
            service_changed: services.changes.subscribe(),
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

    fn service_changed(&self, svc_ref: &ObjectRef<Service>) -> (String, Option<protobuf::Any>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let cluster_name = cluster_name(namespace, name);

        trace!(svc = %svc_ref, cluster = %cluster_name, "building Cluster");

        let proto = match self.services.get(svc_ref).map(|s| build_cluster(&s)) {
            Some(Some(cluster)) => Some(
                protobuf::Any::from_msg(&cluster)
                    .expect("invalid Cluster config: check svc annotations"),
            ),
            Some(None) => {
                info!(svc = %svc_ref, "invalid Cluster config");
                None
            }
            None => None,
        };

        (cluster_name, proto)
    }
}

pub(crate) struct LoadAssignments {
    version_counter: VersionCounter,
    writer: SnapshotWriter,
    slices: Store<EndpointSlice>,
    slices_changed: broadcast::Receiver<ChangedObjects<EndpointSlice>>,
}

impl LoadAssignments {
    pub(crate) fn new(writer: SnapshotWriter, slices: &Watch<EndpointSlice>) -> Self {
        Self {
            version_counter: VersionCounter::default(),
            writer,
            slices: slices.store.clone(),
            slices_changed: slices.changes.subscribe(),
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
    ) -> (String, Option<protobuf::Any>) {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let name = cla_name(namespace, name);
        let proto = build_cla(svc_ref, slice_refs);

        trace!(svc = %svc_ref, cla = %name, "building ClusterLoadAssignment");

        (
            name,
            Some(protobuf::Any::from_msg(&proto).expect("built invalid ClusterLoadAssignment")),
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

    let hash_policy = annotation_hash_policies(service);
    let virtual_host: xds_route::VirtualHost = http_route
        .and_then(|r| to_xds_vhost(r, &hash_policy))
        .unwrap_or_else(|| default_vhost(cluster_name(namespace, name), hash_policy));

    let route_specifier = Some(RouteSpecifier::RouteConfig(xds_route::RouteConfiguration {
        name: route_config_name(namespace, name),
        virtual_hosts: vec![virtual_host],
        ..Default::default()
    }));

    let http_router_filter = xds_http_filter::Router::default();
    let conn_manager = xds_http::HttpConnectionManager {
        route_specifier,
        http_filters: vec![HttpFilter {
            name: "envoy.filters.http.router".to_string(),
            config_type: Some(ConfigType::TypedConfig(
                protobuf::Any::from_msg(&http_router_filter).expect("invalid Router filter"),
            )),
            ..Default::default()
        }],
        ..Default::default()
    };

    let api_listener = Some(
        protobuf::Any::from_msg(&conn_manager).expect("generated invald HttpConnectionManager"),
    );
    xds_listener::Listener {
        name: listener_name(
            service.namespace().as_ref().unwrap(),
            service.meta().name.as_ref().unwrap(),
        ),
        api_listener: Some(xds_listener::ApiListener { api_listener }),
        ..Default::default()
    }
}

// FIXME: convert matchers other than headers.
//
// - method: envoy uses the :method http/2 header to match on method. see: https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto#envoy-v3-api-msg-config-route-v3-headermatcher
// - query: tthere'
fn to_xds_vhost(
    http_route: &HTTPRoute,
    hash_policy: &[xds_route::route_action::HashPolicy],
) -> Option<xds_route::VirtualHost> {
    let route_namespace = http_route.metadata.namespace.as_ref()?;
    let http_route_rules = http_route.spec.rules.iter().flatten();
    let routes: Vec<_> = http_route_rules
        .flat_map(|r| to_xds_route(route_namespace, r, hash_policy))
        .collect();

    Some(xds_route::VirtualHost {
        domains: vec!["*".to_string()],
        routes,
        ..Default::default()
    })
}

fn to_xds_route(
    route_namespace: &str,
    rule: &HTTPRouteRules,
    hash_policy: &[xds_route::route_action::HashPolicy],
) -> Vec<xds_route::Route> {
    use xds_route::route::Action;

    let backend_refs = rule.backend_refs.as_deref().unwrap_or_default();
    let Some(cluster_spec) = to_xds_cluster_specifier(route_namespace, backend_refs) else {
        return Vec::new();
    };

    let matches = rule.matches.iter().flatten().filter_map(to_xds_route_match);
    matches
        .map(|m| xds_route::Route {
            r#match: Some(m),
            action: Some(Action::Route(xds_route::RouteAction {
                hash_policy: hash_policy.to_vec(),
                cluster_specifier: Some(cluster_spec.clone()),
                ..Default::default()
            })),
            ..Default::default()
        })
        .collect()
}

fn to_xds_cluster_specifier(
    route_namespace: &str,
    backend_refs: &[HTTPRouteRulesBackendRefs],
) -> Option<xds_route::route_action::ClusterSpecifier> {
    use xds_route::route_action::ClusterSpecifier;
    use xds_route::weighted_cluster::ClusterWeight;
    use xds_route::WeightedCluster;

    match &backend_refs {
        &[] => None,
        &[svc_ref] => {
            let cluster_name = cluster_name(
                svc_ref.namespace.as_deref().unwrap_or(route_namespace),
                &svc_ref.name,
            );
            Some(ClusterSpecifier::Cluster(cluster_name))
        }
        backend_refs => {
            let clusters = backend_refs
                .iter()
                .map(|svc_ref| {
                    let name = cluster_name(
                        svc_ref.namespace.as_deref().unwrap_or(route_namespace),
                        &svc_ref.name,
                    );
                    let weight = svc_ref.weight.unwrap_or_default() as u32;
                    ClusterWeight {
                        name,
                        weight: Some(weight.into()),
                        ..Default::default()
                    }
                })
                .collect();

            Some(ClusterSpecifier::WeightedClusters(WeightedCluster {
                clusters,
                ..Default::default()
            }))
        }
    }
}

fn to_xds_route_match(h: &HTTPRouteRulesMatches) -> Option<xds_route::RouteMatch> {
    use gateway_api::apis::standard::httproutes::HTTPRouteRulesMatchesPathType;
    use xds_route::route_match::PathSpecifier;

    let headers = h
        .headers
        .iter()
        .flatten()
        .filter_map(convert_header_match)
        .collect();

    let path_spec = h
        .path
        .as_ref()
        .and_then(|p| p.r#type.as_ref().zip(p.value.as_ref()))
        .and_then(|(p_type, value)| match p_type {
            HTTPRouteRulesMatchesPathType::Exact => Some(PathSpecifier::Path(value.clone())),
            HTTPRouteRulesMatchesPathType::PathPrefix => Some(PathSpecifier::Prefix(value.clone())),
            _ => None,
        });

    Some(xds_route::RouteMatch {
        headers,
        path_specifier: path_spec,
        ..Default::default()
    })
}

fn convert_header_match(h: &HTTPRouteRulesMatchesHeaders) -> Option<xds_route::HeaderMatcher> {
    use xds_route::header_matcher::HeaderMatchSpecifier;
    use HTTPRouteRulesMatchesHeadersType::*;

    let match_spec = match h.r#type {
        Some(Exact) => HeaderMatchSpecifier::ExactMatch(h.value.clone()),
        None => HeaderMatchSpecifier::PresentMatch(true),
        _ => return None,
    };

    Some(xds_route::HeaderMatcher {
        name: h.name.clone(),
        header_match_specifier: Some(match_spec),
        ..Default::default()
    })
}

fn annotation_hash_policies(svc: &Service) -> Vec<xds_route::route_action::HashPolicy> {
    use xds_route::route_action::hash_policy::Header;
    use xds_route::route_action::hash_policy::PolicySpecifier;
    use xds_route::route_action::HashPolicy;

    let mut policies = Vec::new();

    if let Some(header_names) = svc.annotations().get("junctionlabs.io/hash-header") {
        let cleaned_names = header_names.split(',').map(|s| s.trim().to_string());

        let header_policies = cleaned_names.map(|name| HashPolicy {
            terminal: false,
            policy_specifier: Some(PolicySpecifier::Header(Header {
                header_name: name,
                regex_rewrite: None,
            })),
        });

        policies.extend(header_policies);
    }

    policies
}

fn default_vhost(
    cluster: String,
    hash_policy: Vec<xds_route::route_action::HashPolicy>,
) -> xds_route::VirtualHost {
    use xds_route::route::Action;
    use xds_route::route_action::ClusterSpecifier;
    use xds_route::route_match::PathSpecifier;

    let path_specifier = PathSpecifier::Prefix(String::new());
    let action = Action::Route(xds_route::RouteAction {
        cluster_specifier: Some(ClusterSpecifier::Cluster(cluster)),
        hash_policy,
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

    xds_route::VirtualHost {
        domains: vec!["*".to_string()],
        routes: vec![default_route],
        ..Default::default()
    }
}

fn build_cluster(svc: &Service) -> Option<xds_cluster::Cluster> {
    use xds_cluster::cluster::ClusterDiscoveryType;
    use xds_cluster::cluster::DiscoveryType;
    use xds_cluster::cluster::EdsClusterConfig;

    let (namespace, name) =
        namespace_and_name(svc).expect("build_cluster: service missing namespace and name");

    let (lb_policy, lb_config) = lb_config_from(svc.annotations())?;

    Some(xds_cluster::Cluster {
        name: cluster_name(namespace, name),
        lb_policy: lb_policy.into(),
        lb_config: Some(lb_config),
        cluster_discovery_type: Some(ClusterDiscoveryType::Type(DiscoveryType::Eds.into())),
        eds_cluster_config: Some(EdsClusterConfig {
            eds_config: Some(ads_config_source()),
            service_name: cla_name(namespace, name),
        }),
        ..Default::default()
    })
}

/// parse an LbConfig from annotations.
///
/// returns `None` if there was a problem parsing or an unsupported policy was
/// specified, but defaults to RoundRobin if no policy was specified.
fn lb_config_from(
    annotations: &BTreeMap<String, String>,
) -> Option<(
    xds_cluster::cluster::LbPolicy,
    xds_cluster::cluster::LbConfig,
)> {
    use xds_cluster::cluster::ring_hash_lb_config::HashFunction;
    use xds_cluster::cluster::LbConfig;
    use xds_cluster::cluster::LbPolicy;
    use xds_cluster::cluster::RingHashLbConfig;

    match annotations
        .get("junctionlabs.io/lb-strategy")
        .map(|s| LbPolicy::from_str_name(s))
    {
        None | Some(Some(LbPolicy::RoundRobin)) => Some((
            LbPolicy::RoundRobin,
            LbConfig::RoundRobinLbConfig(Default::default()),
        )),
        Some(Some(LbPolicy::RingHash)) => {
            let min_size: u64 = annotations
                .get("junctionlabs.io/ring-hash-min-size")
                .and_then(|x| x.parse().ok())
                .unwrap_or(128);
            let max_size: u64 = annotations
                .get("junctionlabs.io/ring-hash-max-size")
                .and_then(|x| x.parse().ok())
                .unwrap_or(1024);

            Some((
                LbPolicy::RingHash,
                LbConfig::RingHashLbConfig(RingHashLbConfig {
                    minimum_ring_size: Some(min_size.into()),
                    maximum_ring_size: Some(max_size.into()),
                    hash_function: HashFunction::XxHash.into(),
                }),
            ))
        }
        _ => None,
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
