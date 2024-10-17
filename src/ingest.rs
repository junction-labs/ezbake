use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use gateway_api::apis::experimental::httproutes::HTTPRoute;
use junction_api::{
    backend::Backend,
    http::Route,
    shared::{ServiceTarget, Target},
};
use k8s_openapi::api::{
    core::v1::Service,
    discovery::v1::{Endpoint, EndpointSlice},
};
use kube::runtime::reflector::{ObjectRef, Store};

use tokio::sync::broadcast;
use tracing::{debug, info, trace};
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
    k8s::{ref_namespace_and_name, ChangedObjects, KubeResource, RefAndParents, Watch},
    xds::{SnapshotWriter, VersionCounter},
};

/// shorthand for protobuf::Any::from_msg(..).expect(...) with a standard message
///
/// this isn't a fn so we don't have to depend on Prost
macro_rules! into_any {
    ($msg:expr) => {
        protobuf::Any::from_msg(&$msg).expect("failed to serialize protobuf::Any. this is a bug")
    };
}

// FIXME: what do we do about errors in conversion? right now we just delete any
// existing config while logging. that's not the best.

// FIXME: Listeners, Clusters, and Endpoints should all be async fn (...) ->
// Result<(), Error> instead of objects that get immediately new()'d then
// run()'d.

// FIXME: tests????

/// An error converting resources or getting bad k8s input.
#[derive(Debug, thiserror::Error)]
enum IngestError {
    #[error("invalid object: {0}")]
    InvalidObject(String),

    #[error("failed to convert: {0}")]
    InvalidConfig(#[from] junction_api::Error),
}

fn missing_metadata_error() -> IngestError {
    IngestError::InvalidObject("missing namespace and name".to_string())
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
            version_counter: VersionCounter::with_process_prefix(),
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
                    let Ok(objs) = services else {
                        debug!(resource_type = ?crate::xds::ResourceType::Listener, "ingest exiting");
                        break;
                    };

                    // time the rest of the select block
                    let _timer = crate::metrics::scoped_timer!("ingest_time", "xds_type" => "Listener", "kube_kind" => "Service");

                    let mut updates = vec![];
                    for obj_ref in &*objs {
                        match self.service_changed(&obj_ref.obj) {
                            Ok(update) => updates.extend(update),
                            Err(e) => info!(route = %obj_ref.obj, err = %e, "updating Listener failed"),
                        }
                    }
                    let version = self.version_counter.next();
                    self.writer.update(version, updates.into_iter());
                    debug!(%version, changed = objs.len(), resource_type = ?crate::xds::ResourceType::Listener, "updated snapshot");
                },
                routes = self.routes_changed.recv() => {
                    let Ok(objs) = routes else {
                        debug!(resource_type = ?crate::xds::ResourceType::Listener, "ingest exiting");
                        break;
                    };

                    // time the rest of the select block
                    let _timer = crate::metrics::scoped_timer!("ingest_time", "xds_type" => "Listener", "kube_kind" => "HTTPRoute");

                    let mut updates = vec![];
                    for obj_ref in &*objs {
                        match self.route_changed(obj_ref) {
                            Ok(update) => updates.push(update),
                            Err(e) => info!(route = %obj_ref.obj, err = %e, "updating Listener failed"),
                        }
                    }
                    let version = self.version_counter.next();
                    self.writer.update(version, updates.into_iter());
                    debug!(%version, changed = objs.len(), resource_type = ?crate::xds::ResourceType::Listener, "updated snapshot");
                },
            }
        }
    }

    // On service change, unconditionally re-create the default Listener for
    // this service to pass load balancer info over xDS.
    //
    // If there's no Listener registered with the same name and namespace, also
    // create an empty route pointing at this Service to poulate the DNS name.
    fn service_changed(
        &self,
        svc_ref: &ObjectRef<Service>,
    ) -> Result<Vec<(String, Option<protobuf::Any>)>, IngestError> {
        let (namespace, name) =
            ref_namespace_and_name(svc_ref).ok_or_else(missing_metadata_error)?;
        let listener_name = listener_name(namespace, name);

        trace!(svc = %svc_ref, listener = %listener_name, "building Listener: Service changed");

        let backends = self
            .services
            .get(svc_ref)
            .map(|s| Backend::from_service(&s))
            .transpose()?;

        let mut updates = vec![];
        // create/destroy the default Listener for every backend
        match &backends {
            Some(backends) => {
                // FIXME: with mulitple ports, these will clobber
                let default_listeners = backends.iter().map(|b| {
                    let listener = default_listener(&b.target, b.to_xds_default_vhost());

                    (
                        default_listener_name(namespace, name),
                        Some(into_any!(listener)),
                    )
                });
                updates.extend(default_listeners);
            }
            None => updates.push((default_listener_name(namespace, name), None)),
        }

        // if there's no corresponding route, generate a passthrough route
        if let Some(backends) = &backends {
            for backend in backends {
                // FIXME: this will clobber with multiple ports. replace it with the target-name
                let route_ref = ObjectRef::new(name).within(namespace);
                if self.routes.get(&route_ref).is_none() {
                    let route = Route::passthrough_route(backend.target.clone());
                    let listener = into_any!(api_listener(route.to_xds()));
                    updates.push((listener_name.clone(), Some(listener)));
                }
            }
        }

        Ok(updates)
    }

    // On HTTPRoute change, convert directly to Junction routes and listeners.
    fn route_changed(
        &self,
        route_ref: &RefAndParents<HTTPRoute>,
    ) -> Result<(String, Option<protobuf::Any>), IngestError> {
        let (namespace, name) =
            ref_namespace_and_name(&route_ref.obj).ok_or_else(missing_metadata_error)?;
        let listener_name = listener_name(namespace, name);

        trace!(route = %route_ref.obj, listener = %listener_name, "building Listener: HTTPRoute changed");

        let route = self.routes.get(&route_ref.obj);
        let listener = route
            .map(|route| Route::from_gateway_httproute(&route.spec))
            .transpose()?
            .map(|route| {
                let listener = api_listener(route.to_xds());
                into_any!(listener)
            });

        Ok((listener_name, listener))
    }

    // fn svc_and_route(
    //     &self,
    //     svc_ref: &ObjectRef<Service>,
    // ) -> Option<(Arc<Service>, Option<Arc<HTTPRoute>>)> {
    //     let svc = self.services.get(svc_ref)?;

    //     let mut route = None;

    //     for r in self.routes.state() {
    //         if r.parent_refs().contains(svc_ref) && route.replace(r).is_some() {
    //             info!(svc = %svc_ref, "found multiple HTTPRoutes attached to the same Service. skipping.");
    //             return None;
    //         }
    //     }

    //     Some((svc, route))
    // }
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
            version_counter: VersionCounter::with_process_prefix(),
            writer,
            services: services.store.clone(),
            service_changed: services.changes.subscribe(),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let Ok(services) = self.service_changed.recv().await else {
                debug!(resource_type = ?crate::xds::ResourceType::Cluster, "ingest exiting");
                break;
            };

            // time each iteration of the loop
            let _timer = crate::metrics::scoped_timer!("ingest_time", "xds_type" => "Cluster", "kube_kind" => "Service");

            let mut updates = vec![];
            for svc_ref in &*services {
                match self.service_changed(&svc_ref.obj) {
                    Ok(clusters) => updates.extend(clusters),
                    Err(e) => info!(svc = %svc_ref.obj, err = %e, "updating Cluster failed"),
                }
            }
            let version = self.version_counter.next();
            self.writer.update(version, updates.into_iter());
            debug!(%version, changed = services.len(), resource_type = ?crate::xds::ResourceType::Cluster, "updated snapshot");
        }
    }

    // On every Service change, create/delete a Cluster for every port defined
    // on this service.
    fn service_changed(
        &self,
        svc_ref: &ObjectRef<Service>,
    ) -> Result<Vec<(String, Option<protobuf::Any>)>, IngestError> {
        let (namespace, name) = ref_namespace_and_name(svc_ref).unwrap();
        let cluster_name = cluster_name(namespace, name);

        trace!(svc = %svc_ref, cluster = %cluster_name, "building Cluster");

        let clusters = self.services.get(svc_ref).map(|s| build_clusters(&s));
        let updates = match clusters {
            Some(clusters) => clusters?
                .into_iter()
                .map(|c| (c.name.clone(), Some(into_any!(c))))
                .collect(),
            // FIXME: this will leak clusters with multiple ports. have to keep
            // an index with the mapping of svc to ports.
            None => vec![(cluster_name, None)],
        };

        Ok(updates)
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
            version_counter: VersionCounter::with_process_prefix(),
            writer,
            slices: slices.store.clone(),
            slices_changed: slices.changes.subscribe(),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let Ok(slice_refs) = self.slices_changed.recv().await else {
                debug!(resource_type = ?crate::xds::ResourceType::ClusterLoadAssignment, "ingest exiting");
                break;
            };

            // time each iteration of the loop
            let _timer = crate::metrics::scoped_timer!("ingest_time", "xds_type" => "ClusterLoadAssignment", "kube_kind" => "EndpointSlice");

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

            debug!(
                %version,
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

        (name, Some(into_any!(proto)))
    }
}

/// Generate a well-known `name.namespace.svc.cluster.local` name for a service.
///
fn listener_name(namespace: &str, name: &str) -> String {
    let target = Target::Service(ServiceTarget {
        name: name.to_string(),
        namespace: namespace.to_string(),
        port: None,
    });
    target.xds_listener_name()
}

fn default_listener_name(namespace: &str, name: &str) -> String {
    let target = Target::Service(ServiceTarget {
        name: name.to_string(),
        namespace: namespace.to_string(),
        port: None,
    });
    target.xds_default_listener_name()
}

fn cluster_name(namespace: &str, name: &str) -> String {
    let target = Target::Service(ServiceTarget {
        name: name.to_string(),
        namespace: namespace.to_string(),
        port: None,
    });
    target.xds_cluster_name()
}

fn cla_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}/endpoints")
}

/// Wrap a Listener with an api_listener around a RouteConfiguration and serve it
/// as part of LDS.
fn api_listener(route: xds_route::RouteConfiguration) -> xds_listener::Listener {
    use xds_http::http_connection_manager::RouteSpecifier;
    use xds_http::http_filter::ConfigType;

    // TODO: figure out the actual name for this route
    let name = route.name.clone();
    let route_specifier = Some(RouteSpecifier::RouteConfig(route));

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

    let api_listener = Some(xds_listener::ApiListener {
        api_listener: Some(
            protobuf::Any::from_msg(&conn_manager).expect("generated invald HttpConnectionManager"),
        ),
    });

    xds_listener::Listener {
        name,
        api_listener,
        ..Default::default()
    }
}

fn default_listener(target: &Target, vhost: xds_route::VirtualHost) -> xds_listener::Listener {
    let name = target.xds_default_listener_name();
    api_listener(xds_route::RouteConfiguration {
        name,
        virtual_hosts: vec![vhost],
        ..Default::default()
    })
}

fn build_clusters(svc: &Service) -> Result<Vec<xds_cluster::Cluster>, IngestError> {
    let backends = Backend::from_service(svc)?;
    Ok(backends.into_iter().map(|b| b.to_xds_cluster()).collect())
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
