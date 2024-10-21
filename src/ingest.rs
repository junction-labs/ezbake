use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use gateway_api::apis::experimental::httproutes::HTTPRoute;
use junction_api::{backend::Backend, http::Route, Name, ServiceTarget, Target};
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
    k8s::{ChangedObjects, KubeResource, RefAndParents, Watch},
    xds::{ResourceType, SnapshotWriter, VersionCounter},
};

// FIXME: switching to targets/ports means that we absolutely want to see the
// full object on update/delete. we have to account for changes in ports and
// remove all ports. this is not a perfect answer since we have to be able to
// handle missing a delete event and seeing e.g. a create (which will only have
// the new state and not the old state).

// FIXME: Listeners, Clusters, and Endpoints should all be async fn (...) ->
// Result<(), Error> instead of objects that get immediately new()'d then
// run()'d.

// FIXME: tests????

/// Shorthand for `protobuf::Any::from_msg(val).expect("...")` with a standard
/// message.
///
/// This isn't a fn so we don't have to depend on Prost to pick up prost::Name
macro_rules! into_any {
    ($msg:expr) => {
        protobuf::Any::from_msg(&$msg).expect("failed to serialize protobuf::Any. this is a bug")
    };
}

/// An error converting resources or getting bad k8s input.
#[derive(Debug, thiserror::Error)]
enum IngestError {
    #[error("invalid object: {0}")]
    InvalidObject(String),

    #[error("failed to convert: {0}")]
    InvalidConfig(#[from] junction_api::Error),
}

// FIXME: should this be in junction-api? maybe as `Target::from_metadata(namespace: Option<&str>, name: &str)`
fn target_from_ref<K: kube::Resource>(obj_ref: &ObjectRef<K>) -> Result<Target, IngestError> {
    let name =
        Name::from_str(&obj_ref.name).map_err(|e| IngestError::InvalidObject(e.to_string()))?;

    let namespace = obj_ref
        .namespace
        .as_deref()
        .ok_or_else(|| IngestError::InvalidObject("missing namespace".to_string()))?;
    let namespace =
        Name::from_str(namespace).map_err(|e| IngestError::InvalidObject(e.to_string()))?;

    Ok(Target::Service(ServiceTarget {
        name,
        namespace,
        port: None,
    }))
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
                            Ok(listeners) => updates.extend(listeners),
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
        let mut updates = vec![];

        match self.services.get(svc_ref) {
            Some(svc) => {
                let backends = Backend::from_service(&svc)?;

                for backend in backends {
                    // make a default listener for this backend if one doesn't exist
                    let listener_exists = self
                        .writer
                        .snapshot()
                        .contains(ResourceType::Listener, &backend.target.name());
                    if !listener_exists {
                        let listener =
                            api_listener(Route::passthrough_route(backend.target.clone()).to_xds());
                        updates.push((listener.name.clone(), Some(into_any!(listener))));
                    }

                    // make passthrough listener unconditionally
                    let passthrough_route = backend.to_xds_passthrough_route();
                    let listener_name = passthrough_route.name.clone();
                    let listener = api_listener(passthrough_route);
                    updates.push((listener_name, Some(into_any!(listener))));
                }
            }
            None => {
                // FIXME: this leaks on delete
            }
        }

        Ok(updates)
    }

    // On HTTPRoute change, convert directly to Junction routes and listeners.
    fn route_changed(
        &self,
        route_ref: &RefAndParents<HTTPRoute>,
    ) -> Result<Vec<(String, Option<protobuf::Any>)>, IngestError> {
        let mut updates = vec![];

        match self.routes.get(&route_ref.obj) {
            Some(http_route) => {
                let route = Route::from_gateway_httproute(&http_route.spec)?;
                let listener = api_listener(route.to_xds());

                updates.push((route.target.name(), Some(into_any!(listener))));
            }
            None => {
                for parent_ref in &route_ref.parents {
                    let target = target_from_ref(parent_ref)?;
                    updates.push((target.name(), None));
                }
            }
        }

        Ok(updates)
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
        let mut updates = vec![];
        match self.services.get(svc_ref) {
            Some(svc) => {
                let backends = Backend::from_service(&svc)?;
                let clusters = backends.into_iter().map(|b| {
                    let cluster = b.to_xds_cluster();
                    (cluster.name.clone(), Some(into_any!(cluster)))
                });
                updates.extend(clusters);
            }
            None => {
                // FIXME: don't leak Clusters. need to keep an index of service
                // to port so we can delete
            }
        }

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

            // find all of the Services that changed, and then iterate through
            // the Store to find all of the EndpointSlices that belong to this
            // Service. Collect the whole list.
            //
            // this should probably be an index of some kind, but don't bother yet.
            let mut changed_svcs = HashMap::new();
            for slice_ref in &*slice_refs {
                for svc_ref in &slice_ref.parents {
                    changed_svcs.insert(svc_ref, Vec::new());
                }
            }

            for slice in self.slices.state() {
                for svc_ref in slice.parent_refs() {
                    if let Some(slices) = changed_svcs.get_mut(&svc_ref) {
                        slices.push(slice.clone());
                    }
                }
            }

            let mut updates = Vec::with_capacity(changed_svcs.len());
            for (svc_ref, slices) in changed_svcs {
                let Ok(target) = target_from_ref(svc_ref) else {
                    continue;
                };

                if !slices.is_empty() {
                    trace!(svc = %svc_ref, cla = %target.name(), "building ClusterLoadAssignment");

                    for cla in build_clas(&target, slices.iter().map(Arc::as_ref)) {
                        let name = cla.cluster_name.clone();
                        let proto = Some(into_any!(cla));
                        updates.push((name, proto));
                    }
                } else {
                    trace!(svc = %svc_ref, cla = %target.name(), "deleting ClusterLoadAssignment: service is gone");

                    // FIXME: this leaks on delete - need to track Service ports
                    updates.push((target.name(), None));
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

fn build_clas<'a>(
    target: &Target,
    endpoint_slices: impl Iterator<Item = &'a EndpointSlice>,
) -> Vec<xds_endpoint::ClusterLoadAssignment> {
    use xds_core::address::Address;
    use xds_core::socket_address::PortSpecifier;
    use xds_endpoint::lb_endpoint::HostIdentifier;

    // target -> zone -> [endpoint]
    let mut endpoints: BTreeMap<Target, BTreeMap<String, Vec<_>>> = BTreeMap::new();

    for endpoint_slice in endpoint_slices {
        let ports = endpoint_slice_ports(endpoint_slice);
        if ports.is_empty() {
            continue;
        }

        for port in ports {
            let target = target.with_port(port);
            for endpoint in &endpoint_slice.endpoints {
                if !is_endpoint_ready(endpoint) {
                    continue;
                }

                let zone = endpoint.zone.clone().unwrap_or_default();
                let endpoints = endpoints
                    .entry(target.clone())
                    .or_default()
                    .entry(zone)
                    .or_default();

                for address in &endpoint.addresses {
                    let socket_addr = xds_core::SocketAddress {
                        address: address.clone(),
                        port_specifier: Some(PortSpecifier::PortValue(port as u32)),
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

    let mut clas = vec![];
    for (target, endpoints_by_zone) in endpoints {
        let endpoints = endpoints_by_zone.into_iter().map(|(zone, lb_endpoints)| {
            let locality = Some(xds_core::Locality {
                zone,
                ..Default::default()
            });

            xds_endpoint::LocalityLbEndpoints {
                locality,
                lb_endpoints,
                ..Default::default()
            }
        });

        let cluster_name = target.name();
        let endpoints = endpoints.collect();
        clas.push(xds_endpoint::ClusterLoadAssignment {
            cluster_name,
            endpoints,
            ..Default::default()
        });
    }

    clas
}

fn endpoint_slice_ports(slice: &EndpointSlice) -> Vec<u16> {
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
