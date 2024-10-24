use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use enum_map::EnumMap;
use junction_api::kube::k8s_openapi;
use junction_api::{kube::gateway_api, DNSTarget};

use gateway_api::apis::experimental::httproutes::HTTPRoute;
use junction_api::{backend::Backend, http::Route, Name, ServiceTarget, Target};
use k8s_openapi::api::{
    core::v1::Service,
    discovery::v1::{Endpoint, EndpointSlice},
};
use kube::runtime::reflector::{ObjectRef, Store};

use tokio::sync::broadcast;
use tower::discover::Change;
use tracing::{debug, info, trace, warn};
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
    xds::{ResourceSnapshot, ResourceType, SnapshotWriter, TypedWriters, VersionCounter},
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
    // FIXME: include the object
    #[error("invalid object: {0}")]
    InvalidObject(String),

    #[error("failed to convert: {0}")]
    InvalidConfig(#[from] junction_api::Error),
}

/// Create a ServiceTarget with no ports from a Serivce
fn service_target(obj_ref: &ObjectRef<Service>) -> Result<Target, IngestError> {
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

pub(crate) struct Ingest {
    version_counter: VersionCounter,
    watches: Watches,
    snapshot_writer: TypedWriters,

    // track the mapping between HTTPRoutes and the Routes they generate for deletes
    route_targets: HashMap<ObjectRef<HTTPRoute>, Target>,
    // an index of Service to [Target], updated every time a Service is seen.
    // tracked so that on update its possible to know which Backend Targets to
    // remove.
    service_targets: HashMap<ObjectRef<Service>, HashSet<Target>>,
    // an index tracking which CLAs we've generated for a Service. this has to
    // be tracked independently of service_targets because while kube SHOULD
    // guarantee that they match, we get the information about creates/deletes
    // at separate times and have to keep the world sane on our own.
    cla_targets: HashMap<ObjectRef<Service>, HashSet<Target>>,
    // the set of Routes created by Services
    inferred_routes: BTreeSet<Target>,
    // the set of Routes created by HTTPRoutes
    explicit_routes: BTreeSet<Target>,
}

// FIXME: give this a better name
struct Watches {
    services: Watch<Service>,
    http_routes: Watch<HTTPRoute>,
    slices: Watch<EndpointSlice>,
}

// DO NEXT: split indexes from the rest and move these methods into that struct.
// DO NEXT: map EndpointSlice watches to a batch of changed Services somewhere else
// DO NEXT: delete RefAndParents
// DO NEXT: remove TypedWriters, make a single writer that accepts a Snapshot as an update

impl Ingest {
    async fn run(mut self) {
        let mut svcs = self.watches.services.changes.subscribe();
        let mut routes = self.watches.http_routes.changes.subscribe();
        loop {
            tokio::select! {
                svcs = svcs.recv() => {
                    if let Ok(svcs) = svcs {
                        self.on_select(svcs, |snapshot, obj| self.service_changed(snapshot, obj));
                    }
                }
                // routes = routes.recv() => {
                //     if let Ok(routes) = routes {
                //         self.on_select(routes, |snapshot, obj| self.httproute_changed(snapshot, obj));
                //     }
                // }
            }
        }
    }

    fn on_select<F, K: KubeResource>(&mut self, changed_objs: ChangedObjects<K>, mut f: F)
    where
        F: FnMut(&mut ResourceSnapshot, &ObjectRef<K>) -> Result<(), IngestError>,
    {
        let mut snapshot = ResourceSnapshot::new();

        for changed_obj in &*changed_objs {
            f(&mut snapshot, &changed_obj.obj);
        }

        let version = self.version_counter.next();
        self.snapshot_writer.update(version, snapshot);
    }

    fn httproute_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        route_ref: &ObjectRef<HTTPRoute>,
    ) -> Result<(), IngestError> {
        match self.watches.http_routes.store.get(route_ref) {
            Some(http_route) => {
                let route = Route::from_gateway_httproute(&http_route.spec)?;
                let listener = api_listener(route.to_xds());
                let xds = into_any!(listener);

                snapshot.insert_update(ResourceType::Listener, listener.name, xds);

                let old_target = self
                    .route_targets
                    .insert(route_ref.clone(), route.target.clone());

                match old_target {
                    Some(old_target) if old_target != route.target => {
                        snapshot.insert_delete(ResourceType::Listener, old_target.name());
                    }
                    _ => (),
                }
            }
            None => {
                if let Some(target) = self.route_targets.get(route_ref) {
                    snapshot.insert_delete(ResourceType::Listener, target.name());
                }
            }
        }

        Ok(())
    }

    /// On service update, we have two main reponsibilities:
    ///
    /// - Generate a passthrough listener for the Service so that there's a
    ///   unique Route pointing at every Backend.
    ///
    /// - Make sure there's a Route pointing to this Service with no port. If
    ///   one has already been overridden
    ///
    /// Also have to handle deleting every Cluster
    fn service_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        svc_ref: &ObjectRef<Service>,
    ) -> Result<(), IngestError> {
        let svc = self.watches.services.store.get(svc_ref);

        let backends = match svc.as_ref() {
            Some(svc) => match Backend::from_service(svc) {
                Ok(backends) => backends,
                Err(_e) => todo!("log and return"),
            },
            None => vec![],
        };

        let mut old_targets = self.service_targets.remove(svc_ref).unwrap_or_default();
        let mut new_targets = HashSet::with_capacity(backends.len());

        for backend in &backends {
            old_targets.remove(&backend.target);
            new_targets.insert(backend.target.clone());

            // update the Cluster for this backend.
            let cluster = backend.to_xds_cluster();
            let xds = into_any!(cluster);
            snapshot.insert_update(ResourceType::Cluster, cluster.name, xds);

            // update the passthrough listener
            let listener = api_listener(backend.to_xds_passthrough_route());
            let xds = into_any!(listener);
            snapshot.insert_update(ResourceType::Listener, listener.name, xds);
        }

        // if there's no explicitly created Route that targets this Service
        // without a port, create one.
        let default_target = service_target(svc_ref)?;
        match svc {
            // the service exists
            Some(_) => {
                if !self.explicit_routes.contains(&default_target) {
                    let route = Route::passthrough_route(default_target.clone());
                    let listener = api_listener(route.to_xds());
                    let xds = into_any!(listener);
                    snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                    self.inferred_routes.insert(default_target);
                }
            }
            // the service is deleted
            None => {
                if self.inferred_routes.contains(&default_target) {
                    snapshot.insert_delete(ResourceType::Listener, default_target.name());
                }
            }
        }

        // remove all the Clusters for missing targets
        for target in old_targets {
            snapshot.insert_delete(ResourceType::Cluster, target.name());
        }
        // update the index
        if !new_targets.is_empty() {
            self.service_targets.insert(svc_ref.clone(), new_targets);
        }

        Ok(())
    }

    // Triggered when 1 or more services have their EndpointSlices changed.
    // Groups all endpoint slices by services, and then recomputes CLAs for
    // changed services.
    //
    // This is almost certainly less efficient than keeping an index of Service
    // to EndpointSlice ourselves and recomputing that way - if you're back here
    // because this code is slow, try that.
    //
    // This function expects something else to do the mapping from EndpointSlice
    // to Service.
    fn endpoints_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        services: &[ObjectRef<Service>],
    ) -> Result<(), IngestError> {
        let mut svc_slices: HashMap<_, _> = services
            .iter()
            .map(|svc_ref| (svc_ref.clone(), Vec::new()))
            .collect();

        let all_slices = self.watches.slices.store.state();
        for slice in all_slices {
            for svc_ref in slice.parent_refs() {
                if let Some(slices) = svc_slices.get_mut(&svc_ref) {
                    slices.push(slice.clone());
                }
            }
        }

        for (svc_ref, slices) in svc_slices {
            let Ok(target) = service_target(&svc_ref) else {
                continue;
            };

            let mut new_targets = HashSet::new();
            let mut old_targets = self.cla_targets.remove(&svc_ref).unwrap_or_default();

            for (target, cla) in build_clas(&target, slices.iter().map(Arc::as_ref)) {
                let xds = into_any!(cla);
                snapshot.insert_update(ResourceType::ClusterLoadAssignment, cla.cluster_name, xds);
                old_targets.remove(&target);
                new_targets.insert(target);
            }

            for target in old_targets {
                snapshot.insert_delete(ResourceType::ClusterLoadAssignment, target.name());
            }
        }

        Ok(())
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
                    let snapshot = self.writer.snapshot();
                    // make a default listener for this backend if one doesn't exist
                    let listener_exists =
                        snapshot.contains(ResourceType::Listener, &backend.target.name());
                    let no_port_listener_exists = snapshot.contains(
                        ResourceType::Listener,
                        &backend.target.without_port().name(),
                    );

                    warn!(target = %backend.target.name(), listener_exists, no_port_listener_exists, "checking passthrough route");

                    if !(listener_exists || no_port_listener_exists) {
                        warn!(target = %backend.target.name(), listener_exists, no_port_listener_exists, "building passthrough route");
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
                    let target = service_target(parent_ref)?;
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
                let Ok(target) = service_target(svc_ref) else {
                    continue;
                };

                if !slices.is_empty() {
                    trace!(svc = %svc_ref, cla = %target.name(), "building ClusterLoadAssignment");

                    for (_, cla) in build_clas(&target, slices.iter().map(Arc::as_ref)) {
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
) -> Vec<(Target, xds_endpoint::ClusterLoadAssignment)> {
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
        clas.push((
            target,
            xds_endpoint::ClusterLoadAssignment {
                cluster_name,
                endpoints,
                ..Default::default()
            },
        ));
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
