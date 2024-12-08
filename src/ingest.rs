use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    str::FromStr,
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use junction_api::{backend::Backend, http::Route};
use junction_api::{backend::BackendId, Service};
use junction_api::{
    http::HostnameMatch,
    kube::{gateway_api, k8s_openapi},
    Name,
};

use gateway_api::apis::experimental::httproutes::HTTPRoute;
use k8s_openapi::{
    api::{core::v1 as core_v1, discovery::v1 as discovery_v1},
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::runtime::reflector::{ObjectRef, Store};
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
    metrics::scoped_timer,
    xds::{ResourceSnapshot, ResourceType, SnapshotWriter},
};

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
    #[error("{obj_ref}: invalid object: {message}")]
    InvalidObject { obj_ref: String, message: String },

    #[error("failed to convert: {0}")]
    InvalidConfig(#[from] junction_api::Error),
}

fn kube_svc_from(svc_ref: &ObjectRef<core_v1::Service>) -> Result<Service, IngestError> {
    let namespace = svc_ref
        .namespace
        .as_deref()
        .ok_or_else(|| IngestError::InvalidObject {
            obj_ref: svc_ref.to_string(),
            message: "missing namespace".to_string(),
        })?;

    Ok(Service::kube(namespace, &svc_ref.name)?)
}

pub(crate) fn index() -> IngestIndex {
    IngestIndex::default()
}

pub(crate) async fn run(
    index: IngestIndex,
    snapshot_writer: SnapshotWriter,
    services: Watch<core_v1::Service>,
    routes: Watch<HTTPRoute>,
    slices: Watch<discovery_v1::EndpointSlice>,
) -> Infallible {
    let watches = Watches {
        services,
        routes,
        slices,
    };
    run_with(snapshot_writer, watches, index).await
}

async fn run_with(
    mut snapshot_writer: SnapshotWriter,
    watches: Watches,
    mut index: IngestIndex,
) -> Infallible {
    let mut svcs = watches.services.changes.subscribe();
    let mut routes = watches.routes.changes.subscribe();
    let mut slices = watches.slices.changes.subscribe();

    loop {
        tokio::select! {
            svcs = svcs.recv() => {
                if let Ok(svcs) = svcs {
                    batch_snapshot(
                        &mut snapshot_writer,
                        svcs,
                        |snapshot, svc| { index.service_changed(snapshot, &watches.services.store, &watches.slices.store, svc) },
                    );
                }
            }
            routes = routes.recv() => {
                if let Ok(routes) = routes {
                    batch_snapshot(
                        &mut snapshot_writer,
                        routes,
                        |snapshot, route| { index.httproute_changed(snapshot, &watches.routes.store, route) },
                    );
                }
            }
            slices = slices.recv() => {
                if let Ok(slices) = slices {
                    compute_snapshot(
                        &mut snapshot_writer,
                        slices,
                        |snapshot, slices| {
                            let slice_svcs = endpoint_slice_services(&*slices);
                            index.endpoints_changed(
                                snapshot,
                                &watches.services.store,
                                &watches.slices.store,
                                &slice_svcs,
                            )
                        },
                    );
                }
            }
        }
    }
}

fn batch_snapshot<K, F>(writer: &mut SnapshotWriter, changed_objects: ChangedObjects<K>, mut f: F)
where
    K: KubeResource,
    F: FnMut(&mut ResourceSnapshot, &ObjectRef<K>) -> Result<(), IngestError>,
{
    let _timer = scoped_timer!("snapshot-update", "kube_kind" => K::static_kind());
    let mut snapshot = ResourceSnapshot::new();
    for changed in &*changed_objects {
        if let Err(e) = f(&mut snapshot, &changed.obj) {
            info!(
                err = %e,
                "kube_kind" = K::static_kind(),
                "object" = %changed.obj,
                "update failed",
            );
        }
    }

    write_snapshot(writer, snapshot);
}

fn compute_snapshot<K, F>(writer: &mut SnapshotWriter, changed_objects: ChangedObjects<K>, mut f: F)
where
    K: KubeResource,
    F: FnMut(&mut ResourceSnapshot, ChangedObjects<K>) -> Result<(), Vec<IngestError>>,
{
    let _timer = scoped_timer!("snapshot-update", "kube_kind" => K::static_kind());
    let mut snapshot = ResourceSnapshot::new();

    if let Err(errs) = f(&mut snapshot, changed_objects) {
        for err in errs {
            warn!(%err, "kube_kind" = K::static_kind(), "snapshot update failed")
        }
    }

    write_snapshot(writer, snapshot);
}

fn write_snapshot(writer: &mut SnapshotWriter, snapshot: ResourceSnapshot) {
    if snapshot.is_empty() {
        return;
    }

    let updates = snapshot.update_counts();
    let deletes = snapshot.delete_counts();
    let version = writer.update(snapshot);
    debug!(
        version = %version,
        // NOTE: updates/deletes are going to serialize as a janky debug string
        // until the `valuable` feature gets stablizing in `tracing`.
        //
        // https://docs.rs/tracing/0.1.40/tracing/index.html#unstable-features
        // https://docs.rs/valuable/0.1.0/valuable/
        ?updates,
        ?deletes,
        "updated snapshot"
    );
}

fn endpoint_slice_services<'a>(
    slices: impl IntoIterator<Item = &'a RefAndParents<discovery_v1::EndpointSlice>>,
) -> HashSet<ObjectRef<core_v1::Service>> {
    let mut parents = HashSet::new();
    for slice in slices {
        parents.extend(slice.parents.iter().cloned());
    }
    parents
}

struct Watches {
    services: Watch<core_v1::Service>,
    routes: Watch<HTTPRoute>,
    slices: Watch<discovery_v1::EndpointSlice>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RouteIndex {
    inner: Arc<RouteIndexInner>,
}

#[derive(Debug, Default)]
struct RouteIndexInner {
    explicit: SkipMap<Name, (Vec<HostnameMatch>, Vec<u16>)>,
    implicit: SkipMap<Name, (Vec<HostnameMatch>, Vec<u16>)>,
}

impl crate::xds::SnapshotCallback for RouteIndex {
    fn call(&self, mut writer: SnapshotWriter, resource_type: ResourceType, resource_name: &str) {
        if resource_type != ResourceType::Listener {
            return;
        }

        // parse (hostname, port) out of the resource name
        let Some((hostname, port)) = resource_name.split_once(':') else {
            return;
        };
        let Ok(port) = port.parse() else {
            return;
        };

        if let Some(route_name) = self.lookup(hostname, port) {
            let mut snapshot = ResourceSnapshot::new();
            let listener = rds_listener(resource_name.to_string(), route_name.to_string());
            let xds = into_any!(listener);
            snapshot.insert_update(ResourceType::Listener, listener.name, xds);
            let _ = writer.update(snapshot);
        }
    }
}

impl RouteIndex {
    fn add_explicit(&self, route: &Route) {
        let hostnames = route.hostnames.clone();
        let ports = route.ports.clone();

        self.inner
            .explicit
            .insert(route.id.clone(), (hostnames, ports));
    }

    fn add_implicit(&self, route: &Route) {
        let hostnames = route.hostnames.clone();
        let ports = route.ports.clone();

        self.inner
            .implicit
            .insert(route.id.clone(), (hostnames, ports));
    }

    fn remove(&self, name: &Name) {
        // TODO: this could be separated, but keep things together
        // for the moment to make life easy.

        self.inner.explicit.remove(name);
        self.inner.implicit.remove(name);
    }

    fn lookup(&self, hostname: &str, port: u16) -> Option<Name> {
        // this is an extremely inefficient linear scan of all of the
        // existing routes. this is fine for ezbake, since we expect to
        // have ~tens of routes, maybe hundreds.

        // walk explicit routes first
        for entry in self.inner.explicit.iter() {
            let (hostnames, ports) = entry.value();
            // check port match first, it's ez
            if !(ports.is_empty() || ports.contains(&port)) {
                continue;
            }

            if !hostnames.iter().any(|h| h.matches_str(hostname)) {
                continue;
            }

            return Some(entry.key().clone());
        }

        // walk implicit routes next
        for entry in self.inner.implicit.iter() {
            let (hostnames, ports) = entry.value();
            // check port match first, it's ez
            if !(ports.is_empty() || ports.contains(&port)) {
                continue;
            }

            if !hostnames.iter().any(|h| h.matches_str(hostname)) {
                continue;
            }

            return Some(entry.key().clone());
        }

        // ope nada
        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct IngestIndex {
    // an index of Service to [BackendId], updated every time a Service is seen.
    // tracked so that on update its possible to know which Backends to remove.
    service_backends: HashMap<ObjectRef<core_v1::Service>, HashSet<BackendId>>,

    // an index tracking which CLAs we've generated for a Service. this has to
    // be tracked independently of service_targets because while kube SHOULD
    // guarantee that they match, we get the information about creates/deletes
    // at separate times and have to keep the world sane on our own.
    cla_backends: HashMap<ObjectRef<core_v1::Service>, HashSet<BackendId>>,

    // a route/hostname index for all implicit and explicit routes
    routes: RouteIndex,
}

impl IngestIndex {
    pub(crate) fn cache_callbacks(
        &self,
    ) -> Vec<(
        ResourceType,
        Box<dyn crate::xds::SnapshotCallback + Send + Sync>,
    )> {
        vec![(ResourceType::Listener, Box::new(self.routes.clone()))]
    }
}

impl IngestIndex {
    /// Update the index when an HTTPRoute changes.
    ///
    /// On update, translate the HTTPRoute to a junction Route, add it to the
    /// current snapshot as an update, and track it in the index. If the same
    /// HTTPRoute had previously generated an old route, remove it. Mark the
    /// Route as an explicit route.
    ///
    /// On delete, remove the HTTPRoute from the index and remove the route. If
    /// the Route targeted an existing kube Service that now no longer has a
    /// Route, re-generate an implicit one.
    fn httproute_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        routes: &Store<HTTPRoute>,
        route_ref: &ObjectRef<HTTPRoute>,
    ) -> Result<(), IngestError> {
        match routes.get(route_ref) {
            Some(http_route) => {
                let route = Route::from_gateway_httproute(&http_route)?;
                self.routes.add_explicit(&route);

                // TODO: if the Route has ports and explicit hostnames, we
                // can generate listeners for them ahead of time. don't bother yet.

                let xds_route = route.to_xds();
                let xds = into_any!(xds_route);
                snapshot.insert_update(ResourceType::RouteConfiguration, xds_route.name, xds);
            }
            None => {
                // FIXME: we're using just the HTTPRoute name as the Route ID which seems terrible?
                let route_name = Name::from_str(&route_ref.name).expect(
                    "a valid Kubernetes name should be a valid name. this is a bug in Junction",
                );
                self.routes.remove(&route_name);
            }
        }

        Ok(())
    }

    /// On service update, we have two main reponsibilities:
    ///
    /// Generate a passthrough listener for the Service so that there's a unique
    /// Route pointing at every Backend.
    ///
    /// Make sure there's a Route pointing to this Service. If one has already
    /// been created by an HTTPRoute, do nothing.
    ///
    /// Returns `true` if this Service is newly created, so that we can re-trigger
    /// an endpointSlice update.
    fn service_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        svc_store: &Store<core_v1::Service>,
        slice_store: &Store<discovery_v1::EndpointSlice>,
        svc_ref: &ObjectRef<core_v1::Service>,
    ) -> Result<(), IngestError> {
        let mut created = false;

        match svc_store.get(svc_ref).as_ref() {
            Some(svc) => {
                let backends = Backend::from_service(svc)?;
                let mut old_targets = self.service_backends.remove(svc_ref).unwrap_or_default();
                let mut new_targets = HashSet::with_capacity(backends.len());

                if old_targets.is_empty() {
                    created = true;
                }

                for backend in &backends {
                    old_targets.remove(&backend.id);
                    new_targets.insert(backend.id.clone());

                    // update the Cluster for this backend.
                    let cluster = backend.to_xds();
                    let xds = into_any!(cluster);
                    snapshot.insert_update(ResourceType::Cluster, cluster.name, xds);

                    // update the lb config listener
                    let listener = route_listener(backend.to_xds_lb_route_config());
                    let xds = into_any!(listener);
                    snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                }

                // clean up old targets and update the index
                for target in old_targets {
                    snapshot.insert_delete(ResourceType::Cluster, target.name());
                }
                if !new_targets.is_empty() {
                    self.service_backends.insert(svc_ref.clone(), new_targets);
                }

                // create an implicit route and add it to the index.
                //
                // as soon as it's been added, convert to xds and shove it into the
                // snapshot update.
                let implicit_route = implicit_route(svc_ref);
                self.routes.add_implicit(&implicit_route);

                let implicit_route = implicit_route.to_xds();
                let xds = into_any!(implicit_route);
                snapshot.insert_update(ResourceType::RouteConfiguration, implicit_route.name, xds);
            }
            None => {
                // remove the svc from the index
                let targets = self.service_backends.remove(svc_ref).unwrap_or_default();

                // delete all of the Clusters for those targets and all of the
                // passthrough listeners
                for target in &targets {
                    snapshot.insert_delete(ResourceType::Cluster, target.name());
                    snapshot.insert_delete(ResourceType::Listener, target.lb_config_route_name());
                }

                let route_name = implicit_route_name(svc_ref);
                self.routes.remove(&route_name);
                snapshot.insert_delete(ResourceType::RouteConfiguration, route_name.to_string());
            }
        }

        // update endpoints if it looks like we went from nothign to something, to
        // deal with the fact that
        if created {
            if let Err(errs) = self.endpoints_changed(snapshot, svc_store, slice_store, [svc_ref]) {
                // safety: endpoints_changed should never return an empty list of errors
                return Err(errs.into_iter().next().unwrap());
            }
        }

        Ok(())
    }

    // Triggered when 1 or more services have their EndpointSlices changed or
    // when a Service is created. Groups all endpoint slices by services, and
    // then recomputes CLAs for changed services.
    //
    // This is almost certainly less efficient than keeping an index of Service
    // to EndpointSlice ourselves and recomputing that way - if you're back here
    // because this code is slow, try that.
    //
    // This function expects something else to do the mapping from EndpointSlice
    // to Service.
    //
    // There's a race here - the Services every EndpointSlice is attached to
    // need to exist before we can actual calculate CLAs because that requires
    // the reverse mapping from `targetPort` back to `port`. Because of this,
    // services_changed calls endpoints_changed every time a service is created
    // from nothing.
    fn endpoints_changed<'a, I>(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        svc_store: &Store<core_v1::Service>,
        slice_store: &Store<discovery_v1::EndpointSlice>,
        services: I,
    ) -> Result<(), Vec<IngestError>>
    where
        I: IntoIterator<Item = &'a ObjectRef<core_v1::Service>>,
    {
        let mut svc_slices: HashMap<_, _> = services
            .into_iter()
            .map(|svc_ref| (svc_ref, Vec::new()))
            .collect();

        let all_slices = slice_store.state();
        for slice in all_slices {
            for svc_ref in slice.parent_refs() {
                if let Some(slices) = svc_slices.get_mut(&svc_ref) {
                    slices.push(slice.clone());
                }
            }
        }

        let mut errors = vec![];
        for (svc_ref, slices) in svc_slices {
            let Some(svc) = svc_store.get(svc_ref) else {
                trace!(%svc_ref, "skipping endpointSlice: Service does not exist");
                continue;
            };
            let port_lookup = match service_ports(svc_ref, &svc) {
                Ok(lookup) => lookup,
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            };

            let Ok(target) = kube_svc_from(svc_ref) else {
                continue;
            };

            let mut new_targets = HashSet::new();
            let mut old_targets = self.cla_backends.remove(svc_ref).unwrap_or_default();

            for (target, cla) in build_clas(&target, &port_lookup, slices.iter().map(Arc::as_ref)) {
                let xds = into_any!(cla);
                snapshot.insert_update(ResourceType::ClusterLoadAssignment, cla.cluster_name, xds);
                old_targets.remove(&target);
                new_targets.insert(target);
            }

            for target in old_targets {
                snapshot.insert_delete(ResourceType::ClusterLoadAssignment, target.name());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// NOTE: this must be the same as implicit_route
fn implicit_route_name(svc_ref: &ObjectRef<core_v1::Service>) -> Name {
    let namespace = svc_ref.namespace.as_deref().unwrap_or_default();
    let name = &svc_ref.name;

    Name::from_str(&format!("ezbi-{namespace}-{name}"))
        .expect("generated an invalid implicit route")
}

// a passthrough route that forces a port
fn implicit_route(svc_ref: &ObjectRef<core_v1::Service>) -> Route {
    // safety: these are kube names, if they're not valid or there's
    // no namespace that's a wild bug.
    //
    // in some future these should be validated way earlier.
    //
    // NOTE: this must be the same as implicit_route_name
    let namespace = svc_ref.namespace.as_deref().unwrap_or_default();
    let name = &svc_ref.name;
    let route_name = Name::from_str(&format!("ezbi-{namespace}-{name}"))
        .expect("generated an invalid implicit route");
    let svc = Service::kube(namespace, name).unwrap();

    let mut route = Route::passthrough_route(route_name, svc);
    route.tags = BTreeMap::from_iter([
        (
            junction_api::http::tags::GENERATED_BY.to_string(),
            "ezbake".to_string(),
        ),
        ("ezbake/namespace".to_string(), namespace.to_string()),
        ("ezbake/name".to_string(), name.to_string()),
    ]);

    route
}

fn rds_listener(name: String, rds: String) -> xds_listener::Listener {
    use xds_http::http_connection_manager::RouteSpecifier;
    use xds_http::http_filter::ConfigType;
    use xds_http::Rds;

    let route_specifier = Some(RouteSpecifier::Rds(Rds {
        route_config_name: rds,
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

/// Wrap a Listener with an api_listener around a RouteConfiguration and serve it
/// as part of LDS.
fn route_listener(route: xds_route::RouteConfiguration) -> xds_listener::Listener {
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
    service: &Service,
    port_lookup: &HashMap<(String, u16), u16>,
    endpoint_slices: impl Iterator<Item = &'a discovery_v1::EndpointSlice>,
) -> Vec<(BackendId, xds_endpoint::ClusterLoadAssignment)> {
    use xds_core::address::Address;
    use xds_core::socket_address::PortSpecifier;
    use xds_endpoint::lb_endpoint::HostIdentifier;

    // backend -> zone -> [endpoint]
    let mut endpoints: BTreeMap<BackendId, BTreeMap<String, Vec<_>>> = BTreeMap::new();

    for endpoint_slice in endpoint_slices {
        if &endpoint_slice.address_type != "IPv4" {
            continue;
        }

        let slice_ports = endpoint_slice_ports(endpoint_slice);
        if slice_ports.is_empty() {
            continue;
        }

        for (name, target_port) in slice_ports {
            let Some(&port) = port_lookup.get(&(name, target_port)) else {
                continue;
            };

            // the actual backend should be named with the "frontend" port to
            // match incoming service traffic.
            let backend = service.as_backend_id(port);
            for endpoint in &endpoint_slice.endpoints {
                if !is_endpoint_ready(endpoint) {
                    continue;
                }

                let zone = endpoint.zone.clone().unwrap_or_default();
                let endpoints = endpoints
                    .entry(backend.clone())
                    .or_default()
                    .entry(zone)
                    .or_default();

                for address in &endpoint.addresses {
                    let socket_addr = xds_core::SocketAddress {
                        address: address.clone(),
                        // the ACTUAL port should be the target_port
                        port_specifier: Some(PortSpecifier::PortValue(target_port as u32)),
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

// this sucks, the fact that targetPort can be a String is terrible!
fn service_ports(
    svc_ref: &ObjectRef<core_v1::Service>,
    svc: &core_v1::Service,
) -> Result<HashMap<(String, u16), u16>, IngestError> {
    let Some(spec) = svc.spec.as_ref() else {
        return Err(IngestError::InvalidObject {
            obj_ref: svc_ref.to_string(),
            message: "missing spec".to_string(),
        });
    };

    let svc_ports = spec.ports.as_deref().unwrap_or_default();

    let mut mapping = HashMap::new();
    for svc_port in svc_ports {
        let port: u16 = svc_port
            .port
            .try_into()
            .map_err(|_| IngestError::InvalidObject {
                obj_ref: svc_ref.to_string(),
                message: format!("invalid port value: {}", svc_port.port),
            })?;

        let target_port: u16 = match svc_port.target_port.as_ref() {
            Some(IntOrString::Int(port)) => {
                (*port).try_into().map_err(|_| IngestError::InvalidObject {
                    obj_ref: svc_ref.to_string(),
                    message: format!("invalid port value: {port}"),
                })?
            }
            Some(IntOrString::String(name)) => {
                return Err(IngestError::InvalidObject {
                    obj_ref: svc_ref.to_string(),
                    message: format!("can't use named port '{name}' as a targetPort"),
                });
            }
            None => port,
        };

        let port_name = svc_port.name.clone().unwrap_or_default();
        mapping.insert((port_name, target_port), port);
    }

    Ok(mapping)
}

fn endpoint_slice_ports(slice: &discovery_v1::EndpointSlice) -> Vec<(String, u16)> {
    let Some(slice_ports) = &slice.ports else {
        return Vec::new();
    };

    let mut ports = Vec::with_capacity(slice_ports.len());
    for port in slice_ports {
        let Some(port_no) = port.port else { continue };
        let port_name = port.name.clone().unwrap_or_default();
        let Ok(port_no) = port_no.try_into() else {
            continue;
        };
        ports.push((port_name, port_no));
    }
    ports
}

fn is_endpoint_ready(endpoint: &discovery_v1::Endpoint) -> bool {
    endpoint.conditions.as_ref().map_or(false, |conditions| {
        let ready = conditions.ready.unwrap_or(false);
        conditions.serving.unwrap_or(ready)
    })
}

// FIXME: need many, many more tests here.
#[cfg(test)]
mod test {
    use junction_api::{
        http::{BackendRef, PathMatch, RouteMatch, RouteRule},
        Name, Service,
    };
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
    use kube::{
        runtime::{reflector::Lookup, watcher},
        Resource, ResourceExt,
    };
    use std::hash::Hash;

    use super::*;

    #[test]
    fn test_new_service() {
        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();

        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let target = Service::kube("prod", "coolsvc").unwrap();

        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        // route lookup should work as soon as the service
        assert_eq!(
            index.routes.lookup("coolsvc.prod.svc.cluster.local", 80),
            Some(implicit_route_name(&svc_ref)),
        );

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    target.clone().as_backend_id(443).name(),
                    target.clone().as_backend_id(80).name(),
                ],
                vec![],
            ),
            "create Clusters",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    target.clone().as_backend_id(443).lb_config_route_name(),
                    target.clone().as_backend_id(80).lb_config_route_name(),
                ],
                vec![]
            ),
            "create LB Config Listeners",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![implicit_route_name(&svc_ref).to_string()], vec![]),
            "create an implicit RouteConfig"
        )
    }

    #[test]
    fn test_new_externalname_service() {
        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();

        let svc = externalname_service("prod", "coolsvc", "api.junctionlabs.io");
        let svc_ref = ObjectRef::from_obj(&svc);
        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let target = Service::dns("api.junctionlabs.io").unwrap();

        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        // route lookup should work as soon as the service exists. note that for
        // externalname services, the name is still the kubernetes route name
        // and not the ExternalName dns name!
        assert_eq!(
            index.routes.lookup("coolsvc.prod.svc.cluster.local", 80),
            Some(implicit_route_name(&svc_ref)),
        );

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    target.as_backend_id(443).name(),
                    target.as_backend_id(80).name(),
                ],
                vec![],
            ),
            "create Clusters",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    target.as_backend_id(443).lb_config_route_name(),
                    target.as_backend_id(80).lb_config_route_name(),
                ],
                vec![]
            ),
            "create LB Config Listeners",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![implicit_route_name(&svc_ref).to_string()], vec![]),
            "create an implicit RouteConfig"
        )
    }

    // deleting a service should remove all of the clusters/listeners created
    #[test]
    fn test_create_delete_service() {
        let svc = clusterip_service(
            "coolsvc",
            "prod",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let svc_target = kube_svc_from(&svc_ref).unwrap();

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc.clone());

        let mut index = IngestIndex::default();
        index
            .service_changed(
                &mut ResourceSnapshot::new(),
                &svc_store,
                &slice_store,
                &svc_ref,
            )
            .unwrap();

        delete(&mut svc_writer, svc);

        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![],
                vec![
                    svc_target.clone().as_backend_id(443).name(),
                    svc_target.clone().as_backend_id(80).name()
                ],
            ),
            "service delete a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![],
                vec![
                    svc_target.as_backend_id(443).lb_config_route_name(),
                    svc_target.as_backend_id(80).lb_config_route_name(),
                ],
            ),
            "service should delete LB passthrough listeners and the default listener",
        );
    }

    // updating on a service not in the index and store we shouldn't do
    // anything, because it means we never saw the create to add it to the
    // cache/index.
    #[test]
    fn test_service_changed_gone() {
        let svc = clusterip_service(
            "coolsvc",
            "prod",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);

        let (svc_store, _) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();

        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![], vec![implicit_route_name(&svc_ref).to_string()]),
            "should try to delete the implicit RouteConfig"
        );
    }

    // creating a new service after an HTTPRoute exists for it.
    #[test]
    fn test_new_service_route_exists() {
        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let service = Service::kube("prod", "coolsvc").unwrap();

        let httproute = example_route("prod", "cool-example", service.clone(), 443);
        let httproute_ref = ObjectRef::from_obj(&httproute);

        // setup store. the order doesn't matter here.
        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute);

        let mut index = IngestIndex::default();

        // handle index changes. the order matters here - all we're testing is
        // how the index deals with update order.
        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &httproute_ref)
            .unwrap();

        // route lookup should work as soon as the route exists
        assert_eq!(
            index.routes.lookup("coolsvc.prod.svc.cluster.local", 80),
            Some(Name::from_static("cool-example")),
        );

        // snapshot should include the new RouteConfig
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (vec![], vec![]),
            "no Listeners should change",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![httproute_ref.name], vec![]),
            "should create a RouteConfig"
        );

        // update to add the Service now
        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    service.as_backend_id(443).name(),
                    service.as_backend_id(80).name()
                ],
                vec![],
            ),
            "service should create a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    service.as_backend_id(443).lb_config_route_name(),
                    service.as_backend_id(80).lb_config_route_name(),
                ],
                vec![]
            ),
            "service should create LB config listeners",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![implicit_route_name(&svc_ref).to_string()], vec![]),
            "should create an implicit RouteConfig"
        );
    }

    #[test]
    fn test_new_route_service_exists() {
        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let svc_target = kube_svc_from(&svc_ref).unwrap();

        let httproute = example_route("coolsvc-pass", "prod", svc_target.clone(), 443);
        let httproute_ref = ObjectRef::from_obj(&httproute);

        // setup store. the order doesn't matter here.
        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute);

        // handle index changes. the order matters here - all we're testing is
        // how the index deals with update order.
        let mut index = IngestIndex::default();

        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();

        // lookup should return the implicit route and the snapshot should
        // contain Clusters and implicit RouteConfigs for the Service.
        assert_eq!(
            index.routes.lookup("coolsvc.prod.svc.cluster.local", 80),
            Some(implicit_route_name(&svc_ref)),
        );

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    svc_target.as_backend_id(443).name(),
                    svc_target.as_backend_id(80).name()
                ],
                vec![],
            ),
            "service should create a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    svc_target.as_backend_id(443).lb_config_route_name(),
                    svc_target.as_backend_id(80).lb_config_route_name(),
                ],
                vec![]
            ),
            "service should create lb config Listeners",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![implicit_route_name(&svc_ref).to_string()], vec![]),
            "service should create implicit route",
        );

        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &httproute_ref)
            .unwrap();

        // lookup should return the explicit route and the snapshot should
        // contain a new RouteConfig.
        assert_eq!(
            index.routes.lookup("coolsvc.prod.svc.cluster.local", 80),
            Some(Name::from_str(&httproute_ref.name).unwrap()),
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::RouteConfiguration),
            (vec![httproute_ref.name], vec![]),
            "service create passthrough and implicit routes",
        );
    }

    #[test]
    fn test_delete_route_cluserip_service_exists() {
        let route = {
            let id = Name::from_static("cool-route");
            let service = Service::kube("prod", "coolsvc").unwrap();
            Route {
                id,
                hostnames: vec![service.hostname().into()],
                ports: vec![],
                tags: Default::default(),
                rules: vec![RouteRule {
                    matches: vec![RouteMatch {
                        path: Some(PathMatch::empty_prefix()),
                        ..Default::default()
                    }],
                    backends: vec![BackendRef {
                        service,
                        port: Some(443),
                        weight: 1,
                    }],
                    ..Default::default()
                }],
            }
        };
        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);

        test_delete_route(
            route,
            svc,
            "coolsvc.prod.svc.cluster.local",
            Some(implicit_route_name(&svc_ref)),
        );
    }

    #[test]
    fn test_delete_route_externalname_service_exists() {
        let route = {
            let id = Name::from_static("coolapi-route");
            let service = Service::dns("coolapi.com").unwrap();
            Route {
                id,
                hostnames: vec![service.hostname().into()],
                ports: vec![],
                tags: Default::default(),
                rules: vec![RouteRule {
                    matches: vec![RouteMatch {
                        path: Some(PathMatch::empty_prefix()),
                        ..Default::default()
                    }],
                    backends: vec![BackendRef {
                        service,
                        port: Some(443),
                        weight: 1,
                    }],
                    ..Default::default()
                }],
            }
        };
        let svc = externalname_service("prod", "coolsvc", "coolapi.com");

        test_delete_route(route, svc, "coolapi.com", None);
    }

    // deleting a route for an existing service should expose the implicit route
    // for that service.
    fn test_delete_route(
        route: Route,
        svc: core_v1::Service,
        lookup_name: &str,
        route_after_delete: Option<Name>,
    ) {
        let httproute = route
            .to_gateway_httproute(svc.meta().namespace.as_deref().unwrap_or_default())
            .unwrap();
        let httproute_ref = ObjectRef::from_obj(&httproute);

        let svc_ref = ObjectRef::from_obj(&svc);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        let (slice_store, _) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute.clone());

        // create the index and insert both the service and route and look it up
        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &slice_store, &svc_ref)
            .unwrap();
        index
            .httproute_changed(&mut snapshot, &route_store, &httproute_ref)
            .unwrap();

        assert_eq!(
            index.routes.lookup(lookup_name, 80),
            Some(route.id.clone()),
            "should find the route before deleting"
        );

        // delete the route from the store and then try to look it up
        delete(&mut route_writer, httproute);

        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &httproute_ref)
            .unwrap();
        assert_eq!(index.routes.lookup(lookup_name, 80), route_after_delete,);
    }

    #[test]
    fn test_cla() {
        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let slice1 = endpoint_slice(
            &svc,
            "coolsvc-slice1",
            [("http", 8009), ("https", 8010)],
            ["192.168.1.1", "192.168.1.2"],
        );
        let slice2 = endpoint_slice(
            &svc,
            "coolsvc-slice2",
            [("http", 8009), ("https", 8010)],
            ["192.168.1.3", "192.168.1.4"],
        );
        let svc_target = Service::kube("prod", "coolsvc").unwrap();
        let svc_ref = ObjectRef::from_obj(&svc);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc.clone());

        let (slice_store, mut slice_writer) = kube::runtime::reflector::store();
        insert(&mut slice_writer, slice1.clone());
        insert(&mut slice_writer, slice2.clone());

        let mut index = IngestIndex::default();
        index
            .service_changed(
                &mut ResourceSnapshot::new(),
                &svc_store,
                &slice_store,
                &svc_ref,
            )
            .unwrap();

        let mut snapshot = ResourceSnapshot::new();
        index
            .endpoints_changed(&mut snapshot, &svc_store, &slice_store, [&svc_ref])
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::ClusterLoadAssignment),
            (
                vec![
                    svc_target.clone().as_backend_id(443).name(),
                    svc_target.clone().as_backend_id(80).name()
                ],
                vec![]
            ),
        );
    }

    fn insert<K>(store: &mut kube::runtime::reflector::store::Writer<K>, object: K)
    where
        K: 'static + Lookup + Clone,
        <K as Lookup>::DynamicType: Eq + Hash + Clone,
    {
        store.apply_watcher_event(&watcher::Event::Apply(object));
    }

    fn delete<K>(store: &mut kube::runtime::reflector::store::Writer<K>, object: K)
    where
        K: 'static + Lookup + Clone,
        <K as Lookup>::DynamicType: Eq + Hash + Clone,
    {
        store.apply_watcher_event(&watcher::Event::Delete(object));
    }

    #[track_caller]
    fn example_route(
        namespace: &'static str,
        name: &'static str,
        service: Service,
        port: u16,
    ) -> HTTPRoute {
        let route = {
            let id = Name::from_static(name);
            Route {
                id,
                hostnames: vec![service.hostname().into()],
                ports: vec![],
                tags: Default::default(),
                rules: vec![RouteRule {
                    matches: vec![RouteMatch {
                        path: Some(PathMatch::empty_prefix()),
                        ..Default::default()
                    }],
                    backends: vec![BackendRef {
                        service,
                        port: Some(port),
                        weight: 1,
                    }],
                    ..Default::default()
                }],
            }
        };
        route.to_gateway_httproute(namespace).unwrap()
    }

    fn endpoint_slice(
        svc: &core_v1::Service,
        slice_name: &'static str,
        ports: impl IntoIterator<Item = (&'static str, u16)>,
        addrs: impl IntoIterator<Item = &'static str>,
    ) -> discovery_v1::EndpointSlice {
        let mut slice = discovery_v1::EndpointSlice::default();
        slice.meta_mut().namespace = svc.meta().namespace.clone();
        slice.meta_mut().name = Some(slice_name.to_string());

        // parent ref
        slice.labels_mut().insert(
            "kubernetes.io/service-name".to_string(),
            svc.meta().name.clone().unwrap(),
        );

        slice.address_type = "IPv4".to_string();
        for addr in addrs {
            let endpoint = discovery_v1::Endpoint {
                addresses: vec![addr.to_string()],
                conditions: Some(discovery_v1::EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            };
            slice.endpoints.push(endpoint);
        }

        for (name, port) in ports {
            let endpoint_port = discovery_v1::EndpointPort {
                name: Some(name.to_string()),
                port: Some(port as i32),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            };
            slice.ports.get_or_insert_with(Vec::new).push(endpoint_port);
        }

        slice
    }

    fn clusterip_service(
        namespace: &'static str,
        name: &'static str,
        ports: &'static [(&'static str, u16, u16)],
    ) -> core_v1::Service {
        let mut svc = core_v1::Service::default();
        svc.meta_mut().name = Some(name.to_string());
        svc.meta_mut().namespace = Some(namespace.to_string());

        let spec = svc.spec.get_or_insert_with(Default::default);
        spec.type_ = Some("ClusterIP".to_string());

        let mut svc_ports = vec![];
        for &(name, port, target_port) in ports {
            svc_ports.push(core_v1::ServicePort {
                name: Some(name.to_string()),
                port: port as i32,
                target_port: Some(IntOrString::Int(target_port as i32)),
                ..Default::default()
            });
        }
        spec.ports = Some(svc_ports);

        svc
    }

    fn externalname_service(
        namespace: &'static str,
        name: &'static str,
        hostname: &'static str,
    ) -> core_v1::Service {
        let mut svc = core_v1::Service::default();
        svc.meta_mut().name = Some(name.to_string());
        svc.meta_mut().namespace = Some(namespace.to_string());

        let spec = svc.spec.get_or_insert_with(Default::default);
        spec.type_ = Some("ExternalName".to_string());
        spec.external_name = Some(hostname.to_string());

        svc
    }
}
