use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
};

use junction_api::kube::{gateway_api, k8s_openapi};
use junction_api::{BackendId, VirtualHost};

use gateway_api::apis::experimental::httproutes::HTTPRoute;
use junction_api::{backend::Backend, http::Route, Target};
use k8s_openapi::{
    api::{
        core::v1::Service,
        discovery::v1::{Endpoint, EndpointSlice},
    },
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
    xds::{ResourceSnapshot, ResourceType, SnapshotWriter, VersionCounter},
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

/// Create a ServiceTarget with no ports from a Serivce
fn to_service_target(obj_ref: &ObjectRef<Service>) -> Result<Target, IngestError> {
    let namespace = obj_ref
        .namespace
        .as_deref()
        .ok_or_else(|| IngestError::InvalidObject {
            obj_ref: obj_ref.to_string(),
            message: "missing namespace".to_string(),
        })?;

    Ok(Target::kube_service(namespace, &obj_ref.name)?)
}

pub(crate) async fn run(
    snapshot_writer: SnapshotWriter,
    services: Watch<Service>,
    routes: Watch<HTTPRoute>,
    slices: Watch<EndpointSlice>,
) -> Infallible {
    let watches = Watches {
        services,
        routes,
        slices,
    };
    let version_counter = VersionCounter::with_process_prefix();
    let index = IngestIndex::default();

    run_with(snapshot_writer, watches, version_counter, index).await
}

async fn run_with(
    mut snapshot_writer: SnapshotWriter,
    watches: Watches,
    version_counter: VersionCounter,
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
                        &version_counter,
                        svcs,
                        |snapshot, svc| { index.service_changed(snapshot, &watches.services.store, svc) },
                    );
                }
            }
            routes = routes.recv() => {
                if let Ok(routes) = routes {
                    batch_snapshot(
                        &mut snapshot_writer,
                        &version_counter,
                        routes,
                        |snapshot, route| { index.httproute_changed(snapshot, &watches.routes.store, &watches.services.store, route) },
                    );
                }
            }
            slices = slices.recv() => {
                if let Ok(slices) = slices {
                    compute_snapshot(
                        &mut snapshot_writer,
                        &version_counter,
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

fn batch_snapshot<K, F>(
    writer: &mut SnapshotWriter,
    version_counter: &VersionCounter,
    changed_objects: ChangedObjects<K>,
    mut f: F,
) where
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

    write_snapshot(writer, version_counter, snapshot);
}

fn compute_snapshot<K, F>(
    writer: &mut SnapshotWriter,
    version_counter: &VersionCounter,
    changed_objects: ChangedObjects<K>,
    mut f: F,
) where
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

    write_snapshot(writer, version_counter, snapshot);
}

fn write_snapshot(
    writer: &mut SnapshotWriter,
    version_counter: &VersionCounter,
    snapshot: ResourceSnapshot,
) {
    if snapshot.is_empty() {
        return;
    }

    let version = version_counter.next();
    let updates = snapshot.update_counts();
    let deletes = snapshot.delete_counts();
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
    writer.update(version, snapshot);
}

fn endpoint_slice_services<'a>(
    slices: impl IntoIterator<Item = &'a RefAndParents<EndpointSlice>>,
) -> HashSet<ObjectRef<Service>> {
    let mut parents = HashSet::new();
    for slice in slices {
        parents.extend(slice.parents.iter().cloned());
    }
    parents
}

struct Watches {
    services: Watch<Service>,
    routes: Watch<HTTPRoute>,
    slices: Watch<EndpointSlice>,
}

#[derive(Debug, Default)]
struct IngestIndex {
    // track the mapping between HTTPRoutes and the Routes they generate for deletes
    route_targets: HashMap<ObjectRef<HTTPRoute>, VirtualHost>,
    // an inverted index from vhost to the service that it's an implicit route for.
    implicit_targets: HashMap<VirtualHost, ObjectRef<Service>>,
    // an index of Service to [BackendId], updated every time a Service is seen.
    // tracked so that on update its possible to know which Backends to remove.
    service_targets: HashMap<ObjectRef<Service>, HashSet<BackendId>>,
    // an index tracking which CLAs we've generated for a Service. this has to
    // be tracked independently of service_targets because while kube SHOULD
    // guarantee that they match, we get the information about creates/deletes
    // at separate times and have to keep the world sane on our own.
    cla_targets: HashMap<ObjectRef<Service>, HashSet<BackendId>>,
    // the set of Routes created by Services
    implicit_routes: BTreeSet<VirtualHost>,
    // the set of Routes created by HTTPRoutes
    explicit_routes: BTreeSet<VirtualHost>,
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
        services: &Store<Service>,
        route_ref: &ObjectRef<HTTPRoute>,
    ) -> Result<(), IngestError> {
        match routes.get(route_ref) {
            Some(http_route) => {
                let route = Route::from_gateway_httproute(&http_route)?;
                let listener = api_listener(route.to_xds());
                let xds = into_any!(listener);

                snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                self.implicit_routes.remove(&route.vhost);
                self.explicit_routes.insert(route.vhost.clone());

                let old_target = self
                    .route_targets
                    .insert(route_ref.clone(), route.vhost.clone());

                match old_target {
                    Some(old_target) if old_target != route.vhost => {
                        snapshot.insert_delete(ResourceType::Listener, old_target.name());
                        self.explicit_routes.remove(&old_target);
                    }
                    _ => (),
                }
            }
            None => {
                if let Some(vhost) = self.route_targets.remove(route_ref) {
                    snapshot.insert_delete(ResourceType::Listener, vhost.name());
                    self.explicit_routes.remove(&vhost);

                    // if this vhost maps back to a service and that service
                    // exists right now, create a Route for it.
                    if let Some(svc_ref) = self.implicit_targets.get(&vhost) {
                        if let Some(svc) = services.get(svc_ref) {
                            let backends = Backend::from_service(&svc).unwrap();
                            let vhost = implicit_vhost(svc_ref, &backends)?;

                            let route = implicit_route(&vhost.target);
                            let listener = api_listener(route.to_xds());
                            let xds = into_any!(listener);

                            self.implicit_routes.insert(vhost.clone());
                            snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                        }
                    }
                }
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
    fn service_changed(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        store: &Store<Service>,
        svc_ref: &ObjectRef<Service>,
    ) -> Result<(), IngestError> {
        match store.get(svc_ref).as_ref() {
            Some(svc) => {
                let backends = Backend::from_service(svc)?;
                let mut old_targets = self.service_targets.remove(svc_ref).unwrap_or_default();
                let mut new_targets = HashSet::with_capacity(backends.len());

                for backend in &backends {
                    old_targets.remove(&backend.id);
                    new_targets.insert(backend.id.clone());

                    // update the Cluster for this backend.
                    let cluster = backend.to_xds_cluster();
                    let xds = into_any!(cluster);
                    snapshot.insert_update(ResourceType::Cluster, cluster.name, xds);

                    // update the passthrough listener
                    let listener = api_listener(backend.to_xds_passthrough_route());
                    let xds = into_any!(listener);
                    snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                }

                // clean up old targets and update the index
                for target in old_targets {
                    snapshot.insert_delete(ResourceType::Cluster, target.name());
                }
                if !new_targets.is_empty() {
                    self.service_targets.insert(svc_ref.clone(), new_targets);
                }

                // if an explicit route hasn't been created for this service,
                // create an implicit route
                //
                // TODO: why isn't this tracked in the service_targets index?
                // should it be?  that would make deletes easier, and would mean
                // we can ditch to_service_target(...) here.
                let implicit_vhost = implicit_vhost(svc_ref, &backends)?;
                if !self.explicit_routes.contains(&implicit_vhost) {
                    let route = implicit_route(&implicit_vhost.target);
                    let listener = api_listener(route.to_xds());
                    let xds = into_any!(listener);

                    snapshot.insert_update(ResourceType::Listener, listener.name, xds);
                    self.implicit_routes.insert(implicit_vhost.clone());
                    self.implicit_targets
                        .insert(implicit_vhost, svc_ref.clone());
                }
            }
            None => {
                // remove the svc from the index
                let targets = self.service_targets.remove(svc_ref).unwrap_or_default();

                // delete all of the Clusters for those targets and all of the
                // passthrough listeners
                for target in &targets {
                    snapshot.insert_delete(ResourceType::Cluster, target.name());
                    snapshot.insert_delete(ResourceType::Listener, target.passthrough_route_name());
                }

                // delete the implicit route if it exists
                let implicit_vhost = to_service_target(svc_ref)?.into_vhost(None);
                let removed = self.implicit_routes.remove(&implicit_vhost)
                    || self.implicit_targets.remove(&implicit_vhost).is_some();
                if removed {
                    snapshot.insert_delete(ResourceType::Listener, implicit_vhost.name());
                }
            }
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
    //
    // FIXME: there's a race here - the Services every EndpointSlice is attached to
    // need to exist before we can actual calculate CLAs because that requires the
    // reverse mapping from `targetPort` back to `port`. On the rare occasion we
    // lose the race, there should be some way to infer the mapping (or assume that
    // port == targetPort) and then recompute everything when the Service appears.
    fn endpoints_changed<'a, I>(
        &mut self,
        snapshot: &mut ResourceSnapshot,
        svc_store: &Store<Service>,
        slice_store: &Store<EndpointSlice>,
        services: I,
    ) -> Result<(), Vec<IngestError>>
    where
        I: IntoIterator<Item = &'a ObjectRef<Service>>,
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

            let Ok(target) = to_service_target(svc_ref) else {
                continue;
            };

            let mut new_targets = HashSet::new();
            let mut old_targets = self.cla_targets.remove(svc_ref).unwrap_or_default();

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

fn implicit_vhost(
    svc_ref: &ObjectRef<Service>,
    backends: &[Backend],
) -> Result<VirtualHost, IngestError> {
    match backends {
        [] => Err(IngestError::InvalidObject {
            obj_ref: svc_ref.to_string(),
            message: "Service has no backends".to_string(),
        }),
        [backend] => Ok(backend.id.target.clone().into_vhost(None)),
        [first, rest @ ..] => {
            for backend in rest {
                if backend.id.target != first.id.target {
                    return Err(IngestError::InvalidObject {
                        obj_ref: svc_ref.to_string(),
                        message: "Service backends have more than one Target".to_string(),
                    });
                }
            }
            Ok(first.id.target.clone().into_vhost(None))
        }
    }
}

// a passthrough route that forces a port
fn implicit_route(target: &Target) -> Route {
    let vhost = target.clone().into_vhost(None);

    let mut route = Route::passthrough_route(vhost);
    route.tags = BTreeMap::from_iter([(
        junction_api::http::tags::GENERATED_BY.to_string(),
        "ezbake".to_string(),
    )]);

    route
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
    svc_target: &Target,
    port_lookup: &HashMap<(String, u16), u16>,
    endpoint_slices: impl Iterator<Item = &'a EndpointSlice>,
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
            let backend = svc_target.clone().into_backend(port);
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
    svc_ref: &ObjectRef<Service>,
    svc: &Service,
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

fn endpoint_slice_ports(slice: &EndpointSlice) -> Vec<(String, u16)> {
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

fn is_endpoint_ready(endpoint: &Endpoint) -> bool {
    endpoint.conditions.as_ref().map_or(false, |conditions| {
        let ready = conditions.ready.unwrap_or(false);
        conditions.serving.unwrap_or(ready)
    })
}

// FIXME: need many, many more tests here.
#[cfg(test)]
mod test {
    use k8s_openapi::{
        api::{
            core::v1::ServicePort,
            discovery::v1::{EndpointConditions, EndpointPort},
        },
        apimachinery::pkg::util::intstr::IntOrString,
    };
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
        insert(&mut svc_writer, svc);

        let target = Target::kube_service("prod", "coolsvc").unwrap();

        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    target.clone().into_backend(443).name(),
                    target.clone().into_backend(80).name(),
                ],
                vec![],
            ),
            "Cluster updates",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    target.clone().into_vhost(None).name(),
                    target.clone().into_backend(443).passthrough_route_name(),
                    target.clone().into_backend(80).passthrough_route_name(),
                ],
                vec![]
            ),
            "Listener updates",
        );
    }

    #[test]
    fn test_new_externalname_service() {
        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();

        let svc = externalname_service("prod", "coolsvc", "api.junctionlabs.io");
        let svc_ref = ObjectRef::from_obj(&svc);
        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let target = Target::dns("api.junctionlabs.io").unwrap();

        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    target.clone().into_backend(443).name(),
                    target.clone().into_backend(80).name(),
                ],
                vec![],
            ),
            "Cluster updates",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    target.clone().into_vhost(None).name(),
                    target.clone().into_backend(443).passthrough_route_name(),
                    target.clone().into_backend(80).passthrough_route_name(),
                ],
                vec![]
            ),
            "Listener updates",
        );
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
        let svc_target = to_service_target(&svc_ref).unwrap();

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc.clone());

        let mut index = IngestIndex::default();
        index
            .service_changed(&mut ResourceSnapshot::new(), &svc_store, &svc_ref)
            .unwrap();

        delete(&mut svc_writer, svc);

        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![],
                vec![
                    svc_target.clone().into_backend(443).name(),
                    svc_target.clone().into_backend(80).name()
                ],
            ),
            "service delete a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![],
                vec![
                    svc_target.clone().into_vhost(None).name(),
                    svc_target
                        .clone()
                        .into_backend(443)
                        .passthrough_route_name(),
                    svc_target.clone().into_backend(80).passthrough_route_name(),
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

        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert!(snapshot.is_empty(), "should do nothing");
    }

    // creating a new service after an HTTPRoute exists for it shouldn't create
    // implicit Routes for it.
    #[test]
    fn test_new_service_route_exists() {
        let svc = clusterip_service(
            "coolsvc",
            "prod",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let svc_target = to_service_target(&svc_ref).unwrap();

        let httproute = example_route("coolsvc-pass", "prod", svc_target.clone().into_vhost(None));
        let httproute_ref = ObjectRef::from_obj(&httproute);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute);

        let mut index = IngestIndex::default();

        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &svc_store, &httproute_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (vec![svc_target.clone().into_vhost(None).name()], vec![]),
            "route should create a Listener",
        );

        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    svc_target.clone().into_backend(443).name(),
                    svc_target.clone().into_backend(80).name()
                ],
                vec![],
            ),
            "service should create a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    svc_target
                        .clone()
                        .into_backend(443)
                        .passthrough_route_name(),
                    svc_target.clone().into_backend(80).passthrough_route_name(),
                ],
                vec![]
            ),
            "service should only create LB passthrough listeners",
        );
    }

    // creating an explicit route for a Service should delete any implicit
    // routes that were already created.
    #[test]
    fn test_new_route_service_exists() {
        let svc = clusterip_service(
            "coolsvc",
            "prod",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_ref = ObjectRef::from_obj(&svc);
        let svc_target = to_service_target(&svc_ref).unwrap();

        let httproute = example_route("coolsvc-pass", "prod", svc_target.clone().into_vhost(None));
        let httproute_ref = ObjectRef::from_obj(&httproute);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute);

        let mut index = IngestIndex::default();

        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();

        assert!(
            index
                .implicit_routes
                .contains(&svc_target.clone().into_vhost(None)),
            "vhost should tracked as having an implicit route"
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Cluster),
            (
                vec![
                    svc_target.clone().into_backend(443).name(),
                    svc_target.clone().into_backend(80).name()
                ],
                vec![],
            ),
            "service should create a Cluster for each port",
        );
        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (
                vec![
                    svc_target.name(),
                    svc_target
                        .clone()
                        .into_backend(443)
                        .passthrough_route_name(),
                    svc_target.clone().into_backend(80).passthrough_route_name(),
                ],
                vec![]
            ),
            "service create passthrough and implicit routes",
        );

        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &svc_store, &httproute_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (vec![svc_target.clone().into_vhost(None).name()], vec![]),
            "route should create a Listener that overrides the passthrough",
        );
        assert!(
            !index
                .implicit_routes
                .contains(&svc_target.clone().into_vhost(None)),
            "vhost should not be tracked as having an implicit route"
        );
    }

    #[test]
    fn test_delete_route_cluserip_service_exists() {
        let svc = clusterip_service(
            "prod",
            "coolsvc",
            &[("http", 80, 8009), ("https", 443, 8010)],
        );
        let svc_target = Target::kube_service("prod", "coolsvc").unwrap();

        test_delete_route(
            svc_target.clone(),
            svc,
            // initial update should create HTTPRoute listener and config listeners
            vec![
                svc_target.name(),
                svc_target
                    .clone()
                    .into_backend(443)
                    .passthrough_route_name(),
                svc_target.clone().into_backend(80).passthrough_route_name(),
            ],
            // deleting the HTTProute should update the named listener
            vec![svc_target.name()],
        );
    }

    #[test]
    fn test_delete_route_externalname_service_exists() {
        let svc = externalname_service("prod", "coolsvc", "coolapi.com");
        let svc_target = Target::dns("coolapi.com").unwrap();

        test_delete_route(
            svc_target.clone(),
            svc,
            // initial update should create HTTPRoute listener and config listeners
            vec![
                svc_target.name(),
                svc_target
                    .clone()
                    .into_backend(443)
                    .passthrough_route_name(),
                svc_target.clone().into_backend(80).passthrough_route_name(),
            ],
            // deleting the HTTProute should update the named listener
            vec![svc_target.name()],
        );
    }

    // deleting a route for an existing service should re-create the implicit
    // route for that service.
    fn test_delete_route(
        svc_target: Target,
        svc: Service,
        initial_updates: Vec<String>,
        updates_after_delete: Vec<String>,
    ) {
        let svc_ref = ObjectRef::from_obj(&svc);

        let httproute = example_route("prod", "example-route", svc_target.clone().into_vhost(None));
        let httproute_ref = ObjectRef::from_obj(&httproute);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc);

        let (route_store, mut route_writer) = kube::runtime::reflector::store();
        insert(&mut route_writer, httproute.clone());

        // create the index and insert both the service and route
        let mut index = IngestIndex::default();
        let mut snapshot = ResourceSnapshot::new();
        index
            .service_changed(&mut snapshot, &svc_store, &svc_ref)
            .unwrap();
        index
            .httproute_changed(&mut snapshot, &route_store, &svc_store, &httproute_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (initial_updates, vec![]),
            "passthrough and implicit routes were created",
        );

        // delete the route from the store
        delete(&mut route_writer, httproute);

        let mut snapshot = ResourceSnapshot::new();
        index
            .httproute_changed(&mut snapshot, &route_store, &svc_store, &httproute_ref)
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::Listener),
            (updates_after_delete, vec![]),
            "route should be replaced with the implicit route",
        )
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
        let svc_target = Target::kube_service("prod", "coolsvc").unwrap();
        let svc_ref = ObjectRef::from_obj(&svc);

        let (svc_store, mut svc_writer) = kube::runtime::reflector::store();
        insert(&mut svc_writer, svc.clone());

        let (slice_store, mut slice_writer) = kube::runtime::reflector::store();
        insert(&mut slice_writer, slice1.clone());
        insert(&mut slice_writer, slice2.clone());

        let mut index = IngestIndex::default();
        index
            .service_changed(&mut ResourceSnapshot::new(), &svc_store, &svc_ref)
            .unwrap();

        let mut snapshot = ResourceSnapshot::new();
        index
            .endpoints_changed(&mut snapshot, &svc_store, &slice_store, [&svc_ref])
            .unwrap();

        assert_eq!(
            snapshot.updates_and_deletes(ResourceType::ClusterLoadAssignment),
            (
                vec![
                    svc_target.clone().into_backend(443).name(),
                    svc_target.clone().into_backend(80).name()
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

    fn example_route(namespace: &'static str, name: &'static str, vhost: VirtualHost) -> HTTPRoute {
        Route::passthrough_route(vhost)
            .to_gateway_httproute(namespace, name)
            .unwrap()
    }

    fn endpoint_slice(
        svc: &Service,
        slice_name: &'static str,
        ports: impl IntoIterator<Item = (&'static str, u16)>,
        addrs: impl IntoIterator<Item = &'static str>,
    ) -> EndpointSlice {
        let mut slice = EndpointSlice::default();
        slice.meta_mut().namespace = svc.meta().namespace.clone();
        slice.meta_mut().name = Some(slice_name.to_string());

        // parent ref
        slice.labels_mut().insert(
            "kubernetes.io/service-name".to_string(),
            svc.meta().name.clone().unwrap(),
        );

        slice.address_type = "IPv4".to_string();
        for addr in addrs {
            let endpoint = Endpoint {
                addresses: vec![addr.to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            };
            slice.endpoints.push(endpoint);
        }

        for (name, port) in ports {
            let endpoint_port = EndpointPort {
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
    ) -> Service {
        let mut svc = Service::default();
        svc.meta_mut().name = Some(name.to_string());
        svc.meta_mut().namespace = Some(namespace.to_string());

        let spec = svc.spec.get_or_insert_with(Default::default);
        spec.type_ = Some("ClusterIP".to_string());

        let mut svc_ports = vec![];
        for &(name, port, target_port) in ports {
            svc_ports.push(ServicePort {
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
    ) -> Service {
        let mut svc = Service::default();
        svc.meta_mut().name = Some(name.to_string());
        svc.meta_mut().namespace = Some(namespace.to_string());

        let spec = svc.spec.get_or_insert_with(Default::default);
        spec.type_ = Some("ExternalName".to_string());
        spec.external_name = Some(hostname.to_string());

        svc
    }
}
