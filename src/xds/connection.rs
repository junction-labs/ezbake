use std::collections::{BTreeMap, BTreeSet};

use enum_map::EnumMap;
use smol_str::{SmolStr, ToSmolStr};
use xds_api::pb::envoy::{
    config::core::v3 as xds_node,
    service::discovery::v3::{DiscoveryRequest, DiscoveryResponse},
};

use crate::xds::cache::Snapshot;
use crate::xds::resources::ResourceType;

use super::cache::ResourceVersion;

pub(crate) struct AdsConnection {
    #[allow(unused)]
    node: xds_node::Node,
    nonce: u64,
    snapshot: Snapshot,
    subscriptions: EnumMap<ResourceType, Option<AdsSubscription>>,
}

/// The state of a subscription to an resource type, managed as part of an
/// [AdsConnection].
#[derive(Debug, Default)]
struct AdsSubscription {
    // the names that the client is subscribed to
    names: ResourceNames,

    // the version of the last response sent
    last_sent_version: Option<ResourceVersion>,

    // the nonce of the last reseponse sent
    last_sent_nonce: Option<SmolStr>,

    // the last version of each resource sent back to the client
    //
    // this isn't used to verify what resources were sent for LDS/CDS, but is
    // kept to track state for CSDS.
    sent: BTreeMap<SmolStr, SmolStr>,

    // whether or not the client applied the last response
    applied: bool,

    // the last version a client successfully ACK'd
    last_ack_version: Option<ResourceVersion>,

    // the last nonce a client successfully ACK'd
    last_ack_nonce: Option<SmolStr>,
}

impl AdsConnection {
    pub(crate) fn from_initial_request(
        mut request: DiscoveryRequest,
        snapshot: Snapshot,
    ) -> Result<(Self, Option<ResourceType>, Vec<DiscoveryResponse>), &'static str> {
        let node = match request.node.take() {
            Some(node) => node,
            None => return Err("missing node info"),
        };

        let mut connection = Self {
            nonce: 0,
            node,
            snapshot,
            subscriptions: EnumMap::default(),
        };

        let (rtype, responses) = connection.handle_ads_request(request);
        Ok((connection, rtype, responses))
    }

    pub(crate) fn handle_ads_request(
        &mut self,
        request: DiscoveryRequest,
    ) -> (Option<ResourceType>, Vec<DiscoveryResponse>) {
        // TODO: should anything else happen if there's an invalid type_url?
        let Some(rtype) = ResourceType::from_type_url(&request.type_url) else {
            return (None, Vec::new());
        };

        let sub = self.subscriptions[rtype].get_or_insert_with(AdsSubscription::default);

        // pull the request version and nonce and immediately verify that the
        // requests is not stale. bail out if it is.
        //
        // NOTE: should we actually validate that these are ostensibly resource
        // versions/nonces we produced? they're checked for matching but that's
        // it - is there a real benefit to telling a client it's behaving badly?
        let Ok(request_version) = nonempty_then(&request.version_info, |s| s.parse()).transpose()
        else {
            return (None, Vec::new());
        };
        let request_nonce = nonempty_then(&request.response_nonce, SmolStr::new);
        if request_nonce != sub.last_sent_nonce {
            return (None, Vec::new());
        }

        // if this isn't the initial request on a stream, update some state
        if request_nonce.is_some() {
            if is_nack(&request) {
                sub.applied = false;
            } else {
                sub.applied = true;
                sub.last_ack_nonce = request_nonce;
                // clone_from is here because clippy
                sub.last_ack_version.clone_from(&request_version);
            }
        }

        // updates should always go out if the version requested by the client
        // isn't the current version.
        let out_of_date = request_version != self.snapshot.version(rtype);

        // update the current subscription's resource names. if the names have
        // changed replace the current connection's names. send an update if
        // names change at all.
        let resource_names = ResourceNames::from_names(&sub.names, request.resource_names);
        let names_changed = sub.names != resource_names;
        if names_changed {
            sub.names = resource_names;
        }

        let mut responses = Vec::new();
        if out_of_date || names_changed {
            if rtype.group_responses() {
                responses = sub.sotw_update(&self.snapshot, &mut self.nonce, rtype)
            } else {
                responses = sub.incremental_update(&self.snapshot, &mut self.nonce, rtype);
            }
        }

        (Some(rtype), responses)
    }

    pub(crate) fn handle_snapshot_update(
        &mut self,
        changed_type: ResourceType,
    ) -> Vec<DiscoveryResponse> {
        let Some(sub) = &mut self.subscriptions[changed_type] else {
            return Vec::new();
        };

        if sub.last_sent_version == self.snapshot.version(changed_type) {
            return Vec::new();
        }

        if changed_type.group_responses() {
            sub.sotw_update(&self.snapshot, &mut self.nonce, changed_type)
        } else {
            sub.incremental_update(&self.snapshot, &mut self.nonce, changed_type)
        }
    }
}

fn nonempty_then<'a, F, T>(s: &'a str, f: F) -> Option<T>
where
    F: FnOnce(&'a str) -> T,
{
    if s.is_empty() {
        None
    } else {
        Some(f(s))
    }
}

impl AdsSubscription {
    // TODO: don't return an update if nothing has changed!
    fn sotw_update(
        &mut self,
        snapshot: &Snapshot,
        nonce: &mut u64,
        rtype: ResourceType,
    ) -> Vec<DiscoveryResponse> {
        if snapshot.len(rtype) == 0 {
            return Vec::new();
        }
        let Some(snapshot_version) = snapshot.version(rtype) else {
            return Vec::new();
        };

        let iter = snapshot_iter(rtype, &self.names, snapshot);
        let (size_hint, _) = iter.size_hint();
        let mut resources = Vec::with_capacity(size_hint);

        for entry in iter {
            self.sent
                .insert(entry.key().to_smolstr(), entry.value().version.to_smolstr());
            resources.push(entry.value().proto.clone());
        }

        let nonce = next_nonce(nonce);
        self.last_sent_nonce = Some(nonce.clone());
        self.last_sent_version = Some(snapshot_version);

        let version_info = snapshot_version.to_string();
        vec![DiscoveryResponse {
            type_url: rtype.type_url().to_string(),
            version_info,
            nonce: nonce.to_string(),
            resources,
            ..Default::default()
        }]
    }

    fn incremental_update(
        &mut self,
        snapshot: &Snapshot,
        nonce: &mut u64,
        rtype: ResourceType,
    ) -> Vec<DiscoveryResponse> {
        // grab the snapshot version ahead of time in case there's a concurrent
        // update while we're sending. better to be a little behind than a
        // little ahead.
        //
        // If the snapshot has no data yet, don't do anything.
        let Some(snapshot_version) = snapshot.version(rtype) else {
            return Vec::new();
        };

        let iter = snapshot_iter(rtype, &self.names, snapshot);
        let (size_hint, _) = iter.size_hint();

        let mut last_nonce = 0;
        let mut responses = Vec::with_capacity(size_hint);
        for entry in iter {
            let name = entry.key();
            let resource = entry.value();
            let resource_version = resource.version.to_smolstr();

            if self.sent.get(name.as_str()) != Some(&resource_version) {
                self.sent.insert(entry.key().to_smolstr(), resource_version);
                responses.push(DiscoveryResponse {
                    type_url: rtype.type_url().to_string(),
                    version_info: resource.version.to_string(),
                    nonce: next_nonce(nonce).to_string(),
                    resources: vec![resource.proto.clone()],
                    ..Default::default()
                });
                last_nonce = *nonce;
            }
        }

        self.last_sent_version = Some(snapshot_version);
        self.last_sent_nonce = Some(last_nonce.to_smolstr());

        responses
    }
}

#[inline]
fn next_nonce(nonce: &mut u64) -> SmolStr {
    *nonce = nonce.wrapping_add(1);
    nonce.to_smolstr()
}

#[inline]
pub(crate) fn is_nack(r: &DiscoveryRequest) -> bool {
    r.error_detail.is_some()
}

/// A set of XDS resource names for tracking connection state.
///
/// LDS and CDS have some extra-special wildcard handling that requires
/// differentiating between two different wildcard states to preserve backwards
/// compatibility.
///
/// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
#[derive(Clone, Debug, PartialEq, Eq)]
enum ResourceNames {
    EmptyWildcard,
    Wildcard(BTreeSet<String>),
    Explicit(BTreeSet<String>),
}

impl Default for ResourceNames {
    fn default() -> Self {
        Self::EmptyWildcard
    }
}

impl FromIterator<String> for ResourceNames {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let mut inner = BTreeSet::new();
        let mut wildcard = false;

        for name in iter {
            if name == "*" {
                wildcard = true;
            } else {
                inner.insert(name);
            }
        }

        if wildcard {
            Self::Wildcard(inner)
        } else {
            Self::Explicit(inner)
        }
    }
}

impl ResourceNames {
    fn from_names(previous: &Self, names: Vec<String>) -> Self {
        if names.is_empty() && matches!(previous, Self::EmptyWildcard) {
            Self::EmptyWildcard
        } else {
            Self::from_iter(names)
        }
    }
}

fn snapshot_iter<'n, 's>(
    resource_type: ResourceType,
    names: &'n ResourceNames,
    snapshot: &'s Snapshot,
) -> SnapshotIter<'n, 's> {
    match names {
        ResourceNames::EmptyWildcard | ResourceNames::Wildcard(_) => {
            SnapshotIter::Wildcard(snapshot.iter(resource_type))
        }
        ResourceNames::Explicit(names) => {
            SnapshotIter::Explicit(resource_type, names.iter(), snapshot)
        }
    }
}

enum SnapshotIter<'n, 's> {
    Wildcard(crossbeam_skiplist::map::Iter<'s, String, crate::xds::cache::VersionedProto>),
    Explicit(
        ResourceType,
        std::collections::btree_set::Iter<'n, String>,
        &'s Snapshot,
    ),
}

impl<'n, 's> Iterator for SnapshotIter<'n, 's> {
    type Item = crate::xds::cache::Entry<'s>;

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SnapshotIter::Wildcard(entries) => entries.next(),
            SnapshotIter::Explicit(rtype, names, snapshot) => {
                while let Some(name) = names.next() {
                    if let Some(entry) = snapshot.get(*rtype, name) {
                        return Some(entry);
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::xds::cache::ResourceVersion;

    use super::*;
    use xds_api::pb::envoy::config::core::v3::{self as xds_core};
    use xds_api::pb::google::protobuf;

    #[test]
    fn test_init_no_data() {
        let snapshot = new_snapshot([]);
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        // LDS and CDS should respond with no data
        let (_conn, _, responses) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Listener, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert!(responses.is_empty());

        let (_conn, _, responses) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Cluster, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert!(responses.is_empty());

        // EDS should return nothing
        let (_conn, _, responses) = AdsConnection::from_initial_request(
            discovery_request(
                ResourceType::ClusterLoadAssignment,
                node.clone(),
                "",
                "",
                vec![],
            ),
            snapshot.clone(),
        )
        .unwrap();
        assert!(responses.is_empty(), "EDS returns an no responses");
    }

    #[test]
    fn test_init_with_data() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        // LDS should respond with a single message containing one resource
        let (_, _, resp) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Listener, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // CDS shoudl respond with a single message containing both resources
        let (_, _, resp) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Cluster, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 2);

        // EDS should only fetch the requested resource
        let (_, _, resp) = AdsConnection::from_initial_request(
            discovery_request(
                ResourceType::ClusterLoadAssignment,
                node.clone(),
                "",
                "",
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 2);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url()
        );
        assert_eq!(resp[0].resources.len(), 1);
    }

    #[test]
    fn test_lds_ack() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        let (mut conn, _, resp) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Listener, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an ACK
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::Listener,
            &resp[0].version_info,
            &resp[0].nonce,
            vec![],
        ));
        assert!(resp.is_empty());

        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_version: Some(_),
                    last_ack_nonce: Some(_),
                    applied: true,
                    ..
                },
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );
    }

    #[test]
    fn test_handle_nack() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        let (mut conn, _, resp) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Listener, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an ACK
        let (_, resp) = conn.handle_ads_request(discovery_nack(
            ResourceType::Listener,
            &resp[0].version_info,
            &resp[0].nonce,
            vec![],
            "you can't cut back on funding, you will regret this",
        ));
        assert!(resp.is_empty());

        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_version: None,
                    last_ack_nonce: None,
                    applied: false,
                    ..
                }
            ),
            "should track the NACK in the subscription: sub={sub:?}",
        );
    }

    // TODO: what do we do about the case where a sub goes from r=[] (wildcard)
    // to r=[a, b, c] if a subset of [a, b, c] is all that exists in the
    // snapshot?  the client already has the current state of the world, but got
    // there with a different subscription.

    #[test]
    fn test_handle_ack_as_update_cds() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        let (mut conn, _, resp) = AdsConnection::from_initial_request(
            discovery_request(ResourceType::Cluster, node.clone(), "", "", vec![]),
            snapshot.clone(),
        )
        .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 2);

        // first ACK changes the subscriptions
        //
        // this should generate a new response, because Clusters are SoTW for
        // update and need to send a response that removes one of the resources.
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::Cluster,
            &resp[0].version_info,
            &resp[0].nonce,
            vec!["default/nginx-staging/cluster"],
        ));
        assert_eq!(resp.len(), 1, "should send back a SotW response");
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(
            resp[0].resources.len(),
            1,
            "response should include a single cluster"
        );

        let sub = conn.subscriptions[ResourceType::Cluster].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_version: Some(_),
                    last_ack_nonce: Some(_),
                    applied: true,
                    ..
                },
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );

        // second ACK shouldn't generate anything else, there's nothing to do
        // because the subscription hasn't changed.
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::Cluster,
            &resp[0].version_info,
            &resp[0].nonce,
            vec!["default/nginx-staging/cluster"],
        ));
        assert!(resp.is_empty());
    }

    #[test]
    fn test_eds_update_remove_subscription() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        // Initial EDS connection should return a a message for each EDS resource.
        let (mut conn, _, resp) = AdsConnection::from_initial_request(
            discovery_request(
                ResourceType::ClusterLoadAssignment,
                node.clone(),
                "",
                "",
                vec![],
            ),
            snapshot.clone(),
        )
        .unwrap();

        assert_eq!(resp.len(), 2);
        assert!(
            resp.iter()
                .all(|msg| msg.type_url == ResourceType::ClusterLoadAssignment.type_url()),
            "should be EDS resources",
        );
        assert!(
            resp.iter().all(|msg| msg.resources.len() == 1),
            "should contain a single response",
        );

        // first ACK changes the subscriptions
        //
        // this should generate a new response, because Clusters are SoTW for
        // update and need to send a response that removes one of the resources.
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::ClusterLoadAssignment,
            &resp[1].version_info,
            &resp[1].nonce,
            vec!["default/nginx-staging/endpoints"],
        ));
        assert_eq!(resp.len(), 0, "nothing has changed, shouldn't do anything");

        let sub = conn.subscriptions[ResourceType::ClusterLoadAssignment]
            .as_ref()
            .unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    names: ResourceNames::Explicit(_),
                    last_ack_version: Some(_),
                    last_ack_nonce: Some(_),
                    applied: true,
                    ..
                },
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );

        // second ACK shouldn't generate anything else, there's nothing to do
        // because the subscription hasn't changed.
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::ClusterLoadAssignment,
            "123",
            &conn.nonce.to_string(),
            vec!["default/nginx-staging/endpoints"],
        ));
        assert!(resp.is_empty());
    }

    #[test]
    fn test_eds_update_add_subscription() {
        let node = Some(xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        });

        let snapshot = new_snapshot([
            (ResourceType::Listener, 123, vec!["default/nginx"]),
            (
                ResourceType::Cluster,
                123,
                vec!["default/nginx/cluster", "default/nginx-staging/cluster"],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                123,
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ),
        ]);

        let (mut conn, _, resp) = AdsConnection::from_initial_request(
            discovery_request(
                ResourceType::ClusterLoadAssignment,
                node.clone(),
                "",
                "",
                vec!["default/nginx/endpoints"],
            ),
            snapshot.clone(),
        )
        .unwrap();

        // should return a single response
        assert_eq!(resp.len(), 1);
        assert!(
            resp.iter()
                .all(|msg| msg.type_url == ResourceType::ClusterLoadAssignment.type_url()),
            "should be EDS resources",
        );
        assert!(
            resp.iter().all(|msg| msg.resources.len() == 1),
            "should contain a single response",
        );

        let next_version = &resp[0].version_info;
        let next_nonce = &resp[0].nonce;

        // should ignore a stale incoming request
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::ClusterLoadAssignment,
            "",
            "",
            vec![
                "default/nginx-staging/endpoints",
                "default/nginx/endpoints",
                "stale/stale/stale",
                "stale/staler/stalest",
            ],
        ));
        assert!(resp.is_empty());

        // should handle the next request. incremental means returning
        // only the data for new names.
        let (_, resp) = conn.handle_ads_request(discovery_ack(
            ResourceType::ClusterLoadAssignment,
            next_version,
            next_nonce,
            vec!["default/nginx-staging/endpoints", "default/nginx/endpoints"],
        ));
        assert_eq!(resp.len(), 1);
        assert!(
            resp.iter()
                .all(|msg| msg.type_url == ResourceType::ClusterLoadAssignment.type_url()),
            "should be EDS resources",
        );
        assert!(
            resp.iter().all(|msg| msg.resources.len() == 1),
            "should contain a single response",
        );
    }

    fn new_snapshot(
        data: impl IntoIterator<Item = (ResourceType, u64, Vec<&'static str>)>,
    ) -> Snapshot {
        let (snapshot, mut writers) = crate::xds::new_snapshot();

        for (rtype, version, names) in data {
            let writer = writers
                .for_type(rtype)
                .expect("resource type specified twice");

            writer.update(
                ResourceVersion::from_raw_parts(0xBEEF, version),
                names
                    .into_iter()
                    .map(|name| (name.to_string(), Some(anything()))),
            );
        }

        snapshot
    }

    fn discovery_request(
        rtype: ResourceType,
        node: Option<xds_core::Node>,
        version_info: &str,
        response_nonce: &str,
        names: Vec<&'static str>,
    ) -> DiscoveryRequest {
        let names = names.into_iter().map(|n| n.to_string()).collect();
        DiscoveryRequest {
            type_url: rtype.type_url().to_string(),
            node,
            resource_names: names,
            version_info: version_info.to_string(),
            response_nonce: response_nonce.to_string(),
            ..Default::default()
        }
    }

    fn discovery_ack(
        rtype: ResourceType,
        version_info: &str,
        response_nonce: &str,
        names: Vec<&str>,
    ) -> DiscoveryRequest {
        let names = names.into_iter().map(|n| n.to_string()).collect();
        DiscoveryRequest {
            type_url: rtype.type_url().to_string(),
            resource_names: names,
            version_info: version_info.to_string(),
            response_nonce: response_nonce.to_string(),
            ..Default::default()
        }
    }

    fn discovery_nack(
        rtype: ResourceType,
        version_info: &str,
        response_nonce: &str,
        names: Vec<&str>,
        error_detail: &str,
    ) -> DiscoveryRequest {
        let names = names.into_iter().map(|n| n.to_string()).collect();
        DiscoveryRequest {
            type_url: rtype.type_url().to_string(),
            resource_names: names,
            version_info: version_info.to_string(),
            response_nonce: response_nonce.to_string(),
            error_detail: Some(xds_api::pb::google::rpc::Status {
                code: tonic::Code::InvalidArgument.into(),
                message: error_detail.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn anything() -> protobuf::Any {
        protobuf::Any {
            type_url: "type_url".to_string(),
            value: vec![],
        }
    }
}
