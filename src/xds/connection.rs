use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use enum_map::EnumMap;
use smol_str::{SmolStr, ToSmolStr};
use tracing::trace;
use xds_api::pb::envoy::{
    config::core::v3 as xds_node,
    service::discovery::v3::{DiscoveryRequest, DiscoveryResponse},
};

use crate::xds::resources::ResourceType;
use crate::xds::{cache::SnapshotCache, is_nack};

use super::cache::ResourceVersion;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnectionError {
    #[error("missing node info")]
    MisingNode,

    #[error("invalid request: {0}")]
    InvalidRequest(anyhow::Error),
}

impl ConnectionError {
    pub(crate) fn into_status(self) -> tonic::Status {
        tonic::Status::invalid_argument(self.to_string())
    }
}

#[derive(Clone, Default)]
pub(crate) struct ConnectionSnapshot {
    connections: Arc<SkipMap<ConnectionSnapshotKey, AdsConnectionInfo>>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ConnectionSnapshotKey {
    cluster: String,
    id: String,
    remote_addr: Option<SocketAddr>,
}

impl ConnectionSnapshotKey {
    fn new(node: &xds_node::Node, remote_addr: Option<SocketAddr>) -> Self {
        Self {
            id: node.id.clone(),
            cluster: node.cluster.clone(),
            remote_addr,
        }
    }
}

pub(crate) struct AdsConnectionInfo {
    node: xds_node::Node,
    subscriptions: EnumMap<ResourceType, Option<AdsSubscription>>,
}

impl ConnectionSnapshot {
    pub(crate) fn update(&self, conn: &AdsConnection, socket_addr: Option<SocketAddr>) {
        let key = ConnectionSnapshotKey::new(&conn.node, socket_addr);
        let node = conn.node.clone();

        self.connections.insert(
            key,
            AdsConnectionInfo {
                node,
                subscriptions: conn.subscriptions(),
            },
        );
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ConnectionSnapshotEntry> {
        self.connections.iter().map(ConnectionSnapshotEntry)
    }
}

pub(crate) struct ConnectionSnapshotEntry<'a>(
    crossbeam_skiplist::map::Entry<'a, ConnectionSnapshotKey, AdsConnectionInfo>,
);

impl ConnectionSnapshotEntry<'_> {
    pub(crate) fn node(&self) -> &xds_node::Node {
        &self.0.value().node
    }

    pub(crate) fn subscriptions(&self) -> impl Iterator<Item = (ResourceType, &AdsSubscription)> {
        let sub_map = &self.0.value().subscriptions;

        sub_map
            .iter()
            .filter_map(|(r, s)| Option::zip(Some(r), s.as_ref()))
    }
}

/// The state of a subscription to an resource type, managed as part of an
/// [AdsConnection].
#[derive(Clone, Debug, Default)]
pub(crate) struct AdsSubscription {
    /// the names that the client is subscribed to
    names: ResourceNames,

    /// the version of the last response sent
    pub(crate) last_sent_version: Option<ResourceVersion>,

    /// the nonce of the last reseponse sent
    pub(crate) last_sent_nonce: Option<SmolStr>,

    /// the last version of each resource sent back to the client
    ///
    /// this isn't used to verify what resources were sent for LDS/CDS, but is
    /// kept to track state for CSDS.
    pub(crate) sent: BTreeMap<SmolStr, SmolStr>,

    /// whether or not the client applied the last response
    pub(crate) applied: bool,

    /// the last version a client successfully ACK'd
    pub(crate) last_ack_version: Option<ResourceVersion>,

    /// the last nonce a client successfully ACK'd
    pub(crate) last_ack_nonce: Option<SmolStr>,
}

pub(crate) struct AdsConnection {
    node: xds_node::Node,
    nonce: u64,
    snapshot: SnapshotCache,
    subscriptions: EnumMap<ResourceType, Option<AdsSubscription>>,
}

impl AdsConnection {
    pub(crate) fn from_initial_request(
        request: &mut DiscoveryRequest,
        snapshot: SnapshotCache,
    ) -> Result<Self, ConnectionError> {
        match request.node.take() {
            Some(node) => Ok(Self {
                nonce: 0,
                node,
                snapshot,
                subscriptions: EnumMap::default(),
            }),
            None => Err(ConnectionError::MisingNode),
        }
    }

    #[cfg(test)]
    fn test_new(node: xds_node::Node, snapshot: SnapshotCache) -> Self {
        Self {
            nonce: 0,
            node,
            snapshot,
            subscriptions: EnumMap::default(),
        }
    }

    pub(crate) fn node(&self) -> &xds_node::Node {
        &self.node
    }

    pub(crate) fn subscriptions(&self) -> EnumMap<ResourceType, Option<AdsSubscription>> {
        self.subscriptions.clone()
    }

    pub(crate) fn handle_ads_request(
        &mut self,
        request: DiscoveryRequest,
    ) -> Result<(Option<ResourceType>, Vec<DiscoveryResponse>), ConnectionError> {
        macro_rules! empty_response {
            () => {
                Ok((None, Vec::new()))
            };
        }

        let Some(rtype) = ResourceType::from_type_url(&request.type_url) else {
            return Ok((None, Vec::new()));
        };

        let sub = self.subscriptions[rtype].get_or_insert_with(AdsSubscription::default);

        // pull the request version and nonce and immediately verify that the
        // requests is not stale. bail out if it is.
        //
        // NOTE: should we actually validate that these are ostensibly resource
        // versions/nonces we produced? they're checked for matching but that's
        // it - is there a real benefit to telling a client it's behaving badly?
        let request_version = nonempty_then(&request.version_info, ResourceVersion::from_str)
            .transpose()
            .map_err(|e| ConnectionError::InvalidRequest(e.into()))?;
        let request_nonce = nonempty_then(&request.response_nonce, SmolStr::new);
        if request_nonce != sub.last_sent_nonce {
            trace!(
                v = request.version_info,
                n = request.response_nonce,
                ty = request.type_url,
                r = ?request.resource_names,
                last_sent_nonce = %sub.last_sent_nonce.as_ref().unwrap_or(&"".to_smolstr()),
                "ignoring stale request",
            );
            return empty_response!();
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

        Ok((Some(rtype), responses))
    }

    pub(crate) fn handle_snapshot_update(
        &mut self,
        changed_type: ResourceType,
    ) -> Vec<DiscoveryResponse> {
        let Some(sub) = &mut self.subscriptions[changed_type] else {
            return Vec::new();
        };

        trace!(
            sub_last_sent_version = ?sub.last_sent_version,
            snapshot_version = ?self.snapshot.version(changed_type),
            ?changed_type,
            "snapshot updated",
        );
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
        snapshot: &SnapshotCache,
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
        snapshot: &SnapshotCache,
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

        let mut last_nonce = None;
        let mut responses = Vec::with_capacity(size_hint);
        for entry in iter {
            let name = entry.key();
            let resource = entry.value();
            let resource_version = resource.version.to_smolstr();

            if self.sent.get(name.as_str()) != Some(&resource_version) {
                self.sent.insert(entry.key().to_smolstr(), resource_version);
                responses.push(DiscoveryResponse {
                    type_url: rtype.type_url().to_string(),
                    version_info: snapshot_version.to_string(),
                    nonce: next_nonce(nonce).to_string(),
                    resources: vec![resource.proto.clone()],
                    ..Default::default()
                });
                last_nonce = Some(*nonce);
            }
        }

        if let Some(last_nonce) = last_nonce {
            self.last_sent_version = Some(snapshot_version);
            self.last_sent_nonce = Some(last_nonce.to_smolstr());
        }

        responses
    }
}

#[inline]
fn next_nonce(nonce: &mut u64) -> SmolStr {
    *nonce = nonce.wrapping_add(1);
    nonce.to_smolstr()
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
    snapshot: &'s SnapshotCache,
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
        &'s SnapshotCache,
    ),
}

impl<'s> Iterator for SnapshotIter<'_, 's> {
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

    use crate::xds::{ResourceSnapshot, SnapshotWriter};

    use super::*;
    use xds_api::pb::envoy::config::core::v3::{self as xds_core};
    use xds_api::pb::google::protobuf;

    #[test]
    fn test_xds_init_no_data() {
        let snapshot = new_snapshot([]);
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        // LDS and CDS should respond with no data
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, responses) = conn
            .handle_ads_request(discovery_request(ResourceType::Listener, "", "", vec![]))
            .unwrap();
        assert!(responses.is_empty());

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, responses) = conn
            .handle_ads_request(discovery_request(ResourceType::Cluster, "", "", vec![]))
            .unwrap();
        assert!(responses.is_empty());

        // EDS should return nothing
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, responses) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec![],
            ))
            .unwrap();
        assert!(responses.is_empty(), "EDS returns an no responses");
    }

    #[test]
    fn test_xds_init_with_data() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (124, "default/nginx/endpoints"),
                    (125, "default/nginx-staging/endpoints"),
                ],
            ),
        ]);

        // LDS should respond with a single message containing one resource
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(ResourceType::Listener, "", "", vec![]))
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // CDS shoudl respond with a single message containing both resources
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(ResourceType::Cluster, "", "", vec![]))
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 2);

        // EDS should only fetch the requested resource
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec!["default/nginx/endpoints", "default/nginx-staging/endpoints"],
            ))
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
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (125, "default/nginx-staging/endpoints"),
                    (124, "default/nginx/endpoints"),
                ],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(ResourceType::Listener, "", "", vec![]))
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an ACK
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::Listener,
                &resp[0].version_info,
                &resp[0].nonce,
                vec![],
            ))
            .unwrap();
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
    fn test_lds_nack() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (125, "default/nginx-staging/endpoints"),
                    (124, "default/nginx/endpoints"),
                ],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(ResourceType::Listener, "", "", vec![]))
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an ACK
        let (_, resp) = conn
            .handle_ads_request(discovery_nack(
                ResourceType::Listener,
                &resp[0].version_info,
                &resp[0].nonce,
                vec![],
                "you can't cut back on funding, you will regret this",
            ))
            .unwrap();
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

    #[test]
    fn test_cds_handle_ack_as_update() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (125, "default/nginx-staging/endpoints"),
                    (124, "default/nginx/endpionts"),
                ],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(ResourceType::Cluster, "", "", vec![]))
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 2);

        // first ACK changes the subscriptions
        //
        // this should generate a new response, because Clusters are SoTW for
        // update and need to send a response that removes one of the resources.
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::Cluster,
                &resp[0].version_info,
                &resp[0].nonce,
                vec!["default/nginx-staging/cluster"],
            ))
            .unwrap();
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
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::Cluster,
                &resp[0].version_info,
                &resp[0].nonce,
                vec!["default/nginx-staging/cluster"],
            ))
            .unwrap();
        assert!(resp.is_empty());
    }

    #[test]
    fn test_eds_ack() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([(
            ResourceType::ClusterLoadAssignment,
            vec![
                (123, "default/nginx/endpoints"),
                (124, "default/something-else/endpoints"),
            ],
        )]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec!["default/nginx/endpoints"],
            ))
            .unwrap();

        // should return a single response for the resource in cache
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url(),
            "should be an EDS resource",
        );
        assert_eq!(
            resp[0].version_info,
            snapshot
                .version(ResourceType::ClusterLoadAssignment)
                .unwrap()
                .to_string(),
        );
        let next_version = &resp[0].version_info;
        let next_nonce = &resp[0].nonce;

        // when the client ACKs the first response, it shouldn't change the state of the connection
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                next_version,
                next_nonce,
                vec!["default/nginx/endpoints"],
            ))
            .unwrap();

        assert!(resp.is_empty());

        let sub = conn.subscriptions[ResourceType::ClusterLoadAssignment]
            .as_ref()
            .unwrap();
        assert_eq!(
            sub.last_sent_nonce,
            Some(next_nonce.to_smolstr()),
            "nonce should not change"
        );
    }

    #[test]
    fn test_eds_update_remove_subscription() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (125, "default/nginx-staging/endpoints"),
                    (124, "default/nginx/endpionts"),
                ],
            ),
        ]);

        // Initial EDS connection should return a a message for each EDS resource.
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec![],
            ))
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
        let last_version = &resp[1].version_info;
        let last_nonce = &resp[1].nonce;

        // first ACK changes the subscriptions. shouldn't generate a response because
        // EDS doesn't require full sotw updates.
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::ClusterLoadAssignment,
                last_version,
                last_nonce,
                vec!["default/nginx-staging/endpoints"],
            ))
            .unwrap();
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
                }
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );
        assert_eq!(
            sub.last_sent_nonce,
            Some(last_nonce.to_smolstr()),
            "should still have the last nonce: sub={sub:#?}",
        );

        // second ACK shouldn't generate anything else, there's nothing to do
        // because the subscription hasn't changed.
        //
        // connection state shouldn't change
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::ClusterLoadAssignment,
                last_version,
                last_nonce,
                vec!["default/nginx-staging/endpoints"],
            ))
            .unwrap();
        assert!(resp.is_empty());

        let sub = conn.subscriptions[ResourceType::ClusterLoadAssignment]
            .as_ref()
            .unwrap();
        assert_eq!(sub.last_sent_nonce, Some(last_nonce.to_smolstr()));
    }

    #[test]
    fn test_eds_update_add_subscription() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let snapshot = new_snapshot([
            (ResourceType::Listener, vec![(121, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (127, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    (125, "default/nginx-staging/endpoints"),
                    (124, "default/nginx/endpoints"),
                ],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec!["default/nginx/endpoints"],
            ))
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
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec![
                    "default/nginx-staging/endpoints",
                    "default/nginx/endpoints",
                    "stale/stale/stale",
                    "stale/staler/stalest",
                ],
            ))
            .unwrap();
        assert!(resp.is_empty());

        // accept the ACK and generate no respones. last_sent_nonce shouldn't change or anything.
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::ClusterLoadAssignment,
                next_version,
                next_nonce,
                vec!["default/nginx/endpoints"],
            ))
            .unwrap();
        assert!(resp.is_empty());
        assert_eq!(
            conn.subscriptions[ResourceType::ClusterLoadAssignment]
                .as_ref()
                .unwrap()
                .last_sent_nonce,
            Some(next_nonce.to_smolstr())
        );

        // should handle the next request as a subscription update. incremental
        // means returning only the data for new names.
        let (_, resp) = conn
            .handle_ads_request(discovery_ack(
                ResourceType::ClusterLoadAssignment,
                next_version,
                next_nonce,
                vec!["default/nginx-staging/endpoints", "default/nginx/endpoints"],
            ))
            .unwrap();
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

    #[test]
    fn test_eds_snapshot_update_during_ack() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (snapshot, mut writer) = new_snapshot_with_writer([
            (ResourceType::Listener, vec![(123, "default/nginx")]),
            (
                ResourceType::Cluster,
                vec![
                    (123, "default/nginx/cluster"),
                    (123, "default/nginx-staging/cluster"),
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![(127, "default/nginx/endpoints")],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                "",
                "",
                vec!["default/nginx/endpoints"],
            ))
            .unwrap();

        // should return a single response for the resource in cache
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url(),
            "should be an EDS resource",
        );
        let next_version = &resp[0].version_info;
        let next_nonce = &resp[0].nonce;

        // update the snapshot with a new endpoint
        let mut snapshot = ResourceSnapshot::new();
        snapshot.insert_update(
            ResourceType::ClusterLoadAssignment,
            "some-endpoints".to_string(),
            anything(),
        );
        writer.update(snapshot);

        // when the client ACKs the first response, it shouldn't change the state of the connection
        let (_, resp) = conn
            .handle_ads_request(discovery_request(
                ResourceType::ClusterLoadAssignment,
                next_version,
                next_nonce,
                vec!["default/nginx/endpoints"],
            ))
            .unwrap();

        assert!(resp.is_empty());

        let sub = conn.subscriptions[ResourceType::ClusterLoadAssignment]
            .as_ref()
            .unwrap();
        assert_eq!(
            sub.last_sent_nonce,
            Some(next_nonce.to_smolstr()),
            "nonce should not change"
        );
    }

    fn new_snapshot(
        data: impl IntoIterator<Item = (ResourceType, Vec<(u64, &'static str)>)>,
    ) -> SnapshotCache {
        let (cache, _writer) = new_snapshot_with_writer(data);
        cache
    }

    fn new_snapshot_with_writer(
        data: impl IntoIterator<Item = (ResourceType, Vec<(u64, &'static str)>)>,
    ) -> (SnapshotCache, SnapshotWriter) {
        let mut snapshot = ResourceSnapshot::new();
        let mut max_version = 0;
        for (rtype, mut names) in data {
            names.sort_by_key(|(v, _)| *v);
            for (version, name) in names {
                max_version = u64::max(max_version, version);
                snapshot.insert_update(rtype, name.to_string(), anything());
            }
        }

        let (cache, mut writer) = crate::xds::snapshot([]);
        writer.update(snapshot);

        (cache, writer)
    }

    fn discovery_request(
        rtype: ResourceType,
        version_info: &str,
        response_nonce: &str,
        names: Vec<&'static str>,
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
