use std::collections::{BTreeMap, BTreeSet};

use enum_map::EnumMap;
use smol_str::{SmolStr, ToSmolStr};
use xds_api::pb::envoy::{
    config::core::v3 as xds_node,
    service::discovery::v3::{DeltaDiscoveryRequest, DeltaDiscoveryResponse, Resource},
};

use crate::xds::is_delta_nack;

use super::{cache::VersionedProto, server::SubInfo, ResourceType, SnapshotCache};

// NOTES: two big things we can do to make this saner and probably also allocate
// less:
//
// - parse ResourceVersions on incoming messages.
// - store xds Resources in cache already. there's nothing (yet?) that gets
//   changed on each response that would involve setting a new resource.

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
pub(crate) struct AdsConnection {
    node: xds_node::Node,
    nonce: u64,
    snapshot: SnapshotCache,
    subscriptions: EnumMap<ResourceType, Option<AdsSubscription>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AdsSubscription {
    // true iff this is a wildcard subscription.
    //
    // we currently ignore wildcards in sending responses, but since this is
    // a pretty key part of the protocol we track and check it.
    is_wildcard: bool,

    /// the nonce of the last reseponse sent
    last_sent_nonce: Option<SmolStr>,

    /// the last nonce a client successfully ACK'd
    last_ack_nonce: Option<SmolStr>,

    /// whether or not the client applied the last response
    applied: bool,

    /// the set of resources the client is subcribed to. this is be a superset
    /// of the keyset of sent - it includes anything we've sent to the client,
    /// and any resources the client is interested in that don't exist yet.
    subscribed: BTreeSet<SmolStr>,

    // the last version of each resource sent back to the client
    sent: BTreeMap<SmolStr, SmolStr>,

    // whether or not the cache has changed and resources need to
    // be rescanned.
    changed: bool,

    // the set of resources that need an update, whether or not the version of
    // the resources in cache has changed.
    sync: BTreeSet<SmolStr>,

    // the set of names that have been removed while in wildcard mode. if the
    // removed resource is part of the wildcard, we have to force re-send it,
    // otherwise we do another round of remove/ack with the client
    remove_wildcard: BTreeSet<SmolStr>,
}

impl AdsConnection {
    pub(crate) fn from_initial_request(
        request: &mut DeltaDiscoveryRequest,
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

    pub(crate) fn sent(&self) -> EnumMap<ResourceType, SubInfo> {
        let mut sent = EnumMap::default();

        for (rtype, sub) in &self.subscriptions {
            let Some(sub) = sub else {
                continue;
            };
            sent[rtype] = SubInfo {
                applied: sub.applied,
                sent: sub.sent.clone(),
            }
        }

        sent
    }

    pub(crate) fn ads_responses(&mut self) -> Vec<DeltaDiscoveryResponse> {
        let mut responses = Vec::with_capacity(ResourceType::all().len());
        for rtype in ResourceType::all() {
            responses.extend(self.ads_response_for(*rtype));
        }
        responses
    }

    fn ads_response_for(&mut self, rtype: ResourceType) -> Option<DeltaDiscoveryResponse> {
        let sub = self.subscriptions[rtype].as_mut()?;

        // get and clear subscription state. we should no longer have to touch
        // the subscription.
        let mut sync = std::mem::take(&mut sub.sync);
        let remove_wildcard = std::mem::take(&mut sub.remove_wildcard);
        let changed = sub.changed;
        sub.changed = false;

        // if the sub is marked as changed, scan the set of sent items to see
        // any of them need updating. pull these all from the pending set so
        // they're not updated twice.
        //
        // tracks the set of updated and removed items because we're iterating over
        // sub.sent and can't modify it in place.
        let mut to_update = BTreeMap::new();
        let mut to_remove = BTreeSet::new();

        let mut resources = vec![];
        let mut removed_resources = vec![];

        // TODO: actually check to see if there are wildcard resources for this
        // node. right now we assume there are never any.
        //
        // if there are, we have to combine them with the sent map somehow and
        // have to do the remove check differently.
        removed_resources.extend(remove_wildcard.into_iter().map(|s| s.into()));

        if changed {
            // TODO: include wildcard resources here
            for name in &sub.subscribed {
                // if we're already sending an update because the version
                // changed, we don't need to do it again, and if we're forcing
                // this, just do it anyway.
                let force = sync.remove(name);
                let last_version = sub.sent.get(name);
                match self.snapshot.get(rtype, name) {
                    Some(entry) => {
                        let VersionedProto { version, proto } = entry.value();

                        if force || Some(&version.to_smolstr()) != last_version {
                            resources.push(Resource {
                                name: name.to_string(),
                                version: version.to_string(),
                                resource: Some(proto.clone()),
                                ..Default::default()
                            });
                            to_update.insert(name.clone(), version.to_smolstr());
                        }
                    }
                    None => {
                        removed_resources.push(name.to_string());
                        to_remove.insert(name.clone());
                    }
                }
            }
        }

        // grab all pending names and send em as well
        for name in sync {
            match self.snapshot.get(rtype, &name) {
                Some(entry) => {
                    let name = entry.key();
                    let VersionedProto { version, proto } = entry.value();

                    resources.push(Resource {
                        name: name.to_string(),
                        version: version.to_string(),
                        resource: Some(proto.clone()),
                        ..Default::default()
                    });
                    to_update.insert(name.to_smolstr(), version.to_smolstr());
                }
                None => {
                    removed_resources.push(name.to_string());
                    to_remove.insert(name);
                }
            }
        }

        // update subscriptions in one go
        for (k, v) in to_update {
            sub.sent.insert(k, v);
        }
        for k in to_remove {
            sub.sent.remove(&k);
        }

        // don't send noop reponses
        if resources.is_empty() && removed_resources.is_empty() {
            return None;
        }

        // there's fundamentally a consistency issue here - since the snapshot can
        // change out from under us, we don't have a single version number that
        // truly represents the state of everything right now. just pick the highest
        // version number at the end for now - this is only intended for debugging
        // anyway.
        let snapshot_version = self.snapshot.version(rtype).to_string();

        let nonce = next_nonce(&mut self.nonce);
        sub.last_sent_nonce = Some(nonce.clone());

        Some(DeltaDiscoveryResponse {
            type_url: rtype.type_url().to_string(),
            nonce: nonce.to_string(),
            system_version_info: snapshot_version,
            resources,
            removed_resources,
            ..Default::default()
        })
    }

    pub(crate) fn handle_snapshot_update(&mut self, changed_type: ResourceType) {
        let Some(sub) = &mut self.subscriptions[changed_type] else {
            return;
        };
        sub.changed = true;
    }

    pub(crate) fn handle_ads_request(
        &mut self,
        mut request: DeltaDiscoveryRequest,
    ) -> Result<(), ConnectionError> {
        let Some(rtype) = ResourceType::from_type_url(&request.type_url) else {
            return Ok(());
        };

        // TODO: validate the request. the client verifies that *responses*
        // can't have duplicate resource names in add/remove, but there's no
        // explicit stipulation on what the client can send in a *request*.
        // it's probably safe to assume that we should be able to assume the
        // same - it's nonsensical to do otherwise. see:
        // https://github.com/envoyproxy/envoy/blob/2674bd9f5dfbfce3db55c4ed8c4c4aeda4b97823/test/extensions/config_subscription/grpc/delta_subscription_state_test.cc#L1153
        if false {
            todo!("request validation");
        }

        let sub = match &mut self.subscriptions[rtype] {
            // handle the initial request for this resource type.
            None => {
                // create a new sub
                let sub = self.subscriptions[rtype].get_or_insert_with(Default::default);

                // set initial resource versions and mark the sub as having state
                // changed so the next set of responses compares sent versions and
                // actual versions.
                let initial_resource_versions =
                    std::mem::take(&mut request.initial_resource_versions);
                for (name, version) in initial_resource_versions {
                    let name = name.to_smolstr();
                    let version = version.to_smolstr();
                    sub.subscribed.insert(name.clone());
                    sub.sent.insert(name, version);
                }
                sub.changed = true;
                sub.applied = true;

                // check to see if this is a new wildcard sub.
                if request.resource_names_subscribe.is_empty() {
                    sub.is_wildcard = true;
                }

                sub
            }
            // on any subsequent, check that initial_resource_versions is empty,
            // handle ACK/NACK bookeeping, and then return the sub.
            Some(sub) => {
                if !request.initial_resource_versions.is_empty() {
                    return Err(ConnectionError::InvalidRequest(anyhow::anyhow!(
                        "initial_resource_versions may only be set on initial requests"
                    )));
                }

                // check the nonce to see if this is an ACK/NACK. this is more
                // than go-control-plane seems to do. it sets nonces but
                // basically ignores them. delta/v3/server.go
                //
                // https://github.com/envoyproxy/go-control-plane/blob/main/pkg/server/delta/v3/server.go#L86-L124
                if let Some(request_nonce) = nonempty_then(&request.response_nonce, SmolStr::new) {
                    if Some(&request_nonce) == sub.last_sent_nonce.as_ref() {
                        match is_delta_nack(&request) {
                            //ACK
                            false => {
                                sub.applied = true;
                                sub.last_ack_nonce = Some(request_nonce);
                            }
                            // NACK
                            true => {
                                sub.applied = false;
                            }
                        }
                    }
                }

                sub
            }
        };

        // on subscribing, we register a name as pending *even if* it's already in the
        // sent set with the same version as is in cache, per the protocol.
        for name in request.resource_names_subscribe {
            if name == "*" {
                sub.is_wildcard = true;
                continue;
            }
            let name = name.to_smolstr();
            sub.remove_wildcard.remove(&name);
            sub.subscribed.insert(name.clone());
            sub.sync.insert(name);
        }
        // on unsubscribing, clear out all of the state for this name. if the
        // sub is currently in wildcard mode, toss it in the pile for special
        // handling on the next outgoing message.
        for name in request.resource_names_unsubscribe {
            let name = name.to_smolstr();
            sub.subscribed.remove(&name);
            sub.sync.remove(&name);
            sub.sent.remove(&name);

            if rtype.supports_wildcard() && sub.is_wildcard {
                sub.remove_wildcard.insert(name);
            }
        }

        Ok(())
    }
}

// not a method because borrowck is silly
fn next_nonce(nonce: &mut u64) -> SmolStr {
    *nonce = nonce.wrapping_add(1);
    nonce.to_smolstr()
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

#[cfg(test)]
mod test {

    use crate::xds::cache::ResourceVersion;
    use crate::xds::{ResourceSnapshot, SnapshotWriter};

    use super::*;
    use xds_api::pb::envoy::config::core::v3::{self as xds_core};
    use xds_api::pb::google::protobuf;

    macro_rules! request {
        ($rypte:expr) => {
            request($rypte, None, vec![], vec![], vec![], None)
        };
        ($rypte:expr, init = $init:expr) => {
            request($rypte, None, $init, vec![], vec![], None)
        };
        ($rypte:expr, add = $add:expr) => {
            request($rypte, None, vec![], $add, vec![], None)
        };
        ($rypte:expr, n = $nonce:expr, add = $add:expr) => {
            request($rypte, Some($nonce), vec![], $add, vec![], None)
        };
        ($rypte:expr, init = $init:expr, add = $add:expr) => {
            request($rypte, None, $init, $add, vec![], None)
        };
        ($rypte:expr, remove = $remove:expr) => {
            request($rypte, None, vec![], vec![], $remove, None)
        };
        ($rypte:expr, n = $nonce:expr) => {
            request($rypte, Some($nonce), vec![], vec![], vec![], None)
        };
        ($rypte:expr, n = $nonce:expr, err = $err:expr) => {
            request($rypte, $nonce, vec![], vec![], vec![], Some($err))
        };
        ($rypte:expr, n = $nonce:expr, init = $init:expr, add = $add:expr, remove = $remove:expr) => {
            request($rypte, $nonce, $init, $add, $remove, None)
        };
    }

    #[test]
    fn test_xds_init_no_data() {
        let (_, snapshot) = new_snapshot([]);
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        // with new wildcard subscription should not respond
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(ResourceType::Listener))
            .unwrap();
        // with an explicit subscription to missing, should NACK
        conn.handle_ads_request(request!(ResourceType::Cluster, add = vec!["example.com"]))
            .unwrap();
        // with initial versions, should respond with a removal
        conn.handle_ads_request(request!(
            ResourceType::RouteConfiguration,
            init = vec![("bar.com", "v2")]
        ))
        .unwrap();
        // new empty non-wildcard subscription, shouldn't respond. technically invalid?
        conn.handle_ads_request(request!(ResourceType::ClusterLoadAssignment))
            .unwrap();

        // should generate a CDS not-found and an RDS delete
        let responses = conn.ads_responses();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(
            responses[0].removed_resources,
            vec!["example.com".to_string()]
        );
        assert_eq!(
            responses[1].type_url,
            ResourceType::RouteConfiguration.type_url()
        );
        assert_eq!(responses[1].removed_resources, vec!["bar.com".to_string()]);
    }

    #[test]
    fn test_xds_init_with_data() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (version, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // all four types get init requests out of order.
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        // when all names and versions match, nothing to do
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            init = vec![
                ("nginx.default.svc.cluster.local:80", &version.to_string(),),
                (
                    "nginx-staging.default.svc.cluster.local:80",
                    &version.to_string(),
                ),
            ]
        ))
        .unwrap();
        // only one name matches, should get one resource back
        conn.handle_ads_request(request!(
            ResourceType::Cluster,
            init = vec![
                ("nginx.default.svc.cluster.local:80", &version.to_string()),
                ("nginx-staging.default.svc.cluster.local:80", "1111.2222"),
            ]
        ))
        .unwrap();
        // first message is a new resource request, but also includes initial
        // version that matches. should be sent anyway.
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            init = vec![("nginx.example.com", &version.to_string()),],
            add = vec!["nginx.example.com"]
        ))
        .unwrap();
        // empty message should continue to do nothing
        conn.handle_ads_request(request!(ResourceType::RouteConfiguration, init = vec![]))
            .unwrap();

        let responses = conn.ads_responses();

        // responses are ordered! this should correspond to ResourceType::all() with
        // any missing types omitted.
        let rtypes: Vec<ResourceType> = responses
            .iter()
            .filter_map(|r| ResourceType::from_type_url(&r.type_url))
            .collect();
        assert_eq!(&rtypes, &[ResourceType::Cluster, ResourceType::Listener,],);

        // CDS should respond with a single message containing only the out of
        // date resource
        assert_eq!(responses[0].resources.len(), 1);
        assert_eq!(
            responses[0].resources[0].name,
            "nginx-staging.default.svc.cluster.local:80"
        );

        // LDS response should contain the subscribed resource
        assert_eq!(responses[1].resources.len(), 1);
        assert_eq!(responses[1].resources[0].name, "nginx.example.com");
    }

    #[test]
    fn test_lds_ack() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (_, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // send a request
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            add = vec!["nginx.example.com"]
        ))
        .unwrap();

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an ACK
        conn.handle_ads_request(request!(ResourceType::Listener, n = &resp[0].nonce))
            .unwrap();

        let resp = conn.ads_responses();
        assert!(resp.is_empty());

        // track the ACK state
        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_nonce: Some(n1),
                    last_sent_nonce: Some(n2),
                    applied: true,
                    ..
                } if n1 == n2,
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

        let (_, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // send a request
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            add = vec!["nginx.example.com"]
        ))
        .unwrap();

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);

        // handle an NACK
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            n = Some(&resp[0].nonce),
            err = "you can't cut back on funding, you will regret this"
        ))
        .unwrap();

        assert!(conn.ads_responses().is_empty());

        // should track the NACK
        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_nonce: None,
                    last_sent_nonce: Some(_),
                    applied: false,
                    ..
                }
            ),
            "should track the NACK in the subscription: sub={sub:?}",
        );
    }

    #[test]
    fn test_lds_remove_subscription() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };
        let (_, snapshot) = new_snapshot([(ResourceType::Listener, vec!["nginx.example.com"])]);

        // send a request for a new subscription
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            add = vec!["nginx.example.com"]
        ))
        .unwrap();

        // should respond with the data and treat the resource as subscribed.
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);
        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(sub.sent.contains_key("nginx.example.com"));

        // handle the ACK
        conn.handle_ads_request(request!(ResourceType::Listener, n = &resp[0].nonce))
            .unwrap();
        let resp = conn.ads_responses();
        assert!(resp.is_empty());

        // send an unsubcribe to the server
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            remove = vec!["nginx.example.com"]
        ))
        .unwrap();

        // server should not generate a response, but should unsubscribe the
        // client.
        assert!(conn.ads_responses().is_empty());
        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(!sub.sent.contains_key("nginx.example.com"));
    }

    #[test]
    fn test_lds_remove_subscription_wildcard() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };
        let (_, snapshot) = new_snapshot([(ResourceType::Listener, vec!["nginx.example.com"])]);

        // send a request for a new subscription
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            add = vec!["*", "nginx.example.com"]
        ))
        .unwrap();

        // should respond with the data and treat the resource as subscribed as well as part
        // of the wildcard.
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].resources.len(), 1);
        let sub = conn.subscriptions[ResourceType::Listener].as_ref().unwrap();
        assert!(sub.is_wildcard);
        assert!(sub.sent.contains_key("nginx.example.com"));

        // handle the ACK
        conn.handle_ads_request(request!(ResourceType::Listener, n = &resp[0].nonce))
            .unwrap();
        let resp = conn.ads_responses();
        assert!(resp.is_empty());

        // send an unsubcribe to the server
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            remove = vec!["nginx.example.com"]
        ))
        .unwrap();

        // server should generate a response to indicate that the resource was not part
        // of the wildcard.
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].removed_resources, vec!["nginx.example.com"]);
    }

    #[test]
    fn test_lds_not_found() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (_, snapshot, mut writer) =
            new_snapshot_with_writer([(ResourceType::Listener, vec!["nginx.example.com"])]);

        // send a request
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Listener,
            add = vec!["new.example.com"]
        ))
        .unwrap();

        // should return a not-found
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url());
        assert_eq!(resp[0].removed_resources, vec!["new.example.com"]);

        // update the snapshot with the new resource
        let mut snapshot = ResourceSnapshot::new();
        snapshot.insert_update(
            ResourceType::Listener,
            "new.example.com".to_string(),
            anything(),
        );
        let next_version = writer.update(snapshot);
        conn.handle_snapshot_update(ResourceType::Listener);

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Listener.type_url(),);
        assert_eq!(resp[0].resources[0].name, "new.example.com");
        assert_eq!(resp[0].resources[0].version, next_version.to_string());
    }

    #[test]
    fn test_cds_handle_ack_as_update() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (_, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::Cluster,
            init = vec![("nginx.default.svc.cluster.local:80", "old-version-number")]
        ))
        .unwrap();

        // should update the cluster
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 1);
        assert_eq!(
            resp[0].resources[0].name,
            "nginx.default.svc.cluster.local:80"
        );

        // first ACK changes the subscription, which should generate a respoonse.
        conn.handle_ads_request(request!(
            ResourceType::Cluster,
            n = &resp[0].nonce,
            add = vec!["nginx-staging.default.svc.cluster.local:80"]
        ))
        .unwrap();

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].type_url, ResourceType::Cluster.type_url());
        assert_eq!(resp[0].resources.len(), 1);
        assert_eq!(
            resp[0].resources[0].name,
            "nginx-staging.default.svc.cluster.local:80"
        );

        // second ack shouldn't change anything
        conn.handle_ads_request(request!(ResourceType::Cluster, n = &resp[0].nonce))
            .unwrap();
        assert!(conn.ads_responses().is_empty());

        let sub = conn.subscriptions[ResourceType::Cluster].as_ref().unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_nonce: Some(n1),
                    last_sent_nonce: Some(n2),
                    applied: true,
                    ..
                } if n1 == n2,
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );
    }

    #[test]
    fn test_eds_remove_subscription() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (_, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // Initial EDS connection should return a a message for each EDS resource.
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            add = vec![
                "nginx.default.svc.cluster.local:80",
                "nginx-staging.default.svc.cluster.local:80",
            ]
        ))
        .unwrap();

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].resources.len(), 2);

        // ACK the response
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            n = &resp[0].nonce
        ))
        .unwrap();
        assert!(conn.ads_responses().is_empty());

        // remove a resource. no response is expected.
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            remove = vec!["nginx-staging.default.svc.cluster.local:80"]
        ))
        .unwrap();
        assert!(conn.ads_responses().is_empty());

        let sub = conn.subscriptions[ResourceType::ClusterLoadAssignment]
            .as_ref()
            .unwrap();
        assert!(
            matches!(
                sub,
                AdsSubscription {
                    last_ack_nonce: Some(n1),
                    last_sent_nonce: Some(n2),
                    applied: true,
                    ..
                } if n1 == n2,
            ),
            "should track the ACK in the subscription: sub={sub:?}",
        );
    }

    #[test]
    fn test_eds_not_found() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (version, snapshot, mut writer) = new_snapshot_with_writer([(
            ResourceType::ClusterLoadAssignment,
            vec![
                "nginx.default.svc.cluster.local:80",
                "nginx-staging.default.svc.cluster.local:80",
            ],
        )]);

        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            init = vec![("nginx.default.svc.cluster.local:80", &version.to_string())]
        ))
        .unwrap();
        assert!(conn.ads_responses().is_empty());

        // ask for something that does not exist
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            add = vec!["nginx-next.default.svc.cluster.local:80"]
        ))
        .unwrap();

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url()
        );
        assert_eq!(
            resp[0].removed_resources,
            vec!["nginx-next.default.svc.cluster.local:80"],
        );

        // create the resource and check that we now send an update for it.
        let mut snapshot = ResourceSnapshot::new();
        snapshot.insert_update(
            ResourceType::ClusterLoadAssignment,
            "nginx-next.default.svc.cluster.local:80".to_string(),
            anything(),
        );
        let next_version = writer.update(snapshot);
        conn.handle_snapshot_update(ResourceType::ClusterLoadAssignment);

        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url(),
        );
        assert_eq!(
            resp[0].resources[0].name,
            "nginx-next.default.svc.cluster.local:80",
        );
        assert_eq!(resp[0].resources[0].version, next_version.to_string());
    }

    #[test]
    fn test_eds_add_remove_add() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (version, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // Initial EDS connection
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            init = vec![("nginx.default.svc.cluster.local:80", &version.to_string())]
        ))
        .unwrap();

        assert!(conn.ads_responses().is_empty());

        // add, remove, and add again before we get a chance to reply
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            add = vec!["nginx-staging.default.svc.cluster.local:80"]
        ))
        .unwrap();
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            remove = vec!["nginx-staging.default.svc.cluster.local:80"]
        ))
        .unwrap();
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            add = vec!["nginx-staging.default.svc.cluster.local:80"]
        ))
        .unwrap();

        // should respond as if we only processed a single add
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url()
        );
        assert_eq!(resp[0].resources.len(), 1);
        assert_eq!(
            resp[0].resources[0].name,
            "nginx-staging.default.svc.cluster.local:80"
        );
    }

    #[test]
    fn test_eds_remove_add_remove() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (version, snapshot) = new_snapshot([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // Initial EDS connection
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            init = vec![("nginx.default.svc.cluster.local:80", &version.to_string())]
        ))
        .unwrap();

        assert!(conn.ads_responses().is_empty());

        // add, remove, and add again before we get a chance to reply
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            remove = vec!["nginx.default.svc.cluster.local:80"]
        ))
        .unwrap();
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            add = vec!["nginx.default.svc.cluster.local:80"]
        ))
        .unwrap();
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            remove = vec!["nginx.default.svc.cluster.local:80"]
        ))
        .unwrap();

        // should not respond on a remove
        assert_eq!(conn.ads_responses(), vec![]);
    }

    #[test]
    fn test_snapshot_update() {
        let node = xds_core::Node {
            id: "test-node".to_string(),
            ..Default::default()
        };

        let (version, snapshot, mut writer) = new_snapshot_with_writer([
            (ResourceType::Listener, vec!["nginx.example.com"]),
            (
                ResourceType::Cluster,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
            (
                ResourceType::ClusterLoadAssignment,
                vec![
                    "nginx.default.svc.cluster.local:80",
                    "nginx-staging.default.svc.cluster.local:80",
                ],
            ),
        ]);

        // Initial EDS connection
        let mut conn = AdsConnection::test_new(node.clone(), snapshot.clone());
        conn.handle_ads_request(request!(
            ResourceType::ClusterLoadAssignment,
            init = vec![
                ("nginx.default.svc.cluster.local:80", &version.to_string()),
                (
                    "nginx-staging.default.svc.cluster.local:80",
                    &version.to_string()
                )
            ]
        ))
        .unwrap();
        assert!(conn.ads_responses().is_empty());

        // update the snapshot and then notify the conn that it changed.
        let mut snapshot = ResourceSnapshot::new();
        snapshot.insert_update(
            ResourceType::ClusterLoadAssignment,
            "nginx.default.svc.cluster.local:80".to_string(),
            anything(),
        );
        let next_version = writer.update(snapshot);

        conn.handle_snapshot_update(ResourceType::ClusterLoadAssignment);

        // should return a single response for the changed resource
        let resp = conn.ads_responses();
        assert_eq!(resp.len(), 1);
        assert_eq!(
            resp[0].type_url,
            ResourceType::ClusterLoadAssignment.type_url(),
        );
        assert_eq!(
            resp[0].resources[0].name,
            "nginx.default.svc.cluster.local:80",
        );
        assert_eq!(resp[0].resources[0].version, next_version.to_string());
    }

    fn new_snapshot(
        data: impl IntoIterator<Item = (ResourceType, Vec<&'static str>)>,
    ) -> (ResourceVersion, SnapshotCache) {
        let (version, cache, _writer) = new_snapshot_with_writer(data);
        (version, cache)
    }

    fn new_snapshot_with_writer(
        data: impl IntoIterator<Item = (ResourceType, Vec<&'static str>)>,
    ) -> (ResourceVersion, SnapshotCache, SnapshotWriter) {
        let mut snapshot = ResourceSnapshot::new();
        for (rtype, names) in data {
            for name in names {
                snapshot.insert_update(rtype, name.to_string(), anything());
            }
        }

        let (cache, mut writer) = crate::xds::snapshot([]);
        let version = writer.update(snapshot);

        (version, cache, writer)
    }

    fn request(
        rtype: ResourceType,
        response_nonce: Option<&str>,
        init: Vec<(&str, &str)>,
        add: Vec<&str>,
        remove: Vec<&str>,
        error: Option<&str>,
    ) -> DeltaDiscoveryRequest {
        let type_url = rtype.type_url().to_string();
        let response_nonce = response_nonce.map(|s| s.to_string()).unwrap_or_default();
        let resource_names_subscribe = add.into_iter().map(|n| n.to_string()).collect();
        let resource_names_unsubscribe = remove.into_iter().map(|n| n.to_string()).collect();
        let initial_resource_versions = init
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let error_detail = error.map(|s| xds_api::pb::google::rpc::Status {
            code: tonic::Code::InvalidArgument.into(),
            message: s.to_string(),
            ..Default::default()
        });

        DeltaDiscoveryRequest {
            type_url,
            response_nonce,
            initial_resource_versions,
            resource_names_subscribe,
            resource_names_unsubscribe,
            error_detail,
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
