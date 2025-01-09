mod cache;
mod delta;
mod resources;
mod server;
mod sotw;

use std::collections::BTreeSet;

pub(crate) use cache::{
    snapshot, ResourceSnapshot, SnapshotCache, SnapshotCallback, SnapshotWriter,
};
pub(crate) use resources::ResourceType;
pub(crate) use server::AdsServer;

use xds_api::pb::envoy::service::discovery::v3::{DeltaDiscoveryRequest, DiscoveryRequest};

#[inline]
pub(crate) fn is_nack(r: &DiscoveryRequest) -> bool {
    r.error_detail.is_some()
}

#[inline]
pub(crate) fn is_delta_nack(r: &DeltaDiscoveryRequest) -> bool {
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
