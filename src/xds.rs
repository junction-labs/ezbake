mod cache;
mod connection;
mod resources;
mod server;

pub(crate) use cache::{
    snapshot_cache, ResourceSnapshot, SnapshotCache, SnapshotWriter, VersionCounter,
};
pub(crate) use connection::AdsConnection;
pub(crate) use resources::ResourceType;
pub(crate) use server::AdsServer;

use xds_api::pb::envoy::service::discovery::v3::DiscoveryRequest;

#[inline]
pub(crate) fn is_nack(r: &DiscoveryRequest) -> bool {
    r.error_detail.is_some()
}
