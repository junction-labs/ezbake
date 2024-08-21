mod cache;
mod connection;
mod resources;
mod server;

pub(crate) use cache::{new_snapshot, Snapshot, SnapshotWriter, TypedWriters};
pub(crate) use connection::AdsConnection;
pub(crate) use resources::ResourceType;
pub(crate) use server::AdsServer;

pub(crate) fn to_any<M: prost::Message + prost::Name>(
    m: M,
) -> Result<xds_api::pb::google::protobuf::Any, prost::EncodeError> {
    let type_url = M::type_url();
    let mut value = Vec::new();
    prost::Message::encode(&m, &mut value)?;

    Ok(xds_api::pb::google::protobuf::Any { type_url, value })
}
