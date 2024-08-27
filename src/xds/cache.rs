use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crossbeam_skiplist::SkipMap;
use enum_map::EnumMap;
use xds_api::pb::google::protobuf;

use crate::xds::resources::ResourceType;

pub(crate) type Entry<'a> = crossbeam_skiplist::map::Entry<'a, String, VersionedProto>;

#[derive(Clone)]
pub(crate) struct Snapshot {
    inner: Arc<SnapshotInner>,
}

pub(crate) struct SnapshotWriter {
    resource_type: ResourceType,
    inner: Arc<SnapshotInner>,
}

pub(crate) fn new_snapshot() -> (Snapshot, TypedWriters) {
    let inner = Arc::new(SnapshotInner::default());
    (
        Snapshot {
            inner: inner.clone(),
        },
        TypedWriters::from_inner(inner),
    )
}

pub(crate) struct VersionedProto {
    pub version: u64,
    pub proto: protobuf::Any,
}

#[derive(Default)]
struct SnapshotInner {
    typed: EnumMap<ResourceType, ResourceSnapshot>,
}

#[derive(Default)]
struct ResourceSnapshot {
    version: AtomicU64,
    resources: SkipMap<String, VersionedProto>,
}

impl Snapshot {
    pub fn version(&self, resource_type: ResourceType) -> u64 {
        self.inner.typed[resource_type]
            .version
            .load(Ordering::SeqCst)
    }

    pub fn get(&self, resource_type: ResourceType, resource_name: &str) -> Option<Entry> {
        self.inner.typed[resource_type].resources.get(resource_name)
    }

    pub fn len(&self, resource_type: ResourceType) -> usize {
        self.inner.typed[resource_type].resources.len()
    }

    pub fn iter(
        &self,
        resource_type: ResourceType,
    ) -> crossbeam_skiplist::map::Iter<String, VersionedProto> {
        self.inner.typed[resource_type].resources.iter()
    }
}

impl SnapshotWriter {
    pub fn update(
        &self,
        version: u64,
        resources: impl Iterator<Item = (String, Option<protobuf::Any>)>,
    ) {
        let typed = &self.inner.typed[self.resource_type];

        typed.version.store(version, Ordering::SeqCst);
        for (k, v) in resources {
            match v {
                Some(proto) => {
                    typed.resources.insert(k, VersionedProto { version, proto });
                }
                None => {
                    typed.resources.remove(&k);
                }
            }
        }
    }
}

pub(crate) struct TypedWriters {
    writers: EnumMap<ResourceType, Option<SnapshotWriter>>,
}

impl TypedWriters {
    fn from_inner(inner: Arc<SnapshotInner>) -> Self {
        let mut writers = EnumMap::default();
        for rtype in ResourceType::all() {
            writers[*rtype] = Some(SnapshotWriter {
                resource_type: *rtype,
                inner: inner.clone(),
            });
        }
        Self { writers }
    }

    pub(crate) fn for_type(&mut self, resource_type: ResourceType) -> Option<SnapshotWriter> {
        self.writers[resource_type].take()
    }
}
