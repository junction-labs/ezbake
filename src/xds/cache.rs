use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use enum_map::EnumMap;
use svix_ksuid::{Ksuid, KsuidLike};
use tokio::sync::broadcast::{self, error::RecvError};
use tracing::warn;
use xds_api::pb::google::protobuf;

use crate::xds::resources::ResourceType;

/// A set of updates/deletes to apply to a [SnapshotCache].
pub(crate) struct ResourceSnapshot {
    resources: EnumMap<ResourceType, BTreeMap<String, Option<protobuf::Any>>>,
    touch: EnumMap<ResourceType, BTreeSet<String>>,
}

impl std::fmt::Debug for ResourceSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_struct = f.debug_struct("ResourceSnapshot");
        for (resource_type, resources) in &self.resources {
            let keys: Vec<_> = resources.keys().collect();
            dbg_struct.field(resource_type.type_url(), &keys);
        }
        dbg_struct.finish()
    }
}

impl ResourceSnapshot {
    pub(crate) fn new() -> Self {
        Self {
            resources: EnumMap::default(),
            touch: EnumMap::default(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.resources.values().all(|m| m.is_empty())
    }

    pub(crate) fn update_counts(&self) -> EnumMap<ResourceType, usize> {
        let mut counts = EnumMap::default();

        for (resource_type, resources) in &self.resources {
            counts[resource_type] = resources.values().filter(|v| v.is_some()).count()
        }

        counts
    }

    pub(crate) fn delete_counts(&self) -> EnumMap<ResourceType, usize> {
        let mut counts = EnumMap::default();

        for (resource_type, resources) in &self.resources {
            counts[resource_type] = resources.values().filter(|v| v.is_none()).count()
        }

        counts
    }

    pub(crate) fn touch_counts(&self) -> EnumMap<ResourceType, usize> {
        let mut counts = EnumMap::default();
        for (resource_type, names) in &self.touch {
            counts[resource_type] = names.len();
        }
        counts
    }

    pub(crate) fn insert_update(
        &mut self,
        resource_type: ResourceType,
        name: String,
        proto: protobuf::Any,
    ) {
        self.resources[resource_type].insert(name, Some(proto));
    }

    pub(crate) fn insert_delete(&mut self, resource_type: ResourceType, name: String) {
        self.resources[resource_type].insert(name, None);
    }

    pub(crate) fn touch(&mut self, resource_type: ResourceType, name: String) {
        self.touch[resource_type].insert(name);
    }

    #[cfg(test)]
    pub(crate) fn updates_and_deletes(
        &self,
        resource_type: ResourceType,
    ) -> (Vec<String>, Vec<String>) {
        let resources = &self.resources[resource_type];

        resources
            .keys()
            .cloned()
            .partition(|k| resources.get(k).map_or(false, |v| v.is_some()))
    }
}

// NOTE: this uses a ksuid for now. no idea if that's good or not long term, but
// it's an easy way to get something relatively unique and roughly ordered.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResourceVersion(Ksuid);

impl ResourceVersion {
    /// Create a new ResourceVersion from a u64. Returns `None` if the value is
    /// zero.
    pub(crate) fn new() -> Self {
        Self(Ksuid::new(None, None))
    }
}

impl Default for ResourceVersion {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ResourceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for ResourceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{id}", id = self.0))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseVersionError {
    #[error("failed to parse resource version")]
    ParseError,
}

impl FromStr for ResourceVersion {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ksuid::from_str(s)
            .map(Self)
            .map_err(|_| ParseVersionError::ParseError)
    }
}

#[derive(Debug)]
pub(crate) struct VersionCounter;

impl VersionCounter {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn next(&self) -> ResourceVersion {
        ResourceVersion::new()
    }
}

pub(crate) type Entry<'a> = crossbeam_skiplist::map::Entry<'a, String, VersionedProto>;

pub(crate) struct VersionedProto {
    pub version: ResourceVersion,
    pub proto: protobuf::Any,
}

impl std::fmt::Debug for VersionedProto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedProto")
            .field("version", &self.version)
            .field("proto", &"...")
            .finish()
    }
}

pub(crate) trait SnapshotCallback {
    fn call(&self, writer: SnapshotWriter, resource_type: ResourceType, resource_name: &str);
}

/// Create a new [SnapshotCache] and a paired [SnapshotWriter]. The writer is
/// the only way to safely write to this cache - don't drop it if you need to
/// write.
pub(crate) fn snapshot(
    resource_callbacks: impl IntoIterator<
        Item = (ResourceType, Box<dyn SnapshotCallback + Send + Sync>),
    >,
) -> (SnapshotCache, SnapshotWriter) {
    let mut callbacks = EnumMap::default();
    for (rtype, cb) in resource_callbacks {
        callbacks[rtype] = Some(cb)
    }

    let inner = Arc::new(SnapshotCacheInner {
        version: VersionCounter::new(),
        typed: Default::default(),
        callbacks,
        // 12 = 3 cache updates for each of 4 types of resource type.
        //
        // this is completely arbitrary.
        notifications: broadcast::Sender::new(12),
    });

    (
        SnapshotCache {
            inner: inner.clone(),
        },
        SnapshotWriter { inner },
    )
}

/// A read handle into a versioned cache of xDS snapshots. A cache can have any
/// number of concurrent readers.
///
/// This handle is cheaply cloneable, for sharing the cache between multiple
/// tasks or threads.
#[derive(Clone)]
pub(crate) struct SnapshotCache {
    inner: Arc<SnapshotCacheInner>,
}

struct SnapshotCacheInner {
    version: VersionCounter,
    typed: EnumMap<ResourceType, ResourceCache>,
    callbacks: EnumMap<ResourceType, Option<Box<dyn SnapshotCallback + Send + Sync>>>,
    notifications: broadcast::Sender<ResourceType>,
}

#[derive(Default)]
struct ResourceCache {
    version: AtomicCell<ResourceVersion>,
    resources: SkipMap<String, VersionedProto>,
}

impl SnapshotCache {
    pub fn version(&self, resource_type: ResourceType) -> ResourceVersion {
        self.inner.typed[resource_type].version.load()
    }

    pub fn get(&self, resource_type: ResourceType, resource_name: &str) -> Option<Entry> {
        // fast path: resource exists
        if let Some(e) = self.inner.typed[resource_type].resources.get(resource_name) {
            return Some(e);
        }

        // slow path: try to compute the resource, allow updating the cache in the callback.
        if let Some(cb) = &self.inner.callbacks[resource_type] {
            cb.call(
                SnapshotWriter {
                    inner: self.inner.clone(),
                },
                resource_type,
                resource_name,
            )
        }

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

    pub fn changes(&self) -> SnapshotChange {
        SnapshotChange {
            _inner: self.inner.clone(),
            notifications: self.inner.notifications.subscribe(),
        }
    }
}

/// A handle to a cache that notifies on cache change.
pub(crate) struct SnapshotChange {
    notifications: broadcast::Receiver<ResourceType>,
    // hold a reference to inner to guarantee the sender half of the
    // channel can't drop.
    _inner: Arc<SnapshotCacheInner>,
}

impl SnapshotChange {
    pub async fn changed(&mut self) -> ResourceType {
        loop {
            match self.notifications.recv().await {
                Ok(rtype) => return rtype,
                Err(RecvError::Lagged(n)) => {
                    warn!(
                        dropped_notifications = %n,
                        "cache subscription fell behind."
                    );
                }
                Err(RecvError::Closed) => panic!(
                    "snapshot subscription dropped. this is a bug in ezbake, please report it"
                ),
            }
        }
    }
}

/// A write handle to a versioned cache of xDS. Write handles cannot be created
/// directly, and can only be obtained from creating a new cache with
/// [snapshot_cache].
///
/// There should be at most one write handle to a cache at any time.
pub(crate) struct SnapshotWriter {
    inner: Arc<SnapshotCacheInner>,
}

impl SnapshotWriter {
    pub(crate) fn update(&mut self, snapshot: ResourceSnapshot) -> ResourceVersion {
        let version = self.inner.version.next();
        let mut notify: EnumMap<ResourceType, bool> = EnumMap::default();

        for (resource_type, updates) in snapshot.resources {
            let cache = &self.inner.typed[resource_type];

            let mut changed = false;
            for (name, update) in updates {
                match update {
                    Some(proto) => {
                        let proto = VersionedProto { version, proto };
                        cache.resources.insert(name, proto);
                    }
                    None => {
                        cache.resources.remove(&name);
                    }
                }

                changed = true;
            }

            if changed {
                // update the cache version AFTER updating all individual resource
                // versions so that once you can see the snapshot version change,
                // changes to all resources are visible as well.
                cache.version.store(version);
                notify[resource_type] = true;
            }
        }

        for (resource_type, names) in snapshot.touch {
            let cache = &self.inner.typed[resource_type];

            for name in names {
                if let Some(entry) = cache.resources.get(&name) {
                    let proto = entry.value().proto.clone();
                    cache
                        .resources
                        .insert(name, VersionedProto { version, proto });
                    notify[resource_type] = true;
                }
            }
        }

        for (resource_type, should_notify) in notify {
            if should_notify {
                // ignore the error. it just means there's nothing to do
                let _ = self.inner.notifications.send(resource_type);
            }
        }

        version
    }
}

#[cfg(test)]
mod test {
    use super::SnapshotCache;

    #[test]
    fn assert_cache_send_sync() {
        assert_send::<SnapshotCache>();
        assert_sync::<SnapshotCache>();
    }

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
}
