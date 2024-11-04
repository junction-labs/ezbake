use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

use crossbeam_skiplist::SkipMap;
use enum_map::EnumMap;
use once_cell::sync::Lazy;
use tokio::sync::broadcast::{self, error::RecvError};
use tracing::warn;
use xds_api::pb::google::protobuf;

use crate::xds::resources::ResourceType;

/// A set of updates/deletes to apply to a [SnapshotCache].
pub(crate) struct ResourceSnapshot {
    resources: EnumMap<ResourceType, BTreeMap<String, Option<protobuf::Any>>>,
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

// FIXME: allow more than one version per ms, since even with the debounce we're
// updating more than once per ms. the time mask should probably change to find
// a few more bits.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResourceVersion(NonZeroU64);

impl ResourceVersion {
    const EPOCH_MASK: u64 = (1 << 48) - 1;
    const PREFIX_MASK: u64 = !Self::EPOCH_MASK;

    /// Create a new ResourceVersion from a u64. Returns `None` if the value is
    /// zero.
    pub(crate) fn new(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    /// Create a ResourceVersion from an epoch-millis timestamp and an already
    /// shifted prefix. Panics if the prefix and epoch_ms are both zero.
    pub(crate) fn from_raw_parts(prefix: u64, epoch_ms: u64) -> Self {
        let version = prefix | (epoch_ms & Self::EPOCH_MASK);
        Self(version.try_into().expect("masked a zero ResourceVersion"))
    }

    #[cfg(test)]
    pub(crate) fn from_parts(prefix: u64, epoch_ms: u64) -> Self {
        let prefix = prefix << 48;
        let version = prefix | (epoch_ms & Self::EPOCH_MASK);
        Self(version.try_into().expect("created a zero ResourceVersion"))
    }

    fn as_parts(&self) -> (u64, u64) {
        let v = self.0.get();
        let prefix = (v & ResourceVersion::PREFIX_MASK) >> 48;
        let epoch = v & ResourceVersion::EPOCH_MASK;
        (prefix, epoch)
    }

    fn to_u64(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Debug for ResourceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for ResourceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, epoch) = self.as_parts();
        f.write_fmt(format_args!("{prefix}.{epoch}"))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseVersionError {
    #[error("resource version cannot be 0.0")]
    Zero,

    #[error("failed to parse resource version")]
    ParseError,
}

impl FromStr for ResourceVersion {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (first, second) = s.split_once('.').ok_or(ParseVersionError::ParseError)?;
        let prefix: u16 = first.parse().map_err(|_| ParseVersionError::ParseError)?;
        let epoch_ms: u64 = second.parse().map_err(|_| ParseVersionError::ParseError)?;

        if prefix == 0 && epoch_ms == 0 {
            return Err(ParseVersionError::Zero);
        }

        // safety: it's safe to skip the mask as prefix was parsed as a u16.
        let prefix = (prefix as u64) << 48;
        let epoch_ms = epoch_ms & Self::EPOCH_MASK;
        Ok(Self::from_raw_parts(prefix, epoch_ms))
    }
}

#[derive(Debug)]
pub(crate) struct VersionCounter {
    prefix: u64,
}

impl VersionCounter {
    pub(crate) fn new(prefix: u16) -> Self {
        let prefix = (prefix as u64) << 48;
        Self { prefix }
    }

    pub(crate) fn with_process_prefix() -> Self {
        Self::new(Self::process_preifx())
    }

    fn process_preifx() -> u16 {
        static PREFIX: Lazy<u16> = Lazy::new(|| {
            use std::hash::Hash;
            use std::hash::Hasher;

            let mut hasher = std::hash::DefaultHasher::new();
            // pid
            std::process::id().hash(&mut hasher);
            // POD_NAME env var
            std::env::var("POD_NAME")
                .ok()
                .inspect(|pod_name| pod_name.hash(&mut hasher));

            hasher.finish() as u16
        });
        *PREFIX
    }

    pub(crate) fn next(&self) -> ResourceVersion {
        let elapsed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("your clock is set before January 1, 1970");

        // safety: in the distant future, this cast will truncate and that's
        // fine. at worst we're wasting a few instructions not masking first.
        ResourceVersion::from_raw_parts(self.prefix, elapsed.as_millis() as u64)
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

/// Create a new [SnapshotCache] and a paired [SnapshotWriter]. The writer is
/// the only way to safely write to this cache - don't drop it if you need to
/// write.
pub(crate) fn snapshot() -> (SnapshotCache, SnapshotWriter) {
    let inner = Arc::new(SnapshotCacheInner::default());
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
    typed: EnumMap<ResourceType, ResourceCache>,
    notifications: broadcast::Sender<ResourceType>,
}

impl Default for SnapshotCacheInner {
    fn default() -> Self {
        Self {
            typed: Default::default(),
            // 12 = 3 cache updates for each of 4 types of resource type.
            //
            // this is completely arbitrary.
            notifications: broadcast::Sender::new(12),
        }
    }
}

#[derive(Default)]
struct ResourceCache {
    version: AtomicU64,
    resources: SkipMap<String, VersionedProto>,
}

impl SnapshotCache {
    pub fn version(&self, resource_type: ResourceType) -> Option<ResourceVersion> {
        let v = self.inner.typed[resource_type]
            .version
            .load(Ordering::SeqCst);
        ResourceVersion::new(v)
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
    pub(crate) fn update(&mut self, version: ResourceVersion, snapshot: ResourceSnapshot) {
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
                cache.version.store(version.to_u64(), Ordering::SeqCst);
                // ignore the error. it just means there's nothing to do
                let _ = self.inner.notifications.send(resource_type);
            }
        }
    }
}
