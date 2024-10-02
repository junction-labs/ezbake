use std::{
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
use xds_api::pb::google::protobuf;

use crate::xds::resources::ResourceType;

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
}

impl SnapshotWriter {
    pub fn update(
        &self,
        version: ResourceVersion,
        resources: impl Iterator<Item = (String, Option<protobuf::Any>)>,
    ) {
        let typed = &self.inner.typed[self.resource_type];

        typed.version.store(version.to_u64(), Ordering::SeqCst);
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
