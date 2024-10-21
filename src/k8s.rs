use futures::TryStreamExt;
use gateway_api::apis::experimental::httproutes::HTTPRoute;
use k8s_openapi::{
    api::{core::v1::Service, discovery::v1::EndpointSlice},
    serde::Deserialize,
};
use kube::{
    runtime::{
        self,
        reflector::{self, store::Writer, ObjectRef, Store},
        watcher, WatchStreamExt,
    },
    Resource, ResourceExt as _,
};
use std::time::Duration;
use std::{collections::HashSet, fmt::Debug, future::Future, sync::Arc, time::Instant};
use tokio::sync::broadcast;
use tracing::{debug, trace};

pub(crate) trait KubeResource:
    Clone + Debug + for<'de> Deserialize<'de> + Resource<DynamicType = ()> + Send + Sync + 'static
{
    type ParentRef: KubeResource;

    fn static_kind() -> &'static str;

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>>;

    fn modify(&mut self);

    fn has_changed(&self, other: &Self) -> bool;
}

macro_rules! check_changed {
    ($old:expr, $new:expr) => {
        if $old != $new {
            return true;
        }
    };
}

const LAST_APPLIED_CONFIG: &str = "kubectl.kubernetes.io/last-applied-configuration";

impl KubeResource for HTTPRoute {
    type ParentRef = Service;

    fn static_kind() -> &'static str {
        "HTTPRoute"
    }

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>> {
        let mut parents = vec![];

        for parent_ref in self.spec.parent_refs.iter().flatten() {
            if !matches!(parent_ref.kind.as_deref(), Some("Service")) {
                continue;
            }
            let namespace = parent_ref
                .namespace
                .as_ref()
                .or(self.metadata.namespace.as_ref());

            if let Some(namespace) = namespace {
                parents.push(ObjectRef::new(&parent_ref.name).within(namespace));
            }
        }

        parents
    }

    fn modify(&mut self) {
        self.annotations_mut().remove(LAST_APPLIED_CONFIG);
        self.managed_fields_mut().clear();
        self.status = None;
    }

    fn has_changed(&self, _other: &Self) -> bool {
        // TODO: HTTPRoute and friends don't implement PartialEq/Eq, so it's
        // hard to check anything meaningful here. always rebuild for now and
        // deal with too many updates.
        //
        // https://github.com/kube-rs/gateway-api-rs/pull/53
        true
    }
}

impl KubeResource for Service {
    type ParentRef = Service;

    fn static_kind() -> &'static str {
        <Service as k8s_openapi::Resource>::KIND
    }

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>> {
        Vec::new()
    }

    fn modify(&mut self) {
        self.annotations_mut().remove(LAST_APPLIED_CONFIG);
        self.managed_fields_mut().clear();
        self.status = None;
    }

    fn has_changed(&self, other: &Self) -> bool {
        check_changed!(self.metadata.labels, other.metadata.labels);
        check_changed!(self.metadata.annotations, other.metadata.annotations);
        check_changed!(self.spec, other.spec);

        false
    }
}

impl KubeResource for EndpointSlice {
    type ParentRef = Service;

    fn static_kind() -> &'static str {
        <EndpointSlice as k8s_openapi::Resource>::KIND
    }

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>> {
        let Some(labels) = self.metadata.labels.as_ref() else {
            return Vec::new();
        };
        let Some(svc_namespace) = self.metadata.namespace.as_ref() else {
            return Vec::new();
        };
        let Some(svc_name) = labels.get("kubernetes.io/service-name") else {
            return Vec::new();
        };

        vec![ObjectRef::new(svc_name).within(svc_namespace)]
    }

    fn modify(&mut self) {
        self.annotations_mut().remove(LAST_APPLIED_CONFIG);
        self.managed_fields_mut().clear();
    }

    fn has_changed(&self, other: &Self) -> bool {
        check_changed!(self.metadata.labels, other.metadata.labels);
        check_changed!(self.ports, other.ports);
        // FIXME: this doesn't check for ordering changes. not sure how often those happen.
        check_changed!(self.endpoints, other.endpoints);

        false
    }
}

pub(crate) type ChangedObjects<K> = Arc<HashSet<RefAndParents<K>>>;

/// A wrapper around an [ObjectRef] that includes references to any of the
/// objects parent refs.
///
/// Only the wrapped [ObjectRef] is used for Eq/Hash/Cmp.
#[derive(Debug)]
pub(crate) struct RefAndParents<K: KubeResource> {
    pub obj: ObjectRef<K>,
    pub parents: Vec<ObjectRef<K::ParentRef>>,
}

impl<K: KubeResource> PartialEq for RefAndParents<K> {
    fn eq(&self, other: &Self) -> bool {
        self.obj == other.obj
    }
}

impl<K: KubeResource> Eq for RefAndParents<K> {}

impl<K: KubeResource> std::hash::Hash for RefAndParents<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.obj.hash(state);
    }
}

impl<K: KubeResource> RefAndParents<K> {
    fn from_obj(o: &K) -> Self {
        let obj = ObjectRef::from_obj(o);
        let parents = o.parent_refs();
        Self { obj, parents }
    }
}

pub(crate) struct Watch<T: KubeResource> {
    pub store: Store<T>,
    pub changes: broadcast::Sender<ChangedObjects<T>>,
}

pub(crate) fn watch<T: KubeResource>(
    api: kube::Api<T>,
    debounce_duration: Duration,
) -> (
    Watch<T>,
    impl Future<Output = Result<(), watcher::Error>> + Send + 'static,
) {
    let (store, writer) = reflector::store();
    let (change_tx, _change_rx) = broadcast::channel(10);

    (
        Watch {
            store: store.clone(),
            changes: change_tx.clone(),
        },
        run_watch(api, store, writer, change_tx, debounce_duration),
    )
}

async fn run_watch<T: KubeResource>(
    api: kube::Api<T>,
    store: Store<T>,
    mut writer: Writer<T>,
    changes: broadcast::Sender<ChangedObjects<T>>,
    debounce_duration: Duration,
) -> Result<(), watcher::Error> {
    let stream = runtime::watcher(api, runtime::watcher::Config::default().any_semantic())
        .default_backoff()
        .modify(T::modify);
    let mut stream = std::pin::pin!(stream);

    debug!(kind = T::static_kind(), "watch starting");
    let mut debounce = None;
    let mut changed: HashSet<_> = HashSet::new();
    loop {
        tokio::select! {
            biased;

            _ = sleep_until(&debounce) => {
                if !changed.is_empty() {
                    let to_send: ChangedObjects<_> = Arc::new(std::mem::take(&mut changed));
                    if changes.send(to_send).is_err() {
                        debug!(kind = T::static_kind(), "watch ended: all recievers dropped");
                        break;
                    };
                }
                debounce.take();
            }
            event = stream.try_next() => {
                // return the error if the stream dies, continue if there's no next item.
                let Some(event) = event? else {
                    continue
                };
                handle_watch_event(&event, &mut changed, &mut debounce, &store, debounce_duration);
                writer.apply_watcher_event(&event);
            },
        }
    }

    debug!(kind = T::static_kind(), "watch exiting");
    Ok(())
}

pub(crate) fn is_api_not_found(e: &watcher::Error) -> bool {
    matches!(
        e,
        watcher::Error::InitialListFailed(kube::Error::Api(e)) if e.code == 404,
    )
}

fn handle_watch_event<T: KubeResource>(
    event: &watcher::Event<T>,
    changed: &mut HashSet<RefAndParents<T>>,
    debounce: &mut Option<Instant>,
    store: &Store<T>,
    debounce_duration: Duration,
) {
    match &event {
        // on apply, compare with the currently cached version of
        // the object and only send it if there's a meaningful
        // change.
        watcher::Event::Applied(new_obj) => {
            let new_ref = RefAndParents::from_obj(new_obj);
            let old_obj = store.get(&new_ref.obj);
            let has_changed = old_obj.map_or(true, |obj| obj.has_changed(new_obj));

            if has_changed {
                changed.insert(new_ref);
                debounce.get_or_insert_with(|| Instant::now() + debounce_duration);
            }
        }
        // On delete, mark everything changed and send it
        watcher::Event::Deleted(obj) => {
            changed.insert(RefAndParents::from_obj(obj));
            debounce.get_or_insert_with(|| Instant::now() + debounce_duration);
        }
        // on init, mark the union of everything in the Store and
        // everything new as changed and let downstream figure it out.
        watcher::Event::Restarted(objs) => {
            trace!(kind = T::static_kind(), "watch restarted");
            for obj in objs {
                changed.insert(RefAndParents::from_obj(obj));
            }
            for obj in store.state() {
                changed.insert(RefAndParents::from_obj(&obj));
            }
            debounce.get_or_insert_with(|| Instant::now() + debounce_duration);
        }
    }
}

async fn sleep_until(deadline: &Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until((*d).into()).await,
        None => futures::future::pending().await,
    }
}
