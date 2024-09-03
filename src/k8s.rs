use futures::TryStreamExt;
use k8s_openapi::{
    api::{core::v1::Service, discovery::v1::EndpointSlice},
    serde::Deserialize,
    Metadata,
};
use kube::{
    api::ObjectMeta,
    runtime::{
        self,
        reflector::{self, ObjectRef, Store},
        watcher, WatchStreamExt,
    },
    Resource, ResourceExt as _,
};
use std::time::Duration;
use std::{collections::HashSet, fmt::Debug, future::Future, sync::Arc, time::Instant};
use tokio::sync::broadcast;
use tracing::{debug, trace};

pub(crate) trait KubeResource:
    Clone
    + Debug
    + for<'de> Deserialize<'de>
    + Metadata
    + Resource<DynamicType = ()>
    + Send
    + Sync
    + 'static
{
    type ParentRef: KubeResource;

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

impl KubeResource for Service {
    type ParentRef = Service;

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>> {
        Vec::new()
    }

    fn modify(&mut self) {
        self.managed_fields_mut().clear();
        self.status = None;
    }

    fn has_changed(&self, other: &Self) -> bool {
        check_changed!(self.meta().labels, other.meta().labels);
        check_changed!(self.meta().annotations, other.meta().annotations);
        check_changed!(self.spec, other.spec);

        false
    }
}

impl KubeResource for EndpointSlice {
    type ParentRef = Service;

    fn parent_refs(&self) -> Vec<ObjectRef<Self::ParentRef>> {
        let Some(labels) = self.meta().labels.as_ref() else {
            return Vec::new();
        };
        let Some(svc_namespace) = self.meta().namespace.as_ref() else {
            return Vec::new();
        };
        let Some(svc_name) = labels.get("kubernetes.io/service-name") else {
            return Vec::new();
        };

        vec![ObjectRef::new(svc_name).within(svc_namespace)]
    }

    fn modify(&mut self) {
        self.managed_fields_mut().clear();
    }

    fn has_changed(&self, other: &Self) -> bool {
        check_changed!(self.meta().labels, other.meta().labels);
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

pub(crate) fn watch<T: KubeResource>(
    client: kube::Client,
    debounce_duration: Duration,
) -> (
    Store<T>,
    broadcast::Sender<ChangedObjects<T>>,
    impl Future<Output = ()> + Send + 'static,
) {
    let (store, mut writer) = reflector::store();
    let (change_tx, _change_rx) = broadcast::channel(10);
    let watch_fut = {
        let store = store.clone();
        let change_tx = change_tx.clone();
        async move {
            let api: kube::Api<T> = kube::Api::all(client);
            let stream = runtime::watcher(api, runtime::watcher::Config::default().any_semantic())
                .default_backoff()
                .modify(T::modify);
            let mut stream = std::pin::pin!(stream);

            debug!(kind = T::KIND, "watch starting");
            let mut debounce = None;
            let mut changed: HashSet<_> = HashSet::new();
            loop {
                tokio::select! {
                    biased;

                    _ = sleep_until(&debounce) => {
                        if !changed.is_empty() {
                            let to_send: ChangedObjects<_> = Arc::new(std::mem::take(&mut changed));
                            if change_tx.send(to_send).is_err() {
                                debug!(kind = T::KIND, "watch ended: all recievers dropped");
                                break;
                            };
                        }
                        debounce.take();
                    }
                    event = stream.try_next() => {
                        let event = event.unwrap().unwrap();
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
                            },
                            // On delete, mark everything changed and send it
                            watcher::Event::Deleted(obj) => {
                                changed.insert(RefAndParents::from_obj(obj));
                                debounce.get_or_insert_with(|| Instant::now() + debounce_duration);
                            },
                            // on init, mark the union of everything in the Store and
                            // everything new as changed and let downstream figure it out.
                            watcher::Event::Restarted(objs) => {
                                trace!(kind = T::KIND, "watch restarted");
                                for obj in objs {
                                    changed.insert(RefAndParents::from_obj(obj));
                                }
                                for obj in store.state() {
                                    changed.insert(RefAndParents::from_obj(&obj));
                                }
                                debounce.get_or_insert_with(|| Instant::now() + debounce_duration);
                            },
                        }
                        writer.apply_watcher_event(&event);
                    },
                }
            }
        }
    };

    (store, change_tx, watch_fut)
}

async fn sleep_until(deadline: &Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until((*d).into()).await,
        None => futures::future::pending().await,
    }
}

pub(crate) fn namespace_and_name<T: k8s_openapi::Metadata<Ty = ObjectMeta>>(
    t: &T,
) -> Option<(&str, &str)> {
    let meta = t.metadata();
    let namespace = meta.namespace.as_ref()?;
    let name = meta.name.as_ref()?;
    Some((namespace, name))
}

pub(crate) fn ref_namespace_and_name<T: KubeResource>(
    obj_ref: &ObjectRef<T>,
) -> Option<(&str, &str)> {
    let namespace = obj_ref.namespace.as_ref()?;
    let name = &obj_ref.name;

    Some((namespace, name))
}
