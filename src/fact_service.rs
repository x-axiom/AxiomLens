use crate::{
    cancellation::CancellationToken,
    connection::{
        ApiCapabilitySnapshot, ApiResourceDiscoveryState, ApiResourceScope,
    },
    target::{CacheScope, NamespaceName, ResourceIdentity, ResourceRef, TargetBinding},
};
use async_channel::Receiver;
use kube::{
    Api, Client,
    api::{DynamicObject, ListParams},
    core::gvk::GroupVersionKind,
    discovery::ApiResource,
};
use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::{runtime::Handle, task::JoinHandle};
use uuid::Uuid;

pub const MAX_FACTS_PER_LIST: usize = 100;
pub const FACT_BATCH_SIZE: usize = 16;
const FACT_BATCH_CHANNEL_CAPACITY: usize = 4;
const DEFAULT_CACHE_ENTRIES: usize = 512;
const DEFAULT_CACHE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CACHE_ENTRY_BYTES: usize = 1024 * 1024;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactErrorKind {
    UnsupportedResource,
    DiscoveryDenied,
    CapabilityUnavailable,
    ListNotSupported,
    NamespaceRequired,
    NamespaceOutsideTarget,
    NamespaceOnClusterResource,
    InvalidLimit,
    MissingName,
    MissingUid,
    PermissionDenied,
    CredentialsRejected,
    ResourceNotFound,
    RequestTimedOut,
    ApiUnavailable,
    ServerUnavailable,
    SessionCancelled,
    InvalidObject,
    RawDataTooLarge,
    CacheScopeMismatch,
    RawReferenceExpired,
    SerializationFailed,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactError {
    kind: FactErrorKind,
}

impl FactError {
    pub(crate) fn new(kind: FactErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> FactErrorKind {
        self.kind
    }

    pub fn error_code(self) -> &'static str {
        match self.kind {
            FactErrorKind::UnsupportedResource => "resource_unsupported",
            FactErrorKind::DiscoveryDenied => "discovery_denied",
            FactErrorKind::CapabilityUnavailable => "capability_unavailable",
            FactErrorKind::ListNotSupported => "resource_list_unsupported",
            FactErrorKind::NamespaceRequired => "namespace_required",
            FactErrorKind::NamespaceOutsideTarget => "namespace_outside_target",
            FactErrorKind::NamespaceOnClusterResource => "namespace_on_cluster_resource",
            FactErrorKind::InvalidLimit => "invalid_list_limit",
            FactErrorKind::MissingName => "resource_name_missing",
            FactErrorKind::MissingUid => "resource_uid_missing",
            FactErrorKind::PermissionDenied => "permission_denied",
            FactErrorKind::CredentialsRejected => "credentials_rejected",
            FactErrorKind::ResourceNotFound => "resource_not_found",
            FactErrorKind::RequestTimedOut => "request_timeout",
            FactErrorKind::ApiUnavailable => "api_unavailable",
            FactErrorKind::ServerUnavailable => "server_unavailable",
            FactErrorKind::SessionCancelled => "session_cancelled",
            FactErrorKind::InvalidObject => "resource_object_invalid",
            FactErrorKind::RawDataTooLarge => "resource_data_too_large",
            FactErrorKind::CacheScopeMismatch => "cache_scope_mismatch",
            FactErrorKind::RawReferenceExpired => "raw_reference_expired",
            FactErrorKind::SerializationFailed => "resource_serialization_failed",
            FactErrorKind::Internal => "fact_service_internal",
        }
    }

    pub fn summary(self) -> &'static str {
        match self.kind {
            FactErrorKind::UnsupportedResource => "The API server does not serve this resource version.",
            FactErrorKind::DiscoveryDenied => "API discovery permission is unavailable for this resource group.",
            FactErrorKind::CapabilityUnavailable => "Resource capability could not be determined.",
            FactErrorKind::ListNotSupported => "The API does not support listing this resource.",
            FactErrorKind::NamespaceRequired => "Choose one authorized Namespace for this request.",
            FactErrorKind::NamespaceOutsideTarget => "Requested Namespace is outside the selected target scope.",
            FactErrorKind::NamespaceOnClusterResource => "Cluster-scoped resource cannot be queried in a Namespace.",
            FactErrorKind::InvalidLimit => "Requested result limit is outside the supported range.",
            FactErrorKind::MissingName => "Resource response did not include a name.",
            FactErrorKind::MissingUid => "Resource response did not include a UID.",
            FactErrorKind::PermissionDenied => "API server denied this read request.",
            FactErrorKind::CredentialsRejected => "Credentials were rejected or expired.",
            FactErrorKind::ResourceNotFound => "The requested resource was not found.",
            FactErrorKind::RequestTimedOut => "Resource read timed out.",
            FactErrorKind::ApiUnavailable => "Kubernetes API server is unreachable.",
            FactErrorKind::ServerUnavailable => "Kubernetes API server is temporarily unavailable.",
            FactErrorKind::SessionCancelled => "Resource read was cancelled with its target session.",
            FactErrorKind::InvalidObject => "API server returned an invalid resource object.",
            FactErrorKind::RawDataTooLarge => "Resource payload exceeds the local facts limit.",
            FactErrorKind::CacheScopeMismatch => "Raw fact reference belongs to a different target session.",
            FactErrorKind::RawReferenceExpired => "Local raw fact reference is no longer cached.",
            FactErrorKind::SerializationFailed => "Resource payload could not be normalized locally.",
            FactErrorKind::Internal => "Read-only facts service failed unexpectedly.",
        }
    }
}

impl fmt::Display for FactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.summary())
    }
}

impl Error for FactError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactSource {
    ApiServer,
    LocalMemoryCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactFreshness {
    Fresh,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactCompleteness {
    Complete,
    ResourceVersionMissing,
    RawDataNotCached,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalRawReference(String);

impl LocalRawReference {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone)]
pub struct ResourceFact {
    resource: ResourceRef,
    raw_reference: Option<LocalRawReference>,
    object: Arc<DynamicObject>,
    collected_at: SystemTime,
    completeness: FactCompleteness,
    source: FactSource,
}

impl ResourceFact {
    pub fn resource(&self) -> &ResourceRef {
        &self.resource
    }

    pub fn raw_reference(&self) -> Option<&LocalRawReference> {
        self.raw_reference.as_ref()
    }

    pub fn object(&self) -> &DynamicObject {
        &self.object
    }

    pub fn collected_at(&self) -> SystemTime {
        self.collected_at
    }

    pub fn completeness(&self) -> FactCompleteness {
        self.completeness
    }

    pub fn source(&self) -> FactSource {
        self.source
    }
}

impl fmt::Debug for ResourceFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceFact")
            .field("resource", &self.resource)
            .field("raw_reference", &self.raw_reference)
            .field("collected_at", &self.collected_at)
            .field("completeness", &self.completeness)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct FactPage {
    target: TargetBinding,
    items: Vec<ResourceFact>,
    collection_resource_version: Option<String>,
    collected_at: SystemTime,
    possibly_truncated: bool,
    source: FactSource,
}

impl FactPage {
    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn items(&self) -> &[ResourceFact] {
        &self.items
    }

    pub fn collection_resource_version(&self) -> Option<&str> {
        self.collection_resource_version.as_deref()
    }

    pub fn collected_at(&self) -> SystemTime {
        self.collected_at
    }

    pub fn possibly_truncated(&self) -> bool {
        self.possibly_truncated
    }

    pub fn source(&self) -> FactSource {
        self.source
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactCacheConfig {
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
    ttl: Duration,
}

impl FactCacheConfig {
    pub fn new(
        max_entries: usize,
        max_bytes: usize,
        max_entry_bytes: usize,
        ttl: Duration,
    ) -> Result<Self, FactError> {
        if max_entries == 0
            || max_bytes == 0
            || max_entry_bytes == 0
            || max_entry_bytes > max_bytes
            || ttl.is_zero()
        {
            return Err(FactError::new(FactErrorKind::InvalidLimit));
        }
        Ok(Self {
            max_entries,
            max_bytes,
            max_entry_bytes,
            ttl,
        })
    }
}

impl Default for FactCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_CACHE_ENTRIES,
            max_bytes: DEFAULT_CACHE_BYTES,
            max_entry_bytes: DEFAULT_CACHE_ENTRY_BYTES,
            ttl: DEFAULT_CACHE_TTL,
        }
    }
}

#[derive(Clone)]
pub struct SharedFactCache {
    config: FactCacheConfig,
    inner: Arc<Mutex<FactCacheInner>>,
}

impl SharedFactCache {
    pub fn new(config: FactCacheConfig) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(FactCacheInner::default())),
        }
    }

    pub fn read_raw(
        &self,
        target: &TargetBinding,
        reference: &LocalRawReference,
    ) -> Result<CachedRawFact, FactError> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = inner
            .raw_references
            .get(reference)
            .ok_or(FactError::new(FactErrorKind::RawReferenceExpired))?;
        if key.scope != target.cache_scope() {
            return Err(FactError::new(FactErrorKind::CacheScopeMismatch));
        }
        let entry = inner
            .entries
            .get(key)
            .ok_or(FactError::new(FactErrorKind::RawReferenceExpired))?;
        Ok(CachedRawFact {
            fact: entry.fact.clone(),
            freshness: if Instant::now() < entry.expires_at {
                FactFreshness::Fresh
            } else {
                FactFreshness::Stale
            },
        })
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert(&self, mut fact: ResourceFact, bytes: usize) -> ResourceFact {
        if bytes > self.config.max_entry_bytes || bytes > self.config.max_bytes {
            fact.completeness = FactCompleteness::RawDataNotCached;
            return fact;
        }
        let reference = LocalRawReference(Uuid::new_v4().to_string());
        fact.raw_reference = Some(reference.clone());
        let key = FactCacheKey {
            scope: fact.resource.target().cache_scope(),
            identity: fact.resource.identity().clone(),
        };
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        remove_cache_entry(&mut inner, &key);
        while inner.entries.len() >= self.config.max_entries
            || inner.current_bytes.saturating_add(bytes) > self.config.max_bytes
        {
            let Some(oldest) = inner.insertion_order.pop_front() else {
                break;
            };
            remove_cache_entry(&mut inner, &oldest);
        }
        inner.raw_references.insert(reference, key.clone());
        inner.current_bytes = inner.current_bytes.saturating_add(bytes);
        inner.insertion_order.push_back(key.clone());
        inner.entries.insert(
            key,
            CachedFactEntry {
                fact: fact.clone(),
                expires_at: Instant::now() + self.config.ttl,
                bytes,
            },
        );
        fact
    }
}

impl Default for SharedFactCache {
    fn default() -> Self {
        Self::new(FactCacheConfig::default())
    }
}

#[derive(Clone)]
struct FactCacheKey {
    scope: CacheScope,
    identity: ResourceIdentity,
}

impl PartialEq for FactCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.scope == other.scope && self.identity == other.identity
    }
}

impl Eq for FactCacheKey {}

impl std::hash::Hash for FactCacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.scope.hash(state);
        self.identity.hash(state);
    }
}

#[derive(Default)]
struct FactCacheInner {
    entries: HashMap<FactCacheKey, CachedFactEntry>,
    raw_references: HashMap<LocalRawReference, FactCacheKey>,
    insertion_order: VecDeque<FactCacheKey>,
    current_bytes: usize,
}

struct CachedFactEntry {
    fact: ResourceFact,
    expires_at: Instant,
    bytes: usize,
}

fn remove_cache_entry(inner: &mut FactCacheInner, key: &FactCacheKey) {
    if let Some(entry) = inner.entries.remove(key) {
        inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
        if let Some(reference) = entry.fact.raw_reference {
            inner.raw_references.remove(&reference);
        }
    }
    inner.insertion_order.retain(|candidate| candidate != key);
}

#[derive(Debug, Clone)]
pub struct CachedRawFact {
    fact: ResourceFact,
    freshness: FactFreshness,
}

impl CachedRawFact {
    pub fn fact(&self) -> &ResourceFact {
        &self.fact
    }

    pub fn freshness(&self) -> FactFreshness {
        self.freshness
    }
}

#[derive(Debug, Clone)]
pub enum FactStreamEvent {
    Batch(Vec<ResourceFact>),
    Complete {
        collection_resource_version: Option<String>,
        collected_at: SystemTime,
        possibly_truncated: bool,
    },
}

pub struct FactBatchStream {
    receiver: Receiver<Result<FactStreamEvent, FactError>>,
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl FactBatchStream {
    pub async fn recv(&self) -> Result<FactStreamEvent, FactStreamReceiveError> {
        self.receiver
            .recv()
            .await
            .map_err(|_| FactStreamReceiveError::Closed)?
            .map_err(FactStreamReceiveError::Service)
    }
}

impl Drop for FactBatchStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactStreamReceiveError {
    Closed,
    Service(FactError),
}

impl fmt::Display for FactStreamReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("fact stream closed"),
            Self::Service(error) => error.fmt(formatter),
        }
    }
}

impl Error for FactStreamReceiveError {}

#[derive(Clone)]
pub struct ReadOnlyFactService {
    client: Client,
    target: TargetBinding,
    capabilities: ApiCapabilitySnapshot,
    cancellation: CancellationToken,
    runtime: Handle,
    cache: SharedFactCache,
    request_timeout: Duration,
}

impl ReadOnlyFactService {
    pub(crate) fn new(
        client: Client,
        target: TargetBinding,
        capabilities: ApiCapabilitySnapshot,
        cancellation: CancellationToken,
        runtime: Handle,
        cache: SharedFactCache,
        request_timeout: Duration,
    ) -> Self {
        Self {
            client,
            target,
            capabilities,
            cancellation,
            runtime,
            cache,
            request_timeout,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn cache(&self) -> &SharedFactCache {
        &self.cache
    }

    pub fn list_stream(
        &self,
        gvk: GroupVersionKind,
        namespace: Option<String>,
        limit: usize,
    ) -> Result<FactBatchStream, FactError> {
        if limit == 0 || limit > MAX_FACTS_PER_LIST {
            return Err(FactError::new(FactErrorKind::InvalidLimit));
        }
        let (sender, receiver) = async_channel::bounded(FACT_BATCH_CHANNEL_CAPACITY);
        let request_cancellation = CancellationToken::new();
        let worker_cancellation = request_cancellation.clone();
        let service = self.clone();
        let task = self.runtime.spawn(async move {
            let result = tokio::select! {
                _ = service.cancellation.cancelled() => {
                    Err(FactError::new(FactErrorKind::SessionCancelled))
                }
                _ = worker_cancellation.cancelled() => {
                    Err(FactError::new(FactErrorKind::SessionCancelled))
                }
                result = service.list_page(gvk, namespace, limit, worker_cancellation.clone()) => result,
            };
            match result {
                Ok(page) => {
                    for batch in page.items.chunks(FACT_BATCH_SIZE) {
                        if sender.send(Ok(FactStreamEvent::Batch(batch.to_vec()))).await.is_err() {
                            return;
                        }
                    }
                    let _ = sender
                        .send(Ok(FactStreamEvent::Complete {
                            collection_resource_version: page.collection_resource_version,
                            collected_at: page.collected_at,
                            possibly_truncated: page.possibly_truncated,
                        }))
                        .await;
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                }
            }
        });
        Ok(FactBatchStream {
            receiver,
            cancellation: request_cancellation,
            task: Some(task),
        })
    }

    pub async fn get(
        &self,
        gvk: GroupVersionKind,
        namespace: Option<String>,
        name: String,
    ) -> Result<ResourceFact, FactError> {
        let service = self.clone();
        let cancellation = self.cancellation.clone();
        self.runtime
            .spawn(async move { service.get_one(gvk, namespace, name, cancellation).await })
            .await
            .map_err(|_| FactError::new(FactErrorKind::Internal))?
    }

    async fn list_page(
        &self,
        gvk: GroupVersionKind,
        requested_namespace: Option<String>,
        limit: usize,
        cancellation: CancellationToken,
    ) -> Result<FactPage, FactError> {
        let (api_resource, scope) = self.resolve_resource(&gvk)?;
        if !self
            .capabilities
            .resource_gvk(&gvk)
            .is_some_and(|capability| capability.is_list_supported())
        {
            return Err(FactError::new(FactErrorKind::ListNotSupported));
        }
        let namespace = self.resolve_namespace(scope, requested_namespace.as_deref())?;
        let api: Api<DynamicObject> = match &namespace {
            Some(namespace) => Api::namespaced_with(self.client.clone(), namespace, &api_resource),
            None => Api::all_with(self.client.clone(), &api_resource),
        };
        let params = ListParams::default().limit(limit as u32);
        let page = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            _ = cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            result = tokio::time::timeout(self.request_timeout, api.list(&params)) => {
                result.map_err(|_| FactError::new(FactErrorKind::RequestTimedOut))?
                    .map_err(map_api_error)?
            }
        };
        let collected_at = SystemTime::now();
        let collection_resource_version = page.metadata.resource_version.clone();
        let mut possibly_truncated = page.items.len() >= limit;
        let mut facts = Vec::with_capacity(page.items.len().min(limit));
        let mut total_bytes = 0_usize;
        for object in page.items.into_iter().take(limit) {
            let mut fact = self.make_fact(&api_resource, scope, namespace.as_deref(), object, collected_at)?;
            let bytes = serde_json::to_vec(fact.object.as_ref())
                .map_err(|_| FactError::new(FactErrorKind::SerializationFailed))?
                .len();
            if total_bytes.saturating_add(bytes) > MAX_PAGE_BYTES {
                possibly_truncated = true;
                break;
            }
            total_bytes += bytes;
            fact = self.cache.insert(fact, bytes);
            facts.push(fact);
        }
        Ok(FactPage {
            target: self.target.clone(),
            items: facts,
            collection_resource_version,
            collected_at,
            possibly_truncated,
            source: FactSource::ApiServer,
        })
    }

    async fn get_one(
        &self,
        gvk: GroupVersionKind,
        requested_namespace: Option<String>,
        name: String,
        cancellation: CancellationToken,
    ) -> Result<ResourceFact, FactError> {
        if name.trim().is_empty() {
            return Err(FactError::new(FactErrorKind::MissingName));
        }
        let (api_resource, scope) = self.resolve_resource(&gvk)?;
        let namespace = self.resolve_namespace(scope, requested_namespace.as_deref())?;
        let api: Api<DynamicObject> = match &namespace {
            Some(namespace) => Api::namespaced_with(self.client.clone(), namespace, &api_resource),
            None => Api::all_with(self.client.clone(), &api_resource),
        };
        let object = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            _ = cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            result = tokio::time::timeout(self.request_timeout, api.get(&name)) => {
                result.map_err(|_| FactError::new(FactErrorKind::RequestTimedOut))?
                    .map_err(map_api_error)?
            }
        };
        let mut fact = self.make_fact(
            &api_resource,
            scope,
            namespace.as_deref(),
            object,
            SystemTime::now(),
        )?;
        let bytes = serde_json::to_vec(fact.object.as_ref())
            .map_err(|_| FactError::new(FactErrorKind::SerializationFailed))?
            .len();
        fact = self.cache.insert(fact, bytes);
        Ok(fact)
    }

    fn resolve_resource(
        &self,
        gvk: &GroupVersionKind,
    ) -> Result<(ApiResource, ApiResourceScope), FactError> {
        let capability = self
            .capabilities
            .resource_gvk(gvk)
            .ok_or(FactError::new(FactErrorKind::UnsupportedResource))?;
        match capability.state() {
            ApiResourceDiscoveryState::Unsupported => {
                return Err(FactError::new(FactErrorKind::UnsupportedResource));
            }
            ApiResourceDiscoveryState::TimedOut | ApiResourceDiscoveryState::Unavailable => {
                return Err(FactError::new(FactErrorKind::CapabilityUnavailable));
            }
            ApiResourceDiscoveryState::DiscoveryDenied => {}
            ApiResourceDiscoveryState::Served { .. } => {}
        }
        let api_resource = capability
            .api_resource()
            .cloned()
            .unwrap_or_else(|| ApiResource::from_gvk(gvk));
        let scope = match capability.state() {
            ApiResourceDiscoveryState::Served { scope, .. } => scope,
            ApiResourceDiscoveryState::DiscoveryDenied => known_scope(gvk)
                .ok_or(FactError::new(FactErrorKind::DiscoveryDenied))?,
            _ => return Err(FactError::new(FactErrorKind::CapabilityUnavailable)),
        };
        Ok((api_resource, scope))
    }

    fn resolve_namespace(
        &self,
        scope: ApiResourceScope,
        requested_namespace: Option<&str>,
    ) -> Result<Option<String>, FactError> {
        match scope {
            ApiResourceScope::Cluster => {
                if requested_namespace.is_some() {
                    return Err(FactError::new(FactErrorKind::NamespaceOnClusterResource));
                }
                Ok(None)
            }
            ApiResourceScope::Namespaced => {
                let namespace = match requested_namespace {
                    Some(namespace) => NamespaceName::new(namespace)
                        .map_err(|_| FactError::new(FactErrorKind::NamespaceOutsideTarget))?,
                    None => self
                        .target
                        .namespace_scope()
                        .single_namespace()
                        .cloned()
                        .ok_or(FactError::new(FactErrorKind::NamespaceRequired))?,
                };
                if !self.target.namespace_scope().includes(&namespace) {
                    return Err(FactError::new(FactErrorKind::NamespaceOutsideTarget));
                }
                Ok(Some(namespace.as_str().to_owned()))
            }
        }
    }

    fn make_fact(
        &self,
        api_resource: &ApiResource,
        scope: ApiResourceScope,
        namespace: Option<&str>,
        object: DynamicObject,
        collected_at: SystemTime,
    ) -> Result<ResourceFact, FactError> {
        let name = object
            .metadata
            .name
            .as_deref()
            .ok_or(FactError::new(FactErrorKind::MissingName))?;
        let uid = object
            .metadata
            .uid
            .as_deref()
            .ok_or(FactError::new(FactErrorKind::MissingUid))?;
        let object_namespace = match scope {
            ApiResourceScope::Namespaced => Some(
                NamespaceName::new(
                    object
                        .metadata
                        .namespace
                        .as_deref()
                        .or(namespace)
                        .ok_or(FactError::new(FactErrorKind::NamespaceRequired))?,
                )
                .map_err(|_| FactError::new(FactErrorKind::InvalidObject))?,
            ),
            ApiResourceScope::Cluster => None,
        };
        let identity = ResourceIdentity::new(
            api_resource.group.clone(),
            api_resource.version.clone(),
            api_resource.kind.clone(),
            object_namespace,
            name.to_owned(),
            uid.to_owned(),
        )
        .map_err(|_| FactError::new(FactErrorKind::InvalidObject))?;
        let resource = ResourceRef::new(
            self.target.clone(),
            identity,
            object.metadata.resource_version.clone(),
        )
        .map_err(|_| FactError::new(FactErrorKind::NamespaceOutsideTarget))?;
        let completeness = if object.metadata.resource_version.is_some() {
            FactCompleteness::Complete
        } else {
            FactCompleteness::ResourceVersionMissing
        };
        Ok(ResourceFact {
            resource,
            raw_reference: None,
            object: Arc::new(object),
            collected_at,
            completeness,
            source: FactSource::ApiServer,
        })
    }
}

fn known_scope(gvk: &GroupVersionKind) -> Option<ApiResourceScope> {
    match (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()) {
        ("", "v1", "Namespace" | "Node") => Some(ApiResourceScope::Cluster),
        ("", "v1", "Pod" | "Service")
        | ("apps", "v1", "Deployment" | "StatefulSet" | "DaemonSet")
        | ("batch", "v1", "Job")
        | ("networking.k8s.io", "v1", "Ingress") => Some(ApiResourceScope::Namespaced),
        _ => None,
    }
}

fn map_api_error(error: kube::Error) -> FactError {
    let kind = match error {
        kube::Error::Api(status) => match status.code {
            401 => FactErrorKind::CredentialsRejected,
            403 => FactErrorKind::PermissionDenied,
            404 => FactErrorKind::ResourceNotFound,
            408 | 504 => FactErrorKind::RequestTimedOut,
            429 | 500..=599 => FactErrorKind::ServerUnavailable,
            _ => FactErrorKind::Internal,
        },
        kube::Error::RustlsTls(_) => FactErrorKind::ApiUnavailable,
        kube::Error::HyperError(error) => map_transport_error(&error),
        kube::Error::Service(error) => map_transport_error(error.as_ref()),
        _ => FactErrorKind::Internal,
    };
    FactError::new(kind)
}

fn map_transport_error(error: &(dyn Error + 'static)) -> FactErrorKind {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
            if io_error.kind() == std::io::ErrorKind::TimedOut {
                return FactErrorKind::RequestTimedOut;
            }
        }
        let message = error.to_string().to_ascii_lowercase();
        if message.contains("timeout") || message.contains("timed out") {
            return FactErrorKind::RequestTimedOut;
        }
        if message.contains("connection refused") || message.contains("unreachable") {
            return FactErrorKind::ApiUnavailable;
        }
        current = error.source();
    }
    FactErrorKind::ApiUnavailable
}
