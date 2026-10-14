use crate::{
    cancellation::CancellationToken,
    connection::{
        ApiCapabilitySnapshot, ApiResourceDiscoveryState, ApiResourceScope,
    },
    target::{CacheScope, NamespaceName, ResourceIdentity, ResourceRef, TargetBinding},
};
use async_channel::Receiver;
use futures::{AsyncReadExt, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use jiff::Timestamp;
use kube::{
    Api, Client,
    api::{DynamicObject, ListParams, LogParams},
    core::gvk::GroupVersionKind,
    discovery::ApiResource,
    runtime::watcher::{self, Event as WatchEvent},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use uuid::Uuid;

pub const MAX_FACTS_PER_LIST: usize = 100;
pub const MAX_FACTS_PER_PAGINATED_LIST: usize = 10_000;
pub const FACT_BATCH_SIZE: usize = 16;
pub const MAX_CONCURRENT_CONTAINER_LOG_STREAMS: usize = 3;
pub const MAX_CONTAINER_LOG_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONTAINER_LOG_LINES: usize = 10_000;
pub const MAX_CONTAINER_LOG_LINE_BYTES: usize = 8 * 1024;
pub const MAX_CONTAINER_LOG_TAIL_LINES: usize = 10_000;
const FACT_BATCH_CHANNEL_CAPACITY: usize = 4;
const LOG_BATCH_CHANNEL_CAPACITY: usize = 4;
const LOG_BATCH_LINES: usize = 64;
const DEFAULT_CACHE_ENTRIES: usize = 512;
const DEFAULT_CACHE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CACHE_ENTRY_BYTES: usize = 1024 * 1024;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_PAGINATED_LIST_BYTES: usize = 32 * 1024 * 1024;
const MAX_PAGINATED_LIST_PAGES: usize = MAX_FACTS_PER_PAGINATED_LIST / MAX_FACTS_PER_LIST;
const MAX_WATCH_PAGE_SIZE: usize = 500;
const MAX_WATCH_RETRIES: u8 = 3;
const WATCH_BATCH_INTERVAL: Duration = Duration::from_millis(100);
const WATCH_RETRY_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactErrorKind {
    UnsupportedResource,
    DiscoveryDenied,
    CapabilityUnavailable,
    ListNotSupported,
    WatchUnsupported,
    WatchReconnectExhausted,
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
    InvalidLogRequest,
    ContainerNotFound,
    PreviousLogsUnavailable,
    PodRecreated,
    LogStreamLimitReached,
    LogStreamInterrupted,
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
            FactErrorKind::WatchUnsupported => "resource_watch_unsupported",
            FactErrorKind::WatchReconnectExhausted => "watch_reconnect_exhausted",
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
            FactErrorKind::InvalidLogRequest => "container_log_request_invalid",
            FactErrorKind::ContainerNotFound => "container_not_found",
            FactErrorKind::PreviousLogsUnavailable => "previous_logs_unavailable",
            FactErrorKind::PodRecreated => "pod_recreated_during_log_request",
            FactErrorKind::LogStreamLimitReached => "container_log_stream_limit_reached",
            FactErrorKind::LogStreamInterrupted => "container_log_stream_interrupted",
        }
    }

    pub fn summary(self) -> &'static str {
        match self.kind {
            FactErrorKind::UnsupportedResource => "The API server does not serve this resource version.",
            FactErrorKind::DiscoveryDenied => "API discovery permission is unavailable for this resource group.",
            FactErrorKind::CapabilityUnavailable => "Resource capability could not be determined.",
            FactErrorKind::ListNotSupported => "The API does not support listing this resource.",
            FactErrorKind::WatchUnsupported => "The API does not support watching this resource.",
            FactErrorKind::WatchReconnectExhausted => "Watch recovery stopped after bounded retries.",
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
            FactErrorKind::InvalidLogRequest => "Container log request has an invalid container, time filter, or tail limit.",
            FactErrorKind::ContainerNotFound => "The requested container does not exist in this Pod.",
            FactErrorKind::PreviousLogsUnavailable => "No previous container instance logs are available.",
            FactErrorKind::PodRecreated => "Pod UID changed; logs were not attached to a replacement Pod.",
            FactErrorKind::LogStreamLimitReached => "All bounded container log stream slots are in use.",
            FactErrorKind::LogStreamInterrupted => "Container log stream ended because the connection was interrupted.",
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

    fn mark_watch_scope_stale(
        &self,
        target: &TargetBinding,
        api_resource: &ApiResource,
        scope: ApiResourceScope,
        namespace: Option<&str>,
    ) {
        let cache_scope = target.cache_scope();
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for (key, entry) in &mut inner.entries {
            if cache_key_matches_watch(key, &cache_scope, api_resource, scope, namespace) {
                entry.expires_at = Instant::now();
            }
        }
    }

    fn finish_watch_snapshot(
        &self,
        target: &TargetBinding,
        api_resource: &ApiResource,
        scope: ApiResourceScope,
        namespace: Option<&str>,
    ) {
        let cache_scope = target.cache_scope();
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let stale_keys = inner
            .entries
            .iter()
            .filter(|(key, entry)| {
                entry.expires_at <= now
                    && cache_key_matches_watch(
                        key,
                        &cache_scope,
                        api_resource,
                        scope,
                        namespace,
                    )
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in stale_keys {
            remove_cache_entry(&mut inner, &key);
        }
    }

    fn remove_resource(&self, target: &TargetBinding, identity: &ResourceIdentity) {
        let key = FactCacheKey {
            scope: target.cache_scope(),
            identity: identity.clone(),
        };
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        remove_cache_entry(&mut inner, &key);
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

fn cache_key_matches_watch(
    key: &FactCacheKey,
    cache_scope: &CacheScope,
    api_resource: &ApiResource,
    resource_scope: ApiResourceScope,
    namespace: Option<&str>,
) -> bool {
    if &key.scope != cache_scope
        || key.identity.api_group() != api_resource.group
        || key.identity.api_version() != api_resource.version
        || key.identity.kind() != api_resource.kind
    {
        return false;
    }
    match resource_scope {
        ApiResourceScope::Cluster => key.identity.namespace().is_none(),
        ApiResourceScope::Namespaced => match namespace {
            Some(namespace) => key
                .identity
                .namespace()
                .is_some_and(|identity_namespace| identity_namespace.as_str() == namespace),
            None => key.identity.namespace().is_some(),
        },
    }
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
    WatchBatch(Vec<FactWatchEvent>),
    Complete {
        collection_resource_version: Option<String>,
        collected_at: SystemTime,
        possibly_truncated: bool,
    },
}

#[derive(Debug, Clone)]
pub enum FactWatchEvent {
    SnapshotStarted,
    ResyncStarted,
    Applied(ResourceFact),
    Deleted(ResourceRef),
    SnapshotComplete {
        last_object_resource_version: Option<String>,
    },
    Reconnecting { error_code: &'static str, attempt: u8 },
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

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerLogMode {
    Current,
    Follow,
    Previous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerLogSince {
    Seconds(u64),
    Timestamp(Timestamp),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLogRequest {
    pub container: String,
    pub mode: ContainerLogMode,
    pub since: Option<ContainerLogSince>,
    pub tail_lines: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLogLine {
    pub timestamp: Option<String>,
    pub message: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerLogStreamEvent {
    Batch(Vec<ContainerLogLine>),
    Complete {
        line_count: usize,
        bytes_read: usize,
        possibly_truncated: bool,
    },
}

pub struct ContainerLogStream {
    receiver: Receiver<Result<ContainerLogStreamEvent, FactError>>,
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
    target: TargetBinding,
    pod_identity: ResourceIdentity,
    container: String,
    mode: ContainerLogMode,
}

impl ContainerLogStream {
    pub async fn recv(&self) -> Result<ContainerLogStreamEvent, FactStreamReceiveError> {
        self.receiver
            .recv()
            .await
            .map_err(|_| FactStreamReceiveError::Closed)?
            .map_err(FactStreamReceiveError::Service)
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn pod_identity(&self) -> &ResourceIdentity {
        &self.pod_identity
    }

    pub fn container(&self) -> &str {
        &self.container
    }

    pub fn mode(&self) -> ContainerLogMode {
        self.mode
    }
}

impl Drop for ContainerLogStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub struct ReadOnlyFactService {
    client: Client,
    target: TargetBinding,
    capabilities: ApiCapabilitySnapshot,
    cancellation: CancellationToken,
    runtime: Handle,
    cache: SharedFactCache,
    request_timeout: Duration,
    log_stream_slots: Arc<Semaphore>,
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
        log_stream_slots: Arc<Semaphore>,
    ) -> Self {
        Self {
            client,
            target,
            capabilities,
            cancellation,
            runtime,
            cache,
            request_timeout,
            log_stream_slots,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn cache(&self) -> &SharedFactCache {
        &self.cache
    }

    pub fn container_log_stream(
        &self,
        pod: &ResourceFact,
        request: ContainerLogRequest,
    ) -> Result<ContainerLogStream, FactError> {
        let identity = pod.resource().identity();
        if pod.resource().target() != &self.target
            || identity.api_group() != ""
            || identity.api_version() != "v1"
            || identity.kind() != "Pod"
        {
            return Err(FactError::new(FactErrorKind::CacheScopeMismatch));
        }
        validate_container_log_request(&request)?;
        if !pod_data_has_container(&pod.object().data, &request.container) {
            return Err(FactError::new(FactErrorKind::ContainerNotFound));
        }
        let namespace = identity
            .namespace()
            .ok_or_else(|| FactError::new(FactErrorKind::NamespaceRequired))?
            .as_str()
            .to_owned();
        let permit = acquire_log_stream_slot(self.log_stream_slots.clone())?;
        let (sender, receiver) = async_channel::bounded(LOG_BATCH_CHANNEL_CAPACITY);
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let service = self.clone();
        let pod_name = identity.name().to_owned();
        let pod_uid = identity.uid().to_owned();
        let stream_target = self.target.clone();
        let stream_identity = identity.clone();
        let stream_container = request.container.clone();
        let mode = request.mode;
        let error_sender = sender.clone();
        let task = self.runtime.spawn(async move {
            let result = service
                .run_container_log_stream(
                    pod_name,
                    pod_uid,
                    namespace,
                    request,
                    permit,
                    worker_cancellation.clone(),
                    sender,
                )
                .await;
            if let Err(error) = result {
                send_container_log_event(
                    &error_sender,
                    Err(error),
                    &service.cancellation,
                    &worker_cancellation,
                )
                .await;
            }
        });
        Ok(ContainerLogStream {
            receiver,
            cancellation,
            task: Some(task),
            target: stream_target,
            pod_identity: stream_identity,
            container: stream_container,
            mode,
        })
    }

    async fn run_container_log_stream(
        &self,
        pod_name: String,
        pod_uid: String,
        namespace: String,
        request: ContainerLogRequest,
        _permit: OwnedSemaphorePermit,
        request_cancellation: CancellationToken,
        sender: async_channel::Sender<Result<ContainerLogStreamEvent, FactError>>,
    ) -> Result<(), FactError> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
        let current_pod = get_pod_for_log(
            &pods,
            &pod_name,
            self.request_timeout,
            &self.cancellation,
            &request_cancellation,
        )
        .await?;
        ensure_pod_uid(&current_pod, &pod_uid)?;
        let current_data = serde_json::to_value(&current_pod)
            .map_err(|_| FactError::new(FactErrorKind::SerializationFailed))?;
        if !pod_value_has_container(&current_data, &request.container) {
            return Err(FactError::new(FactErrorKind::ContainerNotFound));
        }

        let params = container_log_params(&request)?;
        let opened = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            _ = request_cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            result = tokio::time::timeout(
                self.request_timeout,
                pods.log_stream(&pod_name, &params),
            ) => result,
        };
        let log_reader = match opened {
            Err(_) => return Err(FactError::new(FactErrorKind::RequestTimedOut)),
            Ok(Ok(reader)) => reader,
            Ok(Err(error)) => {
                let status_code = match &error {
                    kube::Error::Api(status) => Some(status.code),
                    _ => None,
                };
                if matches!(status_code, Some(400 | 404)) {
                    let live_pod = get_pod_for_log(
                        &pods,
                        &pod_name,
                        self.request_timeout,
                        &self.cancellation,
                        &request_cancellation,
                    )
                    .await?;
                    ensure_pod_uid(&live_pod, &pod_uid)?;
                }
                return Err(map_log_open_error(error, request.mode));
            }
        };

        let live_pod = get_pod_for_log(
            &pods,
            &pod_name,
            self.request_timeout,
            &self.cancellation,
            &request_cancellation,
        )
        .await?;
        ensure_pod_uid(&live_pod, &pod_uid)?;

        stream_container_log_lines(
            Box::pin(log_reader),
            sender,
            self.cancellation.clone(),
            request_cancellation,
        )
        .await
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

        pub fn list_paginated_stream(
            &self,
            gvk: GroupVersionKind,
            namespace: Option<String>,
            limit: usize,
        ) -> Result<FactBatchStream, FactError> {
            if limit == 0 || limit > MAX_FACTS_PER_PAGINATED_LIST {
                return Err(FactError::new(FactErrorKind::InvalidLimit));
            }
            let capability = self
                .capabilities
                .resource_gvk(&gvk)
                .ok_or(FactError::new(FactErrorKind::UnsupportedResource))?;
            match capability.state() {
                ApiResourceDiscoveryState::DiscoveryDenied => {
                    return Err(FactError::new(FactErrorKind::DiscoveryDenied));
                }
                ApiResourceDiscoveryState::Unsupported => {
                    return Err(FactError::new(FactErrorKind::UnsupportedResource));
                }
                ApiResourceDiscoveryState::TimedOut | ApiResourceDiscoveryState::Unavailable => {
                    return Err(FactError::new(FactErrorKind::CapabilityUnavailable));
                }
                ApiResourceDiscoveryState::Served { .. } => {}
            }
            if !capability.is_list_supported() {
                return Err(FactError::new(FactErrorKind::ListNotSupported));
            }
            let (api_resource, scope) = self.resolve_resource(&gvk)?;
            let namespace = self.resolve_namespace(scope, namespace.as_deref())?;
            let api: Api<DynamicObject> = match &namespace {
                Some(namespace) => Api::namespaced_with(self.client.clone(), namespace, &api_resource),
                None => Api::all_with(self.client.clone(), &api_resource),
            };
            let (sender, receiver) = async_channel::bounded(FACT_BATCH_CHANNEL_CAPACITY);
            let request_cancellation = CancellationToken::new();
            let worker_cancellation = request_cancellation.clone();
            let service = self.clone();
            let task = self.runtime.spawn(async move {
                service
                    .run_paginated_list_stream(
                        api,
                        api_resource,
                        scope,
                        namespace,
                        limit,
                        worker_cancellation,
                        sender,
                    )
                    .await;
            });
            Ok(FactBatchStream {
                receiver,
                cancellation: request_cancellation,
                task: Some(task),
            })
        }
    pub fn watch_stream(
        &self,
        gvk: GroupVersionKind,
        namespace: Option<String>,
        page_size: usize,
    ) -> Result<FactBatchStream, FactError> {
        if page_size == 0 || page_size > MAX_WATCH_PAGE_SIZE {
            return Err(FactError::new(FactErrorKind::InvalidLimit));
        }
        let capability = self
            .capabilities
            .resource_gvk(&gvk)
            .ok_or(FactError::new(FactErrorKind::UnsupportedResource))?;
        match capability.state() {
            ApiResourceDiscoveryState::DiscoveryDenied => {
                return Err(FactError::new(FactErrorKind::DiscoveryDenied));
            }
            ApiResourceDiscoveryState::Unsupported => {
                return Err(FactError::new(FactErrorKind::UnsupportedResource));
            }
            ApiResourceDiscoveryState::TimedOut | ApiResourceDiscoveryState::Unavailable => {
                return Err(FactError::new(FactErrorKind::CapabilityUnavailable));
            }
            ApiResourceDiscoveryState::Served { .. } => {}
        }
        if !capability.is_list_supported() {
            return Err(FactError::new(FactErrorKind::ListNotSupported));
        }
        if !capability.is_watch_supported() {
            return Err(FactError::new(FactErrorKind::WatchUnsupported));
        }
        let (api_resource, scope) = self.resolve_resource(&gvk)?;
        let namespace = self.resolve_namespace(scope, namespace.as_deref())?;
        let api: Api<DynamicObject> = match &namespace {
            Some(namespace) => Api::namespaced_with(self.client.clone(), namespace, &api_resource),
            None => Api::all_with(self.client.clone(), &api_resource),
        };
        let (sender, receiver) = async_channel::bounded(FACT_BATCH_CHANNEL_CAPACITY);
        let request_cancellation = CancellationToken::new();
        let worker_cancellation = request_cancellation.clone();
        let service = self.clone();
        let task = self.runtime.spawn(async move {
            service
                .run_watch_stream(
                    api,
                    page_size as u32,
                    api_resource,
                    scope,
                    namespace,
                    worker_cancellation,
                    sender,
                )
                .await;
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

    async fn run_paginated_list_stream(
        &self,
        api: Api<DynamicObject>,
        api_resource: ApiResource,
        scope: ApiResourceScope,
        namespace: Option<String>,
        limit: usize,
        request_cancellation: CancellationToken,
        sender: async_channel::Sender<Result<FactStreamEvent, FactError>>,
    ) {
        let mut count = 0_usize;
        let mut total_bytes = 0_usize;
        let mut collection_resource_version = None;
        let mut continuation: Option<String> = None;
        let mut seen_continuations = HashSet::new();
        let mut possibly_truncated = false;

        for _ in 0..MAX_PAGINATED_LIST_PAGES {
            if count >= limit {
                possibly_truncated = continuation.is_some();
                break;
            }
            let page_limit = (limit - count).min(MAX_FACTS_PER_LIST);
            let mut params = ListParams::default().limit(page_limit as u32);
            if let Some(token) = &continuation {
                params = params.continue_token(token);
            }
            let page = tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = request_cancellation.cancelled() => return,
                result = tokio::time::timeout(self.request_timeout, api.list(&params)) => {
                    match result {
                        Ok(Ok(page)) => page,
                        Ok(Err(error)) => {
                            let _ = send_paginated_error(
                                &sender,
                                map_api_error(error),
                                &self.cancellation,
                                &request_cancellation,
                            ).await;
                            return;
                        }
                        Err(_) => {
                            let _ = send_paginated_error(
                                &sender,
                                FactError::new(FactErrorKind::RequestTimedOut),
                                &self.cancellation,
                                &request_cancellation,
                            ).await;
                            return;
                        }
                    }
                }
            };
            if collection_resource_version.is_none() {
                collection_resource_version = page.metadata.resource_version.clone();
            }
            let next_continuation = page.metadata.continue_.clone();
            let mut batch = Vec::with_capacity(FACT_BATCH_SIZE);
            for object in page.items {
                if count >= limit {
                    possibly_truncated = true;
                    break;
                }
                let fact = match self.make_fact(
                    &api_resource,
                    scope,
                    namespace.as_deref(),
                    object,
                    SystemTime::now(),
                ) {
                    Ok(fact) => fact,
                    Err(error) => {
                        let _ = send_paginated_error(
                            &sender,
                            error,
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                };
                let bytes = match serde_json::to_vec(fact.object.as_ref()) {
                    Ok(bytes) => bytes.len(),
                    Err(_) => {
                        let _ = send_paginated_error(
                            &sender,
                            FactError::new(FactErrorKind::SerializationFailed),
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                };
                if total_bytes.saturating_add(bytes) > MAX_PAGINATED_LIST_BYTES {
                    possibly_truncated = true;
                    break;
                }
                total_bytes += bytes;
                count += 1;
                batch.push(self.cache.insert(fact, bytes));
                if batch.len() == FACT_BATCH_SIZE
                    && !send_paginated_batch(
                        &sender,
                        &mut batch,
                        &self.cancellation,
                        &request_cancellation,
                    )
                    .await
                {
                    return;
                }
            }
            if !send_paginated_batch(
                &sender,
                &mut batch,
                &self.cancellation,
                &request_cancellation,
            )
            .await
            {
                return;
            }
            let Some(next) = next_continuation else {
                continuation = None;
                break;
            };
            if possibly_truncated {
                break;
            }
            if !seen_continuations.insert(next.clone()) {
                possibly_truncated = true;
                break;
            }
            continuation = Some(next);
        }

        if continuation.is_some() {
            possibly_truncated = true;
        }
        tokio::select! {
            _ = self.cancellation.cancelled() => return,
            _ = request_cancellation.cancelled() => return,
            _ = sender.send(Ok(FactStreamEvent::Complete {
                collection_resource_version,
                collected_at: SystemTime::now(),
                possibly_truncated,
            })) => {}
        }
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

    async fn run_watch_stream(
        &self,
        api: Api<DynamicObject>,
        page_size: u32,
        api_resource: ApiResource,
        scope: ApiResourceScope,
        namespace: Option<String>,
        request_cancellation: CancellationToken,
        sender: async_channel::Sender<Result<FactStreamEvent, FactError>>,
    ) {
        let mut pending = Vec::with_capacity(FACT_BATCH_SIZE);
        let mut last_resource_version = None;
        let mut retry_count = 0_u8;
        let mut stream = build_watch_stream(
            api.clone(),
            page_size,
            self.request_timeout.as_secs().min(295) as u32,
        );
        let mut flush_timer = tokio::time::interval_at(
            tokio::time::Instant::now() + WATCH_BATCH_INTERVAL,
            WATCH_BATCH_INTERVAL,
        );
        loop {
            let next = tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = request_cancellation.cancelled() => return,
                _ = flush_timer.tick(), if !pending.is_empty() => {
                    if !send_watch_batch(&sender, &mut pending, &self.cancellation, &request_cancellation).await {
                        return;
                    }
                    continue;
                }
                next = stream.next() => next,
            };

            match next {
                Some(Ok(WatchEvent::Init)) => {
                    self.cache.mark_watch_scope_stale(
                        &self.target,
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                    );
                    last_resource_version = None;
                    pending.push(FactWatchEvent::SnapshotStarted);
                }
                Some(Ok(WatchEvent::InitApply(object))) => {
                    match self.fact_from_watch_object(
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                        object,
                    ) {
                        Ok((fact, resource_version)) => {
                            last_resource_version = resource_version.or(last_resource_version);
                            let fact = self.cache_fact(fact);
                            pending.push(FactWatchEvent::Applied(fact));
                        }
                        Err(error) => {
                            let _ = send_watch_terminal_error(
                                &sender,
                                &mut pending,
                                error,
                                &self.cancellation,
                                &request_cancellation,
                            )
                            .await;
                            return;
                        }
                    }
                }
                Some(Ok(WatchEvent::InitDone)) => {
                    self.cache.finish_watch_snapshot(
                        &self.target,
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                    );
                    pending.push(FactWatchEvent::SnapshotComplete {
                        last_object_resource_version: last_resource_version.clone(),
                    });
                }
                Some(Ok(WatchEvent::Apply(object))) => {
                    match self.fact_from_watch_object(
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                        object,
                    ) {
                        Ok((fact, resource_version)) => {
                            last_resource_version = resource_version.or(last_resource_version);
                            pending.push(FactWatchEvent::Applied(self.cache_fact(fact)));
                            retry_count = 0;
                        }
                        Err(error) => {
                            let _ = send_watch_terminal_error(
                                &sender,
                                &mut pending,
                                error,
                                &self.cancellation,
                                &request_cancellation,
                            )
                            .await;
                            return;
                        }
                    }
                }
                Some(Ok(WatchEvent::Delete(object))) => {
                    match self.fact_from_watch_object(
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                        object,
                    ) {
                        Ok((fact, resource_version)) => {
                            last_resource_version = resource_version.or(last_resource_version);
                            self.cache.remove_resource(&self.target, fact.resource.identity());
                            pending.push(FactWatchEvent::Deleted(fact.resource));
                            retry_count = 0;
                        }
                        Err(error) => {
                            let _ = send_watch_terminal_error(
                                &sender,
                                &mut pending,
                                error,
                                &self.cancellation,
                                &request_cancellation,
                            )
                            .await;
                            return;
                        }
                    }
                }
                Some(Err(error)) if watch_resource_version_expired(&error) => {
                    self.cache.mark_watch_scope_stale(
                        &self.target,
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                    );
                    retry_count = retry_count.saturating_add(1);
                    if retry_count > MAX_WATCH_RETRIES {
                        let _ = send_watch_terminal_error(
                            &sender,
                            &mut pending,
                            FactError::new(FactErrorKind::WatchReconnectExhausted),
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                    pending.push(FactWatchEvent::ResyncStarted);
                    stream = build_watch_stream(
                        api.clone(),
                        page_size,
                        self.request_timeout.as_secs().min(295) as u32,
                    );
                }
                Some(Err(error)) => {
                    let fact_error = map_watcher_error(&error);
                    self.cache.mark_watch_scope_stale(
                        &self.target,
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                    );
                    if matches!(
                        fact_error.kind(),
                        FactErrorKind::PermissionDenied
                            | FactErrorKind::CredentialsRejected
                            | FactErrorKind::WatchUnsupported
                            | FactErrorKind::UnsupportedResource
                    ) {
                        let _ = send_watch_terminal_error(
                            &sender,
                            &mut pending,
                            fact_error,
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                    retry_count = retry_count.saturating_add(1);
                    if retry_count > MAX_WATCH_RETRIES {
                        let _ = send_watch_terminal_error(
                            &sender,
                            &mut pending,
                            FactError::new(FactErrorKind::WatchReconnectExhausted),
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                    pending.push(FactWatchEvent::Reconnecting {
                        error_code: fact_error.error_code(),
                        attempt: retry_count,
                    });
                    tokio::select! {
                        _ = self.cancellation.cancelled() => return,
                        _ = request_cancellation.cancelled() => return,
                        _ = tokio::time::sleep(watch_retry_delay(retry_count)) => {}
                    }
                    stream = build_watch_stream(
                        api.clone(),
                        page_size,
                        self.request_timeout.as_secs().min(295) as u32,
                    );
                }
                None => {
                    self.cache.mark_watch_scope_stale(
                        &self.target,
                        &api_resource,
                        scope,
                        namespace.as_deref(),
                    );
                    retry_count = retry_count.saturating_add(1);
                    if retry_count > MAX_WATCH_RETRIES {
                        let _ = send_watch_terminal_error(
                            &sender,
                            &mut pending,
                            FactError::new(FactErrorKind::WatchReconnectExhausted),
                            &self.cancellation,
                            &request_cancellation,
                        )
                        .await;
                        return;
                    }
                    pending.push(FactWatchEvent::Reconnecting {
                        error_code: "watch_stream_closed",
                        attempt: retry_count,
                    });
                    tokio::select! {
                        _ = self.cancellation.cancelled() => return,
                        _ = request_cancellation.cancelled() => return,
                        _ = tokio::time::sleep(watch_retry_delay(retry_count)) => {}
                    }
                    stream = build_watch_stream(
                        api.clone(),
                        page_size,
                        self.request_timeout.as_secs().min(295) as u32,
                    );
                }
            }

            if pending.len() >= FACT_BATCH_SIZE
                && !send_watch_batch(&sender, &mut pending, &self.cancellation, &request_cancellation).await
            {
                return;
            }
        }
    }

    fn fact_from_watch_object(
        &self,
        api_resource: &ApiResource,
        scope: ApiResourceScope,
        namespace: Option<&str>,
        object: DynamicObject,
    ) -> Result<(ResourceFact, Option<String>), FactError> {
        let resource_version = object.metadata.resource_version.clone();
        let fact = self.make_fact(
            api_resource,
            scope,
            namespace,
            object,
            SystemTime::now(),
        )?;
        Ok((fact, resource_version))
    }

    fn cache_fact(&self, mut fact: ResourceFact) -> ResourceFact {
        let bytes = serde_json::to_vec(fact.object.as_ref())
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        fact = self.cache.insert(fact, bytes);
        fact
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

async fn send_paginated_batch(
    sender: &async_channel::Sender<Result<FactStreamEvent, FactError>>,
    pending: &mut Vec<ResourceFact>,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> bool {
    if pending.is_empty() {
        return true;
    }
    let batch = std::mem::take(pending);
    tokio::select! {
        _ = session_cancellation.cancelled() => false,
        _ = request_cancellation.cancelled() => false,
        result = sender.send(Ok(FactStreamEvent::Batch(batch))) => result.is_ok(),
    }
}

async fn send_paginated_error(
    sender: &async_channel::Sender<Result<FactStreamEvent, FactError>>,
    error: FactError,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = session_cancellation.cancelled() => false,
        _ = request_cancellation.cancelled() => false,
        result = sender.send(Err(error)) => result.is_ok(),
    }
}

fn build_watch_stream(
    api: Api<DynamicObject>,
    page_size: u32,
    timeout_seconds: u32,
) -> futures::stream::BoxStream<'static, watcher::Result<WatchEvent<DynamicObject>>> {
    let config = watcher::Config::default()
        .page_size(page_size)
        .timeout(timeout_seconds);
    Box::pin(watcher::watcher(api, config))
}

async fn send_watch_batch(
    sender: &async_channel::Sender<Result<FactStreamEvent, FactError>>,
    pending: &mut Vec<FactWatchEvent>,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> bool {
    if pending.is_empty() {
        return true;
    }
    let batch = std::mem::take(pending);
    tokio::select! {
        _ = session_cancellation.cancelled() => false,
        _ = request_cancellation.cancelled() => false,
        result = sender.send(Ok(FactStreamEvent::WatchBatch(batch))) => result.is_ok(),
    }
}

async fn send_watch_terminal_error(
    sender: &async_channel::Sender<Result<FactStreamEvent, FactError>>,
    pending: &mut Vec<FactWatchEvent>,
    error: FactError,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> bool {
    if !send_watch_batch(sender, pending, session_cancellation, request_cancellation).await {
        return false;
    }
    tokio::select! {
        _ = session_cancellation.cancelled() => false,
        _ = request_cancellation.cancelled() => false,
        result = sender.send(Err(error)) => result.is_ok(),
    }
}

fn watch_resource_version_expired(error: &watcher::Error) -> bool {
    match error {
        watcher::Error::WatchError(status) => status.code == 410,
        watcher::Error::InitialListFailed(error)
        | watcher::Error::WatchStartFailed(error)
        | watcher::Error::WatchFailed(error) => {
            matches!(error, kube::Error::Api(status) if status.code == 410)
        }
        watcher::Error::NoResourceVersion => false,
    }
}

fn watch_retry_delay(attempt: u8) -> Duration {
    WATCH_RETRY_BACKOFF * u32::from(attempt.min(MAX_WATCH_RETRIES))
}

fn validate_container_log_request(request: &ContainerLogRequest) -> Result<(), FactError> {
    if request.container.is_empty()
        || request.container.len() > 253
        || request.container.chars().any(char::is_control)
        || request
            .tail_lines
            .is_some_and(|lines| lines == 0 || lines > MAX_CONTAINER_LOG_TAIL_LINES)
        || request.since.as_ref().is_some_and(|since| {
            matches!(since, ContainerLogSince::Seconds(0))
                || matches!(since, ContainerLogSince::Seconds(seconds) if *seconds > i64::MAX as u64)
        })
    {
        return Err(FactError::new(FactErrorKind::InvalidLogRequest));
    }
    Ok(())
}

fn container_log_params(request: &ContainerLogRequest) -> Result<LogParams, FactError> {
    validate_container_log_request(request)?;
    let (since_seconds, since_time) = match request.since.as_ref() {
        Some(ContainerLogSince::Seconds(seconds)) => (Some(*seconds as i64), None),
        Some(ContainerLogSince::Timestamp(timestamp)) => (None, Some(timestamp.clone())),
        None => (None, None),
    };
    Ok(LogParams {
        container: Some(request.container.clone()),
        follow: request.mode == ContainerLogMode::Follow,
        limit_bytes: Some(MAX_CONTAINER_LOG_BYTES as i64),
        previous: request.mode == ContainerLogMode::Previous,
        since_seconds,
        since_time,
        tail_lines: Some(request.tail_lines.unwrap_or(MAX_CONTAINER_LOG_TAIL_LINES) as i64),
        timestamps: true,
        ..LogParams::default()
    })
}

fn acquire_log_stream_slot(slots: Arc<Semaphore>) -> Result<OwnedSemaphorePermit, FactError> {
    slots
        .try_acquire_owned()
        .map_err(|_| FactError::new(FactErrorKind::LogStreamLimitReached))
}

fn pod_data_has_container(data: &serde_json::Value, container: &str) -> bool {
    ["containers", "initContainers", "ephemeralContainers"]
        .into_iter()
        .filter_map(|key| data.get("spec")?.get(key)?.as_array())
        .flatten()
        .any(|candidate| {
            candidate.get("name").and_then(serde_json::Value::as_str) == Some(container)
        })
}

fn pod_value_has_container(value: &serde_json::Value, container: &str) -> bool {
    pod_data_has_container(value, container)
}

async fn get_pod_for_log(
    pods: &Api<Pod>,
    name: &str,
    timeout: Duration,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> Result<Pod, FactError> {
    tokio::select! {
        _ = session_cancellation.cancelled() => {
            Err(FactError::new(FactErrorKind::SessionCancelled))
        }
        _ = request_cancellation.cancelled() => {
            Err(FactError::new(FactErrorKind::SessionCancelled))
        }
        result = tokio::time::timeout(timeout, pods.get(name)) => {
            match result {
                Err(_) => Err(FactError::new(FactErrorKind::RequestTimedOut)),
                Ok(Err(error)) => Err(map_api_error(error)),
                Ok(Ok(pod)) => Ok(pod),
            }
        }
    }
}

fn ensure_pod_uid(pod: &Pod, expected_uid: &str) -> Result<(), FactError> {
    let actual_uid = pod
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| FactError::new(FactErrorKind::MissingUid))?;
    if actual_uid != expected_uid {
        return Err(FactError::new(FactErrorKind::PodRecreated));
    }
    Ok(())
}

fn map_log_open_error(error: kube::Error, mode: ContainerLogMode) -> FactError {
    if let kube::Error::Api(status) = &error {
        if mode == ContainerLogMode::Previous && matches!(status.code, 400 | 404) {
            return FactError::new(FactErrorKind::PreviousLogsUnavailable);
        }
        if status.code == 400 {
            return FactError::new(FactErrorKind::ContainerNotFound);
        }
    }
    map_api_error(error)
}

async fn send_container_log_event(
    sender: &async_channel::Sender<Result<ContainerLogStreamEvent, FactError>>,
    event: Result<ContainerLogStreamEvent, FactError>,
    session_cancellation: &CancellationToken,
    request_cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = session_cancellation.cancelled() => false,
        _ = request_cancellation.cancelled() => false,
        result = sender.send(event) => result.is_ok(),
    }
}

async fn stream_container_log_lines<R>(
    mut reader: R,
    sender: async_channel::Sender<Result<ContainerLogStreamEvent, FactError>>,
    session_cancellation: CancellationToken,
    request_cancellation: CancellationToken,
) -> Result<(), FactError>
where
    R: futures::io::AsyncRead + Unpin,
{
    let mut input = [0_u8; 8192];
    let mut line_bytes = Vec::with_capacity(MAX_CONTAINER_LOG_LINE_BYTES);
    let mut line_truncated = false;
    let mut any_line_truncated = false;
    let mut batch = Vec::with_capacity(LOG_BATCH_LINES);
    let mut line_count = 0_usize;
    let mut bytes_read = 0_usize;
    let mut possibly_truncated = false;

    loop {
        let remaining = MAX_CONTAINER_LOG_BYTES.saturating_sub(bytes_read);
        if remaining == 0 || line_count >= MAX_CONTAINER_LOG_LINES {
            possibly_truncated = true;
            break;
        }
        let read_limit = remaining.min(input.len());
        let read = tokio::select! {
            _ = session_cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            _ = request_cancellation.cancelled() => {
                return Err(FactError::new(FactErrorKind::SessionCancelled));
            }
            result = reader.read(&mut input[..read_limit]) => {
                result.map_err(|_| FactError::new(FactErrorKind::LogStreamInterrupted))?
            }
        };
        if read == 0 {
            break;
        }
        bytes_read = bytes_read.saturating_add(read);

        for byte in &input[..read] {
            if *byte == b'\n' {
                batch.push(parse_container_log_line(&line_bytes, line_truncated));
                line_bytes.clear();
                line_truncated = false;
                line_count = line_count.saturating_add(1);
                if batch.len() == LOG_BATCH_LINES {
                    let lines = std::mem::replace(&mut batch, Vec::with_capacity(LOG_BATCH_LINES));
                    if !send_container_log_event(
                        &sender,
                        Ok(ContainerLogStreamEvent::Batch(lines)),
                        &session_cancellation,
                        &request_cancellation,
                    )
                    .await
                    {
                        return Err(FactError::new(FactErrorKind::SessionCancelled));
                    }
                }
                if line_count == MAX_CONTAINER_LOG_LINES {
                    possibly_truncated = true;
                    break;
                }
            } else if line_bytes.len() < MAX_CONTAINER_LOG_LINE_BYTES {
                line_bytes.push(*byte);
            } else {
                line_truncated = true;
                any_line_truncated = true;
            }
        }
        if possibly_truncated {
            break;
        }
        if bytes_read == MAX_CONTAINER_LOG_BYTES {
            possibly_truncated = true;
            break;
        }
    }

    if !line_bytes.is_empty() || line_truncated {
        batch.push(parse_container_log_line(&line_bytes, line_truncated));
        line_count = line_count.saturating_add(1);
    }
    if !batch.is_empty()
        && !send_container_log_event(
            &sender,
            Ok(ContainerLogStreamEvent::Batch(batch)),
            &session_cancellation,
            &request_cancellation,
        )
        .await
    {
        return Err(FactError::new(FactErrorKind::SessionCancelled));
    }
    if !send_container_log_event(
        &sender,
        Ok(ContainerLogStreamEvent::Complete {
            line_count,
            bytes_read,
            possibly_truncated: possibly_truncated || any_line_truncated,
        }),
        &session_cancellation,
        &request_cancellation,
    )
    .await
    {
        return Err(FactError::new(FactErrorKind::SessionCancelled));
    }
    Ok(())
}

fn parse_container_log_line(bytes: &[u8], truncated: bool) -> ContainerLogLine {
    let mut bytes = bytes;
    if bytes.last() == Some(&b'\r') {
        bytes = &bytes[..bytes.len() - 1];
    }
    let line = String::from_utf8_lossy(bytes).into_owned();
    if let Some((timestamp, message)) = line.split_once(' ')
        && timestamp.parse::<Timestamp>().is_ok()
    {
        ContainerLogLine {
            timestamp: Some(timestamp.to_owned()),
            message: message.to_owned(),
            truncated,
        }
    } else {
        ContainerLogLine {
            timestamp: None,
            message: line,
            truncated,
        }
    }
}

fn map_watcher_error(error: &watcher::Error) -> FactError {
    match error {
        watcher::Error::InitialListFailed(error)
        | watcher::Error::WatchStartFailed(error)
        | watcher::Error::WatchFailed(error) => map_api_error_ref(error),
        watcher::Error::WatchError(status) => map_watch_status(status.code),
        watcher::Error::NoResourceVersion => FactError::new(FactErrorKind::InvalidObject),
    }
}

fn map_api_error_ref(error: &kube::Error) -> FactError {
    let kind = match error {
        kube::Error::Api(status) => return map_watch_status(status.code),
        kube::Error::RustlsTls(_) => FactErrorKind::ApiUnavailable,
        kube::Error::HyperError(error) => map_transport_error(error),
        kube::Error::Service(error) => map_transport_error(error.as_ref()),
        _ => FactErrorKind::Internal,
    };
    FactError::new(kind)
}

fn map_watch_status(code: u16) -> FactError {
    FactError::new(match code {
        401 => FactErrorKind::CredentialsRejected,
        403 => FactErrorKind::PermissionDenied,
        404 => FactErrorKind::WatchUnsupported,
        408 | 504 => FactErrorKind::RequestTimedOut,
        429 | 500..=599 => FactErrorKind::ServerUnavailable,
        _ => FactErrorKind::Internal,
    })
}

fn known_scope(gvk: &GroupVersionKind) -> Option<ApiResourceScope> {
    match (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()) {
        ("", "v1", "Namespace" | "Node") => Some(ApiResourceScope::Cluster),
        ("", "v1", "Pod" | "Service" | "Endpoints" | "Event")
        | ("apps", "v1", "Deployment" | "StatefulSet" | "DaemonSet" | "ReplicaSet")
        | ("batch", "v1", "Job")
        | ("networking.k8s.io", "v1", "Ingress")
        | ("discovery.k8s.io", "v1", "EndpointSlice") => Some(ApiResourceScope::Namespaced),
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

#[cfg(test)]
mod watch_tests {
    use super::*;

    #[test]
    fn watch_batch_send_waits_for_channel_capacity() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (sender, receiver) = async_channel::bounded(1);
            sender
                .send(Ok(FactStreamEvent::WatchBatch(Vec::new())))
                .await
                .unwrap();
            let session_cancellation = CancellationToken::new();
            let request_cancellation = CancellationToken::new();
            let task_sender = sender.clone();
            let task_session_cancellation = session_cancellation.clone();
            let task_request_cancellation = request_cancellation.clone();
            let task = tokio::spawn(async move {
                let mut pending = vec![FactWatchEvent::ResyncStarted];
                send_watch_batch(
                    &task_sender,
                    &mut pending,
                    &task_session_cancellation,
                    &task_request_cancellation,
                )
                .await
            });

            tokio::task::yield_now().await;
            assert!(!task.is_finished());
            receiver.recv().await.unwrap().unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert!(matches!(
                receiver.recv().await.unwrap(),
                Ok(FactStreamEvent::WatchBatch(batch))
                    if matches!(batch.as_slice(), [FactWatchEvent::ResyncStarted])
            ));
        });
    }

    #[test]
    fn watch_batch_send_cancels_while_channel_is_full() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for cancel_session in [false, true] {
                let (sender, receiver) = async_channel::bounded(1);
                sender
                    .send(Ok(FactStreamEvent::WatchBatch(Vec::new())))
                    .await
                    .unwrap();
                let session_cancellation = CancellationToken::new();
                let request_cancellation = CancellationToken::new();
                let task_sender = sender.clone();
                let task_session_cancellation = session_cancellation.clone();
                let task_request_cancellation = request_cancellation.clone();
                let task = tokio::spawn(async move {
                    let mut pending = vec![FactWatchEvent::ResyncStarted];
                    send_watch_batch(
                        &task_sender,
                        &mut pending,
                        &task_session_cancellation,
                        &task_request_cancellation,
                    )
                    .await
                });

                tokio::task::yield_now().await;
                assert!(!task.is_finished());
                if cancel_session {
                    session_cancellation.cancel();
                } else {
                    request_cancellation.cancel();
                }
                assert!(
                    !tokio::time::timeout(Duration::from_secs(1), task)
                        .await
                        .unwrap()
                        .unwrap()
                );
                assert_eq!(receiver.len(), 1);
            }
        });
    }

    #[test]
    fn watch_retry_delay_is_bounded() {
        assert_eq!(watch_retry_delay(1), WATCH_RETRY_BACKOFF);
        assert_eq!(watch_retry_delay(MAX_WATCH_RETRIES), WATCH_RETRY_BACKOFF * 3);
        assert_eq!(watch_retry_delay(u8::MAX), WATCH_RETRY_BACKOFF * 3);
    }
}

#[cfg(test)]
mod container_log_tests {
    use super::*;
    use futures::io::Cursor;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn collect_log_lines(input: Vec<u8>) -> (Vec<ContainerLogLine>, usize, usize, bool) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (sender, receiver) = async_channel::bounded(LOG_BATCH_CHANNEL_CAPACITY);
            let task = tokio::spawn(stream_container_log_lines(
                Cursor::new(input),
                sender,
                CancellationToken::new(),
                CancellationToken::new(),
            ));
            let mut lines = Vec::new();
            let (line_count, bytes_read, possibly_truncated) = loop {
                match receiver.recv().await.unwrap() {
                    Ok(ContainerLogStreamEvent::Batch(batch)) => lines.extend(batch),
                    Ok(ContainerLogStreamEvent::Complete {
                        line_count,
                        bytes_read,
                        possibly_truncated,
                    }) => break (line_count, bytes_read, possibly_truncated),
                    Err(error) => panic!("unexpected log error: {}", error.error_code()),
                }
            };
            task.await.unwrap().unwrap();
            (lines, line_count, bytes_read, possibly_truncated)
        })
    }

    #[test]
    fn log_request_sets_mode_timestamp_and_time_window_parameters() {
        let timestamp = "2026-10-14T12:00:00Z".parse::<Timestamp>().unwrap();
        let request = ContainerLogRequest {
            container: "api".to_owned(),
            mode: ContainerLogMode::Follow,
            since: Some(ContainerLogSince::Timestamp(timestamp)),
            tail_lines: Some(128),
        };
        let params = container_log_params(&request).unwrap();
        assert!(params.follow);
        assert!(!params.previous);
        assert!(params.timestamps);
        assert_eq!(params.container.as_deref(), Some("api"));
        assert_eq!(params.tail_lines, Some(128));
        assert_eq!(params.limit_bytes, Some(MAX_CONTAINER_LOG_BYTES as i64));
        assert!(params.since_time.is_some());
        assert_eq!(params.since_seconds, None);

        let previous = container_log_params(&ContainerLogRequest {
            mode: ContainerLogMode::Previous,
            since: Some(ContainerLogSince::Seconds(3600)),
            ..request
        })
        .unwrap();
        assert!(!previous.follow);
        assert!(previous.previous);
        assert_eq!(previous.since_seconds, Some(3600));
        assert!(previous.since_time.is_none());
    }

    #[test]
    fn invalid_container_time_and_tail_requests_are_rejected() {
        for request in [
            ContainerLogRequest {
                container: String::new(),
                mode: ContainerLogMode::Current,
                since: None,
                tail_lines: None,
            },
            ContainerLogRequest {
                container: "api".to_owned(),
                mode: ContainerLogMode::Current,
                since: Some(ContainerLogSince::Seconds(0)),
                tail_lines: None,
            },
            ContainerLogRequest {
                container: "api".to_owned(),
                mode: ContainerLogMode::Current,
                since: None,
                tail_lines: Some(MAX_CONTAINER_LOG_TAIL_LINES + 1),
            },
        ] {
            assert_eq!(
                container_log_params(&request).unwrap_err().kind(),
                FactErrorKind::InvalidLogRequest
            );
        }
    }

    #[test]
    fn timestamped_and_unstamped_lines_remain_distinguishable() {
        let timestamped =
            parse_container_log_line(b"2026-10-14T12:00:00Z request accepted", false);
        assert_eq!(
            timestamped.timestamp.as_deref(),
            Some("2026-10-14T12:00:00Z")
        );
        assert_eq!(timestamped.message, "request accepted");
        assert!(!timestamped.truncated);

        let unstamped = parse_container_log_line(b"not a timestamp payload", false);
        assert_eq!(unstamped.timestamp, None);
        assert_eq!(unstamped.message, "not a timestamp payload");
    }

    #[test]
    fn oversized_log_lines_are_marked_truncated() {
        let input = vec![b'x'; MAX_CONTAINER_LOG_LINE_BYTES + 64];
        let (lines, line_count, bytes_read, possibly_truncated) = collect_log_lines(input);
        assert_eq!(line_count, 1);
        assert_eq!(bytes_read, MAX_CONTAINER_LOG_LINE_BYTES + 64);
        assert_eq!(lines[0].message.len(), MAX_CONTAINER_LOG_LINE_BYTES);
        assert!(lines[0].truncated);
        assert!(possibly_truncated);
    }

    #[test]
    fn log_stream_caps_total_bytes_and_rows() {
        let input = vec![b'x'; MAX_CONTAINER_LOG_BYTES + 1];
        let (_, _, bytes_read, possibly_truncated) = collect_log_lines(input);
        assert_eq!(bytes_read, MAX_CONTAINER_LOG_BYTES);
        assert!(possibly_truncated);

        let mut rows = Vec::new();
        for _ in 0..=MAX_CONTAINER_LOG_LINES {
            rows.extend_from_slice(b"2026-10-14T12:00:00Z row\n");
        }
        let (lines, line_count, _, possibly_truncated) = collect_log_lines(rows);
        assert_eq!(line_count, MAX_CONTAINER_LOG_LINES);
        assert_eq!(lines.len(), MAX_CONTAINER_LOG_LINES);
        assert!(possibly_truncated);
    }

    #[test]
    fn pod_uid_change_is_reported_as_recreation() {
        let pod = Pod {
            metadata: ObjectMeta {
                uid: Some("replacement-uid".to_owned()),
                ..ObjectMeta::default()
            },
            ..Pod::default()
        };
        assert_eq!(
            ensure_pod_uid(&pod, "selected-uid").unwrap_err().kind(),
            FactErrorKind::PodRecreated
        );
    }

    #[test]
    fn log_stream_limit_rejects_excess_concurrent_streams() {
        let slots = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTAINER_LOG_STREAMS));
        let permits = (0..MAX_CONCURRENT_CONTAINER_LOG_STREAMS)
            .map(|_| acquire_log_stream_slot(slots.clone()).unwrap())
            .collect::<Vec<_>>();
        let error = match acquire_log_stream_slot(slots) {
            Err(error) => error,
            Ok(_) => panic!("log stream limit allowed an excess stream"),
        };
        assert_eq!(error.kind(), FactErrorKind::LogStreamLimitReached);
        drop(permits);
    }

    struct PendingLogReader;

    impl futures::io::AsyncRead for PendingLogReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            _buffer: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Pending
        }
    }

    #[test]
    fn log_stream_wait_is_interrupted_by_session_or_request_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for cancel_session in [false, true] {
                let (sender, _receiver) = async_channel::bounded(1);
                let session_cancellation = CancellationToken::new();
                let request_cancellation = CancellationToken::new();
                let worker_session = session_cancellation.clone();
                let worker_request = request_cancellation.clone();
                let task = tokio::spawn(async move {
                    stream_container_log_lines(
                        PendingLogReader,
                        sender,
                        worker_session,
                        worker_request,
                    )
                    .await
                });
                tokio::task::yield_now().await;
                if cancel_session {
                    session_cancellation.cancel();
                } else {
                    request_cancellation.cancel();
                }
                let error = tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert_eq!(error.kind(), FactErrorKind::SessionCancelled);
            }
        });
    }
}
