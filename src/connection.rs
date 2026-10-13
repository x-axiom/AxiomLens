use crate::{
    cancellation::CancellationToken,
    exec_plugin::{self, ExecPluginFailureKind},
    fact_service::{FactError, ReadOnlyFactService, SharedFactCache},
    kubeconfig::{
        ImportedKubeconfig, KubeconfigImportError, KubeconfigReferenceKind,
    },
    target::TargetBinding,
};
use k8s_openapi::{
    api::{authorization::v1::{ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec}, core::v1::Pod},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    Api, Client,
    api::{ListParams, PostParams},
    config::{Config, ExecAuthCluster, KubeConfigOptions, Kubeconfig, KubeconfigError},
    core::gvk::{GroupVersion, GroupVersionKind},
    discovery::{self, ApiResource, Scope, verbs},
};
use jiff::Timestamp;
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    io,
    time::Duration,
};
use tokio::runtime::{Builder, Runtime};

const MAX_CONNECT_TIMEOUT_SECONDS: u64 = 60;
const MAX_REQUEST_TIMEOUT_SECONDS: u64 = 120;
const MAX_RETRY_LIMIT: u8 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(100);
const DISCOVERY_GROUPS: &[(&str, &str, &[&str])] = &[
    ("", "v1", &["Namespace", "Pod", "Service", "Node"]),
    ("apps", "v1", &["Deployment", "StatefulSet", "DaemonSet"]),
    ("batch", "v1", &["Job"]),
    ("networking.k8s.io", "v1", &["Ingress"]),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionPolicy {
    connect_timeout: Duration,
    request_timeout: Duration,
    retry_limit: u8,
}

impl ConnectionPolicy {
    pub fn from_seconds(
        connect_timeout_seconds: u16,
        request_timeout_seconds: u16,
        retry_limit: u8,
    ) -> Result<Self, ConnectionPolicyError> {
        let connect_timeout_seconds = u64::from(connect_timeout_seconds);
        let request_timeout_seconds = u64::from(request_timeout_seconds);
        if !(1..=MAX_CONNECT_TIMEOUT_SECONDS).contains(&connect_timeout_seconds)
            || !(1..=MAX_REQUEST_TIMEOUT_SECONDS).contains(&request_timeout_seconds)
        {
            return Err(ConnectionPolicyError::InvalidTimeout);
        }
        if retry_limit > MAX_RETRY_LIMIT {
            return Err(ConnectionPolicyError::RetryLimitExceeded);
        }
        Ok(Self {
            connect_timeout: Duration::from_secs(connect_timeout_seconds),
            request_timeout: Duration::from_secs(request_timeout_seconds),
            retry_limit,
        })
    }

    pub fn connect_timeout_seconds(self) -> u16 {
        self.connect_timeout.as_secs() as u16
    }

    pub fn request_timeout_seconds(self) -> u16 {
        self.request_timeout.as_secs() as u16
    }

    pub fn retry_limit(self) -> u8 {
        self.retry_limit
    }
}

impl Default for ConnectionPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(20),
            retry_limit: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPolicyError {
    InvalidTimeout,
    RetryLimitExceeded,
}

impl fmt::Display for ConnectionPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimeout => formatter.write_str("connection timeout is outside allowed bounds"),
            Self::RetryLimitExceeded => formatter.write_str("connection retry limit exceeds maximum"),
        }
    }
}

impl Error for ConnectionPolicyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiResourceScope {
    Namespaced,
    Cluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiResourceDiscoveryState {
    Served {
        scope: ApiResourceScope,
        list_supported: bool,
        access: ApiResourceAccessAssessment,
    },
    Unsupported,
    DiscoveryDenied,
    Unavailable,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiResourceAccessAssessment {
    Allowed,
    Denied,
    NotChecked,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResourceCapability {
    kind: &'static str,
    api_group: &'static str,
    api_version: &'static str,
    resource: Option<ApiResource>,
    state: ApiResourceDiscoveryState,
}

impl ApiResourceCapability {
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    pub fn api_group(&self) -> &'static str {
        self.api_group
    }

    pub fn api_version(&self) -> &'static str {
        self.api_version
    }

    pub fn state(&self) -> ApiResourceDiscoveryState {
        self.state
    }

    pub fn is_list_supported(&self) -> bool {
        matches!(
            self.state,
            ApiResourceDiscoveryState::Served {
                list_supported: true,
                ..
            }
        )
    }

    pub fn api_resource(&self) -> Option<&ApiResource> {
        self.resource.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiCapabilitySnapshot {
    resources: Vec<ApiResourceCapability>,
}

impl ApiCapabilitySnapshot {
    pub fn resources(&self) -> &[ApiResourceCapability] {
        &self.resources
    }

    pub fn resource(&self, kind: &str) -> Option<&ApiResourceCapability> {
        self.resources.iter().find(|resource| resource.kind == kind)
    }

    pub fn resource_gvk(&self, gvk: &GroupVersionKind) -> Option<&ApiResourceCapability> {
        self.resources.iter().find(|resource| {
            resource.kind == gvk.kind
                && resource.api_group == gvk.group
                && resource.api_version == gvk.version
        })
    }

    pub fn served_count(&self) -> usize {
        self.resources
            .iter()
            .filter(|resource| matches!(resource.state, ApiResourceDiscoveryState::Served { .. }))
            .count()
    }

    pub fn unsupported_count(&self) -> usize {
        self.resources
            .iter()
            .filter(|resource| resource.state == ApiResourceDiscoveryState::Unsupported)
            .count()
    }

    pub fn denied_count(&self) -> usize {
        self.resources
            .iter()
            .filter(|resource| match resource.state {
                ApiResourceDiscoveryState::DiscoveryDenied => true,
                ApiResourceDiscoveryState::Served {
                    access: ApiResourceAccessAssessment::Denied,
                    ..
                } => true,
                _ => false,
            })
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionFailureKind {
    InvalidConfiguration,
    ConfigurationChanged,
    CertificateProblem,
    CredentialsUnavailable,
    CredentialsRejected,
    PermissionDenied,
    NamespaceNotFound,
    ApiUnavailable,
    TimedOut,
    ServerUnavailable,
    ExecPluginRequiresTrust,
    ExecPluginFailed,
    ExecPluginTimedOut,
    ExecPluginInvalidOutput,
    ExecPluginInteractiveUnsupported,
    Cancelled,
    UnsupportedAuthProvider,
    TlsVerificationDisabled,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionFailure {
    kind: ConnectionFailureKind,
}

impl ConnectionFailure {
    fn new(kind: ConnectionFailureKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> ConnectionFailureKind {
        self.kind
    }

    pub fn error_code(self) -> &'static str {
        match self.kind {
            ConnectionFailureKind::InvalidConfiguration => "invalid_configuration",
            ConnectionFailureKind::ConfigurationChanged => "configuration_changed",
            ConnectionFailureKind::CertificateProblem => "certificate_problem",
            ConnectionFailureKind::CredentialsUnavailable => "credentials_unavailable",
            ConnectionFailureKind::CredentialsRejected => "credentials_rejected",
            ConnectionFailureKind::PermissionDenied => "permission_denied",
            ConnectionFailureKind::NamespaceNotFound => "namespace_not_found",
            ConnectionFailureKind::ApiUnavailable => "api_unavailable",
            ConnectionFailureKind::TimedOut => "request_timeout",
            ConnectionFailureKind::ServerUnavailable => "server_unavailable",
            ConnectionFailureKind::ExecPluginRequiresTrust => "exec_plugin_requires_trust",
            ConnectionFailureKind::ExecPluginFailed => "exec_plugin_failed",
            ConnectionFailureKind::ExecPluginTimedOut => "exec_plugin_timeout",
            ConnectionFailureKind::ExecPluginInvalidOutput => "exec_plugin_invalid_output",
            ConnectionFailureKind::ExecPluginInteractiveUnsupported => {
                "exec_plugin_interactive_unsupported"
            }
            ConnectionFailureKind::Cancelled => "connection_cancelled",
            ConnectionFailureKind::UnsupportedAuthProvider => "unsupported_auth_provider",
            ConnectionFailureKind::TlsVerificationDisabled => "tls_verification_disabled",
            ConnectionFailureKind::Internal => "connection_internal",
        }
    }

    pub fn summary(self) -> &'static str {
        match self.kind {
            ConnectionFailureKind::InvalidConfiguration => "Kubeconfig could not be used.",
            ConnectionFailureKind::ConfigurationChanged => {
                "Kubeconfig target changed after import."
            }
            ConnectionFailureKind::CertificateProblem => {
                "Certificate or private-key validation failed."
            }
            ConnectionFailureKind::CredentialsUnavailable => {
                "Credential reference could not be read."
            }
            ConnectionFailureKind::CredentialsRejected => {
                "Credentials were rejected or have expired."
            }
            ConnectionFailureKind::PermissionDenied => {
                "API server denied read access to the selected Namespace."
            }
            ConnectionFailureKind::NamespaceNotFound => {
                "Selected Namespace was not found."
            }
            ConnectionFailureKind::ApiUnavailable => "Kubernetes API server is unreachable.",
            ConnectionFailureKind::TimedOut => "Kubernetes API request timed out.",
            ConnectionFailureKind::ServerUnavailable => {
                "Kubernetes API server returned a temporary error."
            }
            ConnectionFailureKind::ExecPluginRequiresTrust => {
                "Context uses an exec authentication plugin that requires explicit trust."
            }
            ConnectionFailureKind::ExecPluginFailed => {
                "Authentication plugin could not start or exited unsuccessfully."
            }
            ConnectionFailureKind::ExecPluginTimedOut => {
                "Authentication plugin exceeded its time limit."
            }
            ConnectionFailureKind::ExecPluginInvalidOutput => {
                "Authentication plugin returned invalid or oversized credentials."
            }
            ConnectionFailureKind::ExecPluginInteractiveUnsupported => {
                "Authentication plugin requires interactive input not available in this flow."
            }
            ConnectionFailureKind::Cancelled => "Connection attempt was cancelled.",
            ConnectionFailureKind::UnsupportedAuthProvider => {
                "Context uses an authentication provider not supported by this connection flow."
            }
            ConnectionFailureKind::TlsVerificationDisabled => {
                "Connection refused because TLS certificate verification is disabled."
            }
            ConnectionFailureKind::Internal => "Connection check failed unexpectedly.",
        }
    }

    pub fn next_step(self) -> &'static str {
        match self.kind {
            ConnectionFailureKind::InvalidConfiguration => {
                "Check the selected Context, cluster reference, and credential-file paths."
            }
            ConnectionFailureKind::ConfigurationChanged => {
                "Import kubeconfig again and select the intended Context."
            }
            ConnectionFailureKind::CertificateProblem => {
                "Check certificate/key files, CA, certificate validity, and system clock. Keep TLS verification enabled."
            }
            ConnectionFailureKind::CredentialsUnavailable => {
                "Restore the referenced token file or credential entry, then retry."
            }
            ConnectionFailureKind::CredentialsRejected => {
                "Refresh credentials through their configured provider, then retry."
            }
            ConnectionFailureKind::PermissionDenied => {
                "Ask the cluster owner for the minimum required read permission in this Namespace; do not request admin access."
            }
            ConnectionFailureKind::NamespaceNotFound => {
                "Select a Namespace that exists in the chosen cluster."
            }
            ConnectionFailureKind::ApiUnavailable => {
                "Check the API endpoint, VPN, network route, and cluster availability."
            }
            ConnectionFailureKind::TimedOut => {
                "Check network latency or increase the configured request timeout."
            }
            ConnectionFailureKind::ServerUnavailable => {
                "Retry after checking API server health; retries are bounded by connection settings."
            }
            ConnectionFailureKind::ExecPluginRequiresTrust => {
                "Review and trust the authentication command in the dedicated plugin-trust flow."
            }
            ConnectionFailureKind::ExecPluginFailed => {
                "Check executable permissions, plugin installation, arguments, and exit status."
            }
            ConnectionFailureKind::ExecPluginTimedOut => {
                "Check plugin availability and authentication provider; execution was stopped at the configured limit."
            }
            ConnectionFailureKind::ExecPluginInvalidOutput => {
                "Verify plugin API version and ExecCredential token or certificate output."
            }
            ConnectionFailureKind::ExecPluginInteractiveUnsupported => {
                "Use a non-interactive credential flow or a plugin configured for IfAvailable/Never."
            }
            ConnectionFailureKind::UnsupportedAuthProvider => {
                "Use a supported token or client-certificate Context."
            }
            ConnectionFailureKind::TlsVerificationDisabled => {
                "Configure a valid CA or system-trusted certificate; do not disable verification."
            }
            ConnectionFailureKind::Internal => {
                "Retry once; if failure continues, report the error code without sharing credentials."
            }
            ConnectionFailureKind::Cancelled => {
                "Selected target changed; results from the cancelled session were discarded."
            }
        }
    }

    fn is_retryable(self) -> bool {
        matches!(
            self.kind,
            ConnectionFailureKind::TimedOut
                | ConnectionFailureKind::ApiUnavailable
                | ConnectionFailureKind::ServerUnavailable
        )
    }
}

impl fmt::Display for ConnectionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.summary())
    }
}

impl Error for ConnectionFailure {}

pub struct ActiveConnection {
    target: TargetBinding,
    client: Client,
    _runtime: Option<Runtime>,
    cancellation: CancellationToken,
    request_timeout: Duration,
    attempts: u8,
}

impl ActiveConnection {
    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn attempts(&self) -> u8 {
        self.attempts
    }

    pub fn read_only_facts(
        &self,
        capabilities: ApiCapabilitySnapshot,
        cache: SharedFactCache,
    ) -> Result<ReadOnlyFactService, FactError> {
        let runtime = self
            ._runtime
            .as_ref()
            .ok_or_else(|| FactError::new(crate::fact_service::FactErrorKind::Internal))?;
        Ok(ReadOnlyFactService::new(
            self.client.clone(),
            self.target.clone(),
            capabilities,
            self.cancellation.clone(),
            runtime.handle().clone(),
            cache,
            self.request_timeout,
        ))
    }

    pub async fn discover_capabilities(
        &self,
        cancellation: CancellationToken,
        timeout: Duration,
    ) -> Result<ApiCapabilitySnapshot, ConnectionFailure> {
        let namespace = self
            .target
            .namespace_scope()
            .single_namespace()
            .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?
            .as_str()
            .to_owned();
        let Some(runtime) = &self._runtime else {
            return Err(ConnectionFailure::new(ConnectionFailureKind::Internal));
        };
        runtime
            .spawn(discover_api_capabilities(
                self.client.clone(),
                namespace,
                cancellation,
                timeout,
            ))
            .await
            .map_err(|_| ConnectionFailure::new(ConnectionFailureKind::Internal))?
    }
}

impl fmt::Debug for ActiveConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveConnection")
            .field("context", &self.target.context_name().as_str())
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        if let Some(runtime) = self._runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// Connect to the explicitly selected target using a read-only Pod list probe.
/// The owned Tokio runtime keeps kube's Hyper executor available for later reads.
pub async fn connect(
    imported: ImportedKubeconfig,
    target: TargetBinding,
    policy: ConnectionPolicy,
    trusted_exec_plugins: BTreeSet<String>,
    cancellation: CancellationToken,
) -> Result<ActiveConnection, ConnectionFailure> {
    if cancellation.is_cancelled() {
        return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
    }
    let context_name = target.context_name().as_str().to_owned();
    let context = imported
        .contexts()
        .iter()
        .find(|context| context.name().as_str() == context_name)
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    if context.connection_identity().fingerprint() != target.connection().fingerprint() {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::ConfigurationChanged,
        ));
    }
    let namespace = target
        .namespace_scope()
        .single_namespace()
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?
        .as_str()
        .to_owned();
    let exec_trust_fingerprint = context
        .exec_plugin()
        .map(|plugin| plugin.trust_fingerprint().to_owned());
    let mut kubeconfig = imported
        .load_raw_for_connection(&context_name)
        .map_err(map_import_error)?;
    ensure_auth_requires_no_untrusted_plugin(
        &kubeconfig,
        &context_name,
        exec_trust_fingerprint.as_deref(),
        &trusted_exec_plugins,
    )?;

    let (sender, receiver) = tokio::sync::oneshot::channel();
    let worker_cancellation = cancellation.clone();
    std::thread::Builder::new()
        .name("axiomlens-kubernetes-connect".to_owned())
        .spawn(move || {
            let result = if worker_cancellation.is_cancelled() {
                Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled))
            } else {
                prepare_exec_credentials(
                    &mut kubeconfig,
                    &context_name,
                    exec_trust_fingerprint.as_deref(),
                    &trusted_exec_plugins,
                    policy.request_timeout,
                    &worker_cancellation,
                )
            };
            let outcome = result.and_then(|valid_until| Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|_| ConnectionFailure::new(ConnectionFailureKind::Internal))
                .and_then(|runtime| {
                            let active_cancellation = worker_cancellation.clone();
                            let request_timeout = policy.request_timeout;
                    let result = runtime.block_on(connect_and_probe(
                        kubeconfig,
                        context_name,
                        namespace,
                        policy,
                        valid_until,
                        worker_cancellation,
                    ));
                    result.map(|(client, attempts)| ActiveConnection {
                        target,
                        client,
                        _runtime: Some(runtime),
                        cancellation: active_cancellation,
                        request_timeout,
                        attempts,
                    })
                }));
            let _ = sender.send(outcome);
        })
        .map_err(|_| ConnectionFailure::new(ConnectionFailureKind::Internal))?;
    receiver
        .await
        .map_err(|_| ConnectionFailure::new(ConnectionFailureKind::Internal))?
}

async fn connect_and_probe(
    kubeconfig: Kubeconfig,
    context_name: String,
    namespace: String,
    policy: ConnectionPolicy,
    valid_until: Option<Timestamp>,
    cancellation: CancellationToken,
) -> Result<(Client, u8), ConnectionFailure> {
    let options = KubeConfigOptions {
        context: Some(context_name),
        ..KubeConfigOptions::default()
    };
    let mut config = Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .map_err(map_config_error)?;
    config.default_namespace = namespace.clone();
    if config.accept_invalid_certs {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::TlsVerificationDisabled,
        ));
    }
    config.connect_timeout = Some(policy.connect_timeout);
    config.read_timeout = Some(policy.request_timeout);
    config.write_timeout = Some(policy.request_timeout);
    config.default_retry = false;

    let client = Client::try_from(config)
        .map_err(|error| classify_kube_error(&error))?
        .with_valid_until(valid_until);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let list_params = ListParams::default().limit(1);
    for attempt_index in 0..=policy.retry_limit {
        if cancellation.is_cancelled() {
            return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
        }
        let attempt = attempt_index + 1;
        let probe = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
            }
            probe = tokio::time::timeout(
                policy.request_timeout,
                pods.list(&list_params),
            ) => probe,
        };
        let failure = match probe {
            Ok(Ok(_)) => return Ok((client, attempt)),
            Ok(Err(error)) => classify_kube_error(&error),
            Err(_) => ConnectionFailure::new(ConnectionFailureKind::TimedOut),
        };
        if !failure.is_retryable() || attempt_index == policy.retry_limit {
            return Err(failure);
        }
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
            }
            _ = tokio::time::sleep(RETRY_BACKOFF * u32::from(attempt)) => {}
        }
    }
    Err(ConnectionFailure::new(ConnectionFailureKind::Internal))
}

async fn discover_api_capabilities(
    client: Client,
    namespace: String,
    cancellation: CancellationToken,
    timeout: Duration,
) -> Result<ApiCapabilitySnapshot, ConnectionFailure> {
    let mut resources = Vec::new();
    for &(group, version, kinds) in DISCOVERY_GROUPS {
        if cancellation.is_cancelled() {
            return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
        }
        let group_version = GroupVersion::gv(group, version);
        let result = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
            }
            result = tokio::time::timeout(
                timeout,
                discovery::pinned_group(&client, &group_version),
            ) => result,
        };

        match result {
            Ok(Ok(api_group)) => {
                let discovered = api_group.versioned_resources(version);
                for kind in kinds {
                    let found = discovered
                        .iter()
                        .find(|(resource, _)| resource.kind == *kind);
                    let resource = found.map(|(resource, _)| resource.clone());
                    let state = found
                        .map(|(_, capabilities)| ApiResourceDiscoveryState::Served {
                            scope: match capabilities.scope {
                                Scope::Namespaced => ApiResourceScope::Namespaced,
                                Scope::Cluster => ApiResourceScope::Cluster,
                            },
                            list_supported: capabilities.supports_operation(verbs::LIST),
                            access: ApiResourceAccessAssessment::NotChecked,
                        })
                        .unwrap_or(ApiResourceDiscoveryState::Unsupported);
                    resources.push(ApiResourceCapability {
                        kind,
                        api_group: group,
                        api_version: version,
                        resource,
                        state,
                    });
                }
            }
            Ok(Err(kube::Error::Api(status))) if status.code == 403 => {
                resources.extend(kinds.iter().map(|kind| ApiResourceCapability {
                    kind,
                    api_group: group,
                    api_version: version,
                    resource: None,
                    state: ApiResourceDiscoveryState::DiscoveryDenied,
                }));
            }
            Ok(Err(kube::Error::Api(status))) if status.code == 404 => {
                resources.extend(kinds.iter().map(|kind| ApiResourceCapability {
                    kind,
                    api_group: group,
                    api_version: version,
                    resource: None,
                    state: ApiResourceDiscoveryState::Unsupported,
                }));
            }
            Ok(Err(error)) => {
                let failure = classify_kube_error(&error);
                let state = match failure.kind() {
                    ConnectionFailureKind::PermissionDenied => {
                        ApiResourceDiscoveryState::DiscoveryDenied
                    }
                    ConnectionFailureKind::TimedOut => ApiResourceDiscoveryState::TimedOut,
                    _ => ApiResourceDiscoveryState::Unavailable,
                };
                resources.extend(kinds.iter().map(|kind| ApiResourceCapability {
                    kind,
                    api_group: group,
                    api_version: version,
                    resource: None,
                    state,
                }));
            }
            Err(_) => {
                resources.extend(kinds.iter().map(|kind| ApiResourceCapability {
                    kind,
                    api_group: group,
                    api_version: version,
                    resource: None,
                    state: ApiResourceDiscoveryState::TimedOut,
                }));
            }
        }
    }
    let reviews: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let review_params = PostParams::default();
    for capability in &mut resources {
        let ApiResourceDiscoveryState::Served {
            scope,
            list_supported: true,
            access: ApiResourceAccessAssessment::NotChecked,
        } = capability.state
        else {
            continue;
        };
        let Some(resource) = capability.resource.as_ref() else {
            capability.state = ApiResourceDiscoveryState::Served {
                scope,
                list_supported: true,
                access: ApiResourceAccessAssessment::Unavailable,
            };
            continue;
        };
        let review = SelfSubjectAccessReview {
            metadata: ObjectMeta::default(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    group: Some(resource.group.clone()),
                    version: Some(resource.version.clone()),
                    resource: Some(resource.plural.clone()),
                    namespace: match scope {
                        ApiResourceScope::Namespaced => Some(namespace.clone()),
                        ApiResourceScope::Cluster => None,
                    },
                    verb: Some("list".to_owned()),
                    ..ResourceAttributes::default()
                }),
                ..SelfSubjectAccessReviewSpec::default()
            },
            ..SelfSubjectAccessReview::default()
        };
        let result = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
            }
            result = tokio::time::timeout(timeout, reviews.create(&review_params, &review)) => result,
        };
        let access = match result {
            Ok(Ok(review)) => assess_access_review(review.status.as_ref()),
            Ok(Err(_)) | Err(_) => ApiResourceAccessAssessment::Unavailable,
        };
        capability.state = ApiResourceDiscoveryState::Served {
            scope,
            list_supported: true,
            access,
        };
    }
    Ok(ApiCapabilitySnapshot { resources })
}

fn assess_access_review(
    status: Option<&k8s_openapi::api::authorization::v1::SubjectAccessReviewStatus>,
) -> ApiResourceAccessAssessment {
    match status {
        Some(status) if status.allowed => ApiResourceAccessAssessment::Allowed,
        Some(status) if status.denied == Some(true) => ApiResourceAccessAssessment::Denied,
        Some(_) | None => ApiResourceAccessAssessment::Unavailable,
    }
}

fn ensure_auth_requires_no_untrusted_plugin(
    kubeconfig: &Kubeconfig,
    context_name: &str,
    exec_trust_fingerprint: Option<&str>,
    trusted_exec_plugins: &BTreeSet<String>,
) -> Result<(), ConnectionFailure> {
    let context = kubeconfig
        .contexts
        .iter()
        .find(|context| context.name == context_name)
        .and_then(|context| context.context.as_ref())
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    let Some(user_name) = context.user.as_deref() else {
        return Ok(());
    };
    let auth_info = kubeconfig
        .auth_infos
        .iter()
        .find(|user| user.name == user_name)
        .and_then(|user| user.auth_info.as_ref())
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    if auth_info.exec.is_some() {
        let Some(fingerprint) = exec_trust_fingerprint else {
            return Err(ConnectionFailure::new(
                ConnectionFailureKind::ExecPluginRequiresTrust,
            ));
        };
        if !trusted_exec_plugins.contains(fingerprint) {
            return Err(ConnectionFailure::new(
                ConnectionFailureKind::ExecPluginRequiresTrust,
            ));
        }
    } else if exec_trust_fingerprint.is_some() {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::ConfigurationChanged,
        ));
    }
    if auth_info.auth_provider.is_some() {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::UnsupportedAuthProvider,
        ));
    }
    Ok(())
}

fn prepare_exec_credentials(
    kubeconfig: &mut Kubeconfig,
    context_name: &str,
    trust_fingerprint: Option<&str>,
    trusted_exec_plugins: &std::collections::BTreeSet<String>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<Option<Timestamp>, ConnectionFailure> {
    if cancellation.is_cancelled() {
        return Err(ConnectionFailure::new(ConnectionFailureKind::Cancelled));
    }
    let context = kubeconfig
        .contexts
        .iter()
        .find(|context| context.name == context_name)
        .and_then(|context| context.context.as_ref())
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    let cluster_name = context.cluster.clone();
    let user_name = context
        .user
        .as_deref()
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    let auth_info = kubeconfig
        .auth_infos
        .iter_mut()
        .find(|user| user.name == user_name)
        .and_then(|user| user.auth_info.as_mut())
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    let Some(exec) = auth_info.exec.clone() else {
        return Ok(None);
    };
    let fingerprint = trust_fingerprint.ok_or_else(|| {
        ConnectionFailure::new(ConnectionFailureKind::ExecPluginRequiresTrust)
    })?;
    if !trusted_exec_plugins.contains(fingerprint) {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::ExecPluginRequiresTrust,
        ));
    }
    if auth_info.username.is_some()
        || auth_info.password.is_some()
        || auth_info.token.is_some()
        || auth_info.token_file.is_some()
        || auth_info.client_certificate.is_some()
        || auth_info.client_certificate_data.is_some()
        || auth_info.client_key.is_some()
        || auth_info.client_key_data.is_some()
        || auth_info.auth_provider.is_some()
    {
        return Err(ConnectionFailure::new(
            ConnectionFailureKind::InvalidConfiguration,
        ));
    }
    let api_version = exec
        .api_version
        .as_deref()
        .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
    let cluster_info = if exec.provide_cluster_info {
        let cluster = kubeconfig
            .clusters
            .iter()
            .find(|cluster| cluster.name == cluster_name)
            .and_then(|cluster| cluster.cluster.as_ref())
            .ok_or_else(|| ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration))?;
        Some(
            ExecAuthCluster::try_from(cluster)
                .map_err(map_config_error)?,
        )
    } else {
        None
    };
    let credentials = exec_plugin::execute_cancellable(
        &exec,
        api_version,
        cluster_info.as_ref(),
        timeout,
        cancellation,
    )
    .map_err(map_exec_plugin_error)?;
    let expiration = credentials.expiration();
    credentials.install(auth_info);
    Ok(expiration)
}

fn map_exec_plugin_error(error: exec_plugin::ExecPluginFailure) -> ConnectionFailure {
    let kind = match error.kind() {
        ExecPluginFailureKind::TimedOut => ConnectionFailureKind::ExecPluginTimedOut,
        ExecPluginFailureKind::InteractiveModeUnsupported => {
            ConnectionFailureKind::ExecPluginInteractiveUnsupported
        }
        ExecPluginFailureKind::MissingCommand
        | ExecPluginFailureKind::StartFailed
        | ExecPluginFailureKind::NonZeroExit => ConnectionFailureKind::ExecPluginFailed,
        ExecPluginFailureKind::OutputTooLarge
        | ExecPluginFailureKind::OutputReadFailed
        | ExecPluginFailureKind::InvalidOutput
        | ExecPluginFailureKind::ApiVersionMismatch
        | ExecPluginFailureKind::MissingCredentials
        | ExecPluginFailureKind::ConflictingCredentials
        | ExecPluginFailureKind::InvalidEnvironment => ConnectionFailureKind::ExecPluginInvalidOutput,
        ExecPluginFailureKind::ExpiredCredentials => ConnectionFailureKind::CredentialsRejected,
        ExecPluginFailureKind::Cancelled => ConnectionFailureKind::Cancelled,
    };
    ConnectionFailure::new(kind)
}

fn map_import_error(error: KubeconfigImportError) -> ConnectionFailure {
    use KubeconfigReferenceKind as Reference;
    let kind = match error {
        KubeconfigImportError::ConfigChangedSinceImport => {
            ConnectionFailureKind::ConfigurationChanged
        }
        KubeconfigImportError::MissingReferencedFile(Reference::TokenFile)
        | KubeconfigImportError::UnreadableReferencedFile(Reference::TokenFile) => {
            ConnectionFailureKind::CredentialsUnavailable
        }
        KubeconfigImportError::MissingReferencedFile(Reference::CertificateAuthority)
        | KubeconfigImportError::UnreadableReferencedFile(Reference::CertificateAuthority)
        | KubeconfigImportError::InvalidReferencedFile(Reference::CertificateAuthority)
        | KubeconfigImportError::ReferencedFileTooLarge(Reference::CertificateAuthority)
        | KubeconfigImportError::InvalidCertificateAuthorityData
        | KubeconfigImportError::IncompleteClientCertificatePair
        | KubeconfigImportError::MissingReferencedFile(Reference::ClientCertificate)
        | KubeconfigImportError::UnreadableReferencedFile(Reference::ClientCertificate)
        | KubeconfigImportError::InvalidReferencedFile(Reference::ClientCertificate)
        | KubeconfigImportError::MissingReferencedFile(Reference::ClientKey)
        | KubeconfigImportError::UnreadableReferencedFile(Reference::ClientKey)
        | KubeconfigImportError::InvalidReferencedFile(Reference::ClientKey) => {
            ConnectionFailureKind::CertificateProblem
        }
        _ => ConnectionFailureKind::InvalidConfiguration,
    };
    ConnectionFailure::new(kind)
}

fn map_config_error(error: KubeconfigError) -> ConnectionFailure {
    use kube::config::KubeconfigError as ImportError;
    match error {
        ImportError::LoadCertificateAuthority(_)
        | ImportError::LoadClientCertificate(_)
        | ImportError::LoadClientKey(_)
        | ImportError::ParseCertificates(_) => {
            ConnectionFailure::new(ConnectionFailureKind::CertificateProblem)
        }
        _ => ConnectionFailure::new(ConnectionFailureKind::InvalidConfiguration),
    }
}

fn classify_kube_error(error: &kube::Error) -> ConnectionFailure {
    let kind = match error {
        kube::Error::Api(status) => match status.code {
            401 => ConnectionFailureKind::CredentialsRejected,
            403 => ConnectionFailureKind::PermissionDenied,
            404 => ConnectionFailureKind::NamespaceNotFound,
            429 | 500..=599 => ConnectionFailureKind::ServerUnavailable,
            _ => ConnectionFailureKind::InvalidConfiguration,
        },
        kube::Error::RustlsTls(_) => ConnectionFailureKind::CertificateProblem,
        kube::Error::Auth(_) => ConnectionFailureKind::CredentialsRejected,
        kube::Error::HyperError(error) => classify_error_chain(error),
        kube::Error::Service(error) => classify_error_chain(error.as_ref()),
        kube::Error::InferKubeconfig(_) | kube::Error::InferConfig(_) => {
            ConnectionFailureKind::InvalidConfiguration
        }
        _ => ConnectionFailureKind::Internal,
    };
    ConnectionFailure::new(kind)
}

fn classify_error_chain(error: &(dyn Error + 'static)) -> ConnectionFailureKind {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(io_error) = error.downcast_ref::<io::Error>() {
            match io_error.kind() {
                io::ErrorKind::TimedOut => return ConnectionFailureKind::TimedOut,
                io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::NotConnected
                | io::ErrorKind::AddrNotAvailable
                | io::ErrorKind::NetworkUnreachable
                | io::ErrorKind::HostUnreachable => return ConnectionFailureKind::ApiUnavailable,
                _ => {}
            }
        }
        let message = error.to_string().to_ascii_lowercase();
        if message.contains("certificate")
            || message.contains("unknownissuer")
            || message.contains("tls")
        {
            return ConnectionFailureKind::CertificateProblem;
        }
        if message.contains("timed out") || message.contains("timeout") {
            return ConnectionFailureKind::TimedOut;
        }
        if message.contains("connection refused")
            || message.contains("network is unreachable")
            || message.contains("failed to lookup address")
            || message.contains("dns error")
        {
            return ConnectionFailureKind::ApiUnavailable;
        }
        current = error.source();
    }
    ConnectionFailureKind::ApiUnavailable
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fact_service::{
        FactCacheConfig, FactCompleteness, FactErrorKind, FactFreshness, FactSource,
        FactStreamEvent, FactStreamReceiveError, SharedFactCache,
    };
    use std::{
        fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        sync::{Arc, Mutex},
        thread,
    };
    use tempfile::TempDir;

    const TEST_TOKEN: &str = "CONNECTION_TEST_TOKEN_NEVER_LOG";
    const POD_LIST_WITH_ITEM: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"list-rv-4"},"items":[{"apiVersion":"v1","kind":"Pod","metadata":{"name":"api-0","namespace":"payments","uid":"uid-pod-123","resourceVersion":"rv-23"},"spec":{}},{"apiVersion":"v1","kind":"Pod","metadata":{"name":"worker-1","namespace":"payments","uid":"uid-pod-124","resourceVersion":"rv-24"},"spec":{}}]}"#;
    const POD_DETAIL: &str = r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"api-0","namespace":"payments","uid":"uid-pod-123","resourceVersion":"rv-23"},"spec":{}}"#;

    struct MockApi {
        address: String,
        requests: Arc<Mutex<Vec<String>>>,
        worker: thread::JoinHandle<()>,
    }

    impl MockApi {
        fn start(statuses: &[u16]) -> Self {
            Self::start_with_delay(statuses, Duration::ZERO)
        }

        fn start_with_delay(statuses: &[u16], response_delay: Duration) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured = requests.clone();
            let statuses = statuses.to_vec();
            let worker = thread::spawn(move || {
                for status in statuses {
                    let Ok((mut stream, _)) = listener.accept() else {
                        return;
                    };
                    let request = read_request(&mut stream);
                    captured.lock().unwrap().push(request.clone());
                    if !response_delay.is_zero() {
                        thread::sleep(response_delay);
                    }
                    let body = match status {
                        200 => mock_success_body(&request),
                        401 => r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Unauthorized","message":"expired token detail","code":401}"#,
                        403 => r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","message":"permission denied","code":403}"#,
                        404 => r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","message":"api version unavailable","code":404}"#,
                        _ => r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"ServiceUnavailable","message":"temporary failure","code":503}"#,
                    };
                    let response = format!(
                        "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
            });
            Self {
                address,
                requests,
                worker,
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }

        fn join(self) {
            self.worker.join().unwrap();
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let Ok(read) = stream.read(&mut buffer) else {
                break;
            };
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn mock_success_body(request: &str) -> &'static str {
        const CORE: &str = r#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"namespaces","singularName":"","namespaced":false,"kind":"Namespace","verbs":["get","list"]},{"name":"pods","singularName":"","namespaced":true,"kind":"Pod","verbs":["get","list"]},{"name":"services","singularName":"","namespaced":true,"kind":"Service","verbs":["get","list"]},{"name":"nodes","singularName":"","namespaced":false,"kind":"Node","verbs":["get","list"]}]}"#;
        const NETWORKING: &str = r#"{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"networking.k8s.io/v1","resources":[{"name":"ingresses","singularName":"","namespaced":true,"kind":"Ingress","verbs":["get","list"]}]}"#;
        const ACCESS_REVIEW: &str = r#"{"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectAccessReview","metadata":{},"spec":{},"status":{"allowed":true,"denied":false}}"#;
        const PODS: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"1"},"items":[]}"#;
        let request_target = request.split_whitespace().nth(1).unwrap_or_default();
        let path_start = request_target.find("/api").unwrap_or(0);
        let path = request_target[path_start..]
            .split('?')
            .next()
            .unwrap_or_default();
        match path {
            "/api/v1" => CORE,
            "/apis/networking.k8s.io/v1" => NETWORKING,
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => ACCESS_REVIEW,
            "/api/v1/namespaces/payments/pods" => POD_LIST_WITH_ITEM,
            "/api/v1/namespaces/payments/pods/api-0" => POD_DETAIL,
            _ => PODS,
        }
    }

    fn config_file(directory: &TempDir, api_server: &str, auth: &str) -> PathBuf {
        let path = directory.path().join("config");
        let yaml = format!(
            "apiVersion: v1\nkind: Config\ncurrent-context: local\n\
             clusters:\n- name: local\n  cluster:\n    server: {api_server}\n\
             contexts:\n- name: local\n  context:\n    cluster: local\n    user: local-user\n    namespace: payments\n\
             users:\n- name: local-user\n  user:\n{auth}"
        );
        fs::write(&path, yaml).unwrap();
        path
    }

    fn imported_target(path: &PathBuf) -> (ImportedKubeconfig, TargetBinding) {
        let imported = ImportedKubeconfig::import_path(path).unwrap();
        let mut sessions = crate::target::SessionRegistry::default();
        let target = imported.select_context("local", &mut sessions).unwrap();
        (imported, target)
    }

    fn imported_target_in_namespace(
        path: &PathBuf,
        namespace: &str,
    ) -> (ImportedKubeconfig, TargetBinding) {
        let imported = ImportedKubeconfig::import_path(path).unwrap();
        let mut sessions = crate::target::SessionRegistry::default();
        let target = imported
            .select_context_in_namespace("local", namespace, &mut sessions)
            .unwrap();
        (imported, target)
    }

    fn policy(retry_limit: u8) -> ConnectionPolicy {
        ConnectionPolicy::from_seconds(2, 2, retry_limit).unwrap()
    }

    fn run_connect(
        imported: ImportedKubeconfig,
        target: TargetBinding,
        policy: ConnectionPolicy,
    ) -> Result<ActiveConnection, ConnectionFailure> {
        run_connect_with_trust(imported, target, policy, BTreeSet::new())
    }

    fn run_connect_with_trust(
        imported: ImportedKubeconfig,
        target: TargetBinding,
        policy: ConnectionPolicy,
        trusted_exec_plugins: BTreeSet<String>,
    ) -> Result<ActiveConnection, ConnectionFailure> {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(connect(
            imported,
            target,
            policy,
            trusted_exec_plugins,
            CancellationToken::new(),
        ))
    }

    #[test]
    fn valid_bearer_token_connects_with_read_only_namespace_probe() {
        let mock = MockApi::start(&[200]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);

        let connected = run_connect(imported, target.clone(), policy(0)).unwrap();
        assert_eq!(connected.target(), &target);
        assert_eq!(connected.attempts(), 1);
        let requests = mock.requests();
        mock.join();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("/api/v1/namespaces/payments/pods"), "{requests:?}");
        assert!(requests[0].to_ascii_lowercase().contains(&format!(
            "authorization: bearer {TEST_TOKEN}"
        ).to_ascii_lowercase()));
    }

    #[test]
    fn explicit_namespace_override_constrains_probe_request() {
        let mock = MockApi::start(&[200]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target_in_namespace(&path, "team-a");

        let connected = run_connect(imported, target.clone(), policy(0)).unwrap();
        assert_eq!(
            connected
                .target()
                .namespace_scope()
                .single_namespace()
                .unwrap()
                .as_str(),
            "team-a"
        );
        let requests = mock.requests();
        mock.join();
        assert!(requests[0].contains("/api/v1/namespaces/team-a/pods"));
    }

    #[test]
    fn discovery_preserves_supported_unsupported_and_forbidden_group_states() {
        let mock = MockApi::start(&[200, 200, 403, 404, 200, 200, 200, 200, 200, 200]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let connection = run_connect(imported, target, policy(0)).unwrap();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let capabilities = runtime
            .block_on(connection.discover_capabilities(
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .unwrap();

        let node = capabilities.resource("Node").unwrap();
        assert_eq!(
            node.state(),
            ApiResourceDiscoveryState::Served {
                scope: ApiResourceScope::Cluster,
                list_supported: true,
                access: ApiResourceAccessAssessment::Allowed,
            }
        );
        assert_eq!(
            capabilities.resource("Deployment").unwrap().state(),
            ApiResourceDiscoveryState::DiscoveryDenied
        );
        assert_eq!(
            capabilities.resource("Job").unwrap().state(),
            ApiResourceDiscoveryState::Unsupported
        );
        assert_eq!(
            capabilities.resource("Ingress").unwrap().state(),
            ApiResourceDiscoveryState::Served {
                scope: ApiResourceScope::Namespaced,
                list_supported: true,
                access: ApiResourceAccessAssessment::Allowed,
            }
        );
        assert_eq!(capabilities.served_count(), 5);
        assert_eq!(capabilities.denied_count(), 3);
        assert_eq!(capabilities.unsupported_count(), 1);

        let requests = mock.requests();
        mock.join();
        assert_eq!(requests.len(), 10);
        assert!(requests[0].contains("/api/v1/namespaces/payments/pods"));
        assert!(requests[1].contains("/api/v1"));
        assert!(requests[2].contains("/apis/apps/v1"));
        assert!(requests[3].contains("/apis/batch/v1"));
        assert!(requests[4].contains("/apis/networking.k8s.io/v1"));
        assert!(requests[5..]
            .iter()
            .all(|request| request.contains("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")));
    }

    #[test]
    fn facts_service_lists_gets_and_caches_target_bound_resources() {
        let mock = MockApi::start(&[200; 12]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let connection = run_connect(imported, target.clone(), policy(0)).unwrap();
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let capabilities = caller_runtime
            .block_on(connection.discover_capabilities(
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .unwrap();
        let cache = SharedFactCache::new(
            FactCacheConfig::new(1, 1024 * 1024, 512 * 1024, Duration::from_millis(30))
                .unwrap(),
        );
        let service = connection
            .read_only_facts(capabilities, cache.clone())
            .unwrap();
        let stream = service
            .list_stream(GroupVersionKind::gvk("", "v1", "Pod"), None, 10)
            .unwrap();
        let (facts, list_version, truncated) = caller_runtime.block_on(async {
            let mut facts = Vec::new();
            loop {
                match stream.recv().await.unwrap() {
                    FactStreamEvent::Batch(batch) => facts.extend(batch),
                    FactStreamEvent::Complete {
                        collection_resource_version,
                        possibly_truncated,
                        ..
                    } => break (facts, collection_resource_version, possibly_truncated),
                }
            }
        });

        assert_eq!(facts.len(), 2);
        assert_eq!(list_version.as_deref(), Some("list-rv-4"));
        assert!(!truncated);
        assert_eq!(facts[0].resource().identity().uid(), "uid-pod-123");
        assert_eq!(facts[0].resource().resource_version(), Some("rv-23"));
        assert_eq!(facts[0].completeness(), FactCompleteness::Complete);
        assert_eq!(facts[0].source(), FactSource::ApiServer);
        assert!(facts[0].raw_reference().is_some());
        assert_eq!(cache.len(), 1);

        let raw_reference = facts[1].raw_reference().unwrap();
        let cached = cache.read_raw(&target, raw_reference).unwrap();
        assert_eq!(cached.freshness(), FactFreshness::Fresh);
        assert_eq!(cached.fact().resource().identity().uid(), "uid-pod-124");
        assert!(matches!(
            cache.read_raw(&target, facts[0].raw_reference().unwrap()),
            Err(error) if error.kind() == FactErrorKind::RawReferenceExpired
        ));

        let mut other_sessions = crate::target::SessionRegistry::default();
        let other_target = other_sessions.begin_session(
            target.connection().clone(),
            target.context_name().clone(),
            crate::target::NamespaceScope::single("other-team").unwrap(),
        );
        assert_eq!(
            cache.read_raw(&other_target, raw_reference).unwrap_err().kind(),
            FactErrorKind::CacheScopeMismatch
        );
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            cache.read_raw(&target, raw_reference).unwrap().freshness(),
            FactFreshness::Stale
        );

        let detail = caller_runtime
            .block_on(service.get(
                GroupVersionKind::gvk("", "v1", "Pod"),
                None,
                "api-0".to_owned(),
            ))
            .unwrap();
        assert_eq!(detail.resource().identity().uid(), "uid-pod-123");
        assert_eq!(detail.source(), FactSource::ApiServer);

        let requests = mock.requests();
        mock.join();
        assert!(
            requests[10].contains("/api/v1/namespaces/payments/pods?&limit=10"),
            "{requests:?}"
        );
        assert!(requests[11].contains("/api/v1/namespaces/payments/pods/api-0"));
    }

    #[test]
    fn facts_service_rejects_namespace_escape_before_api_request() {
        let mock = MockApi::start(&[200, 200, 200, 200, 200, 200, 200, 200, 200, 200]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let connection = run_connect(imported, target, policy(0)).unwrap();
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let capabilities = caller_runtime
            .block_on(connection.discover_capabilities(
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .unwrap();
        let service = connection
            .read_only_facts(capabilities, SharedFactCache::default())
            .unwrap();
        let stream = service
            .list_stream(
                GroupVersionKind::gvk("", "v1", "Pod"),
                Some("team-b".to_owned()),
                10,
            )
            .unwrap();
        let error = caller_runtime.block_on(stream.recv()).unwrap_err();
        assert!(matches!(
            error,
            crate::fact_service::FactStreamReceiveError::Service(fact_error)
                if fact_error.kind() == FactErrorKind::NamespaceOutsideTarget
        ));
        assert_eq!(mock.request_count(), 10);
        mock.join();
    }

    #[test]
    fn facts_service_preserves_forbidden_read_as_error_not_empty_page() {
        let mock = MockApi::start(&[200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 403]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let connection = run_connect(imported, target, policy(0)).unwrap();
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let capabilities = caller_runtime
            .block_on(connection.discover_capabilities(
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .unwrap();
        let service = connection
            .read_only_facts(capabilities, SharedFactCache::default())
            .unwrap();
        let stream = service
            .list_stream(GroupVersionKind::gvk("", "v1", "Pod"), None, 10)
            .unwrap();
        let error = caller_runtime.block_on(stream.recv()).unwrap_err();
        assert!(matches!(
            error,
            crate::fact_service::FactStreamReceiveError::Service(fact_error)
                if fact_error.kind() == FactErrorKind::PermissionDenied
        ));
        assert_eq!(mock.request_count(), 11);
        mock.join();
    }

    #[test]
    fn facts_service_reports_forbidden_read_instead_of_empty_success() {
        let mock = MockApi::start(&[200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 403]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let connection = run_connect(imported, target, policy(0)).unwrap();
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let capabilities = caller_runtime
            .block_on(connection.discover_capabilities(
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .unwrap();
        let service = connection
            .read_only_facts(capabilities, SharedFactCache::default())
            .unwrap();
        let stream = service
            .list_stream(GroupVersionKind::gvk("", "v1", "Pod"), None, 10)
            .unwrap();
        let message = caller_runtime.block_on(stream.recv());
        assert!(matches!(
            message,
            Err(FactStreamReceiveError::Service(error))
                if error.kind() == FactErrorKind::PermissionDenied
        ));
        mock.join();
    }

    #[test]
    fn ssar_precheck_distinguishes_allowed_denied_and_unknown() {
        use k8s_openapi::api::authorization::v1::SubjectAccessReviewStatus;

        assert_eq!(
            assess_access_review(Some(&SubjectAccessReviewStatus {
                allowed: true,
                denied: Some(false),
                ..SubjectAccessReviewStatus::default()
            })),
            ApiResourceAccessAssessment::Allowed
        );
        assert_eq!(
            assess_access_review(Some(&SubjectAccessReviewStatus {
                allowed: false,
                denied: Some(true),
                ..SubjectAccessReviewStatus::default()
            })),
            ApiResourceAccessAssessment::Denied
        );
        assert_eq!(
            assess_access_review(Some(&SubjectAccessReviewStatus::default())),
            ApiResourceAccessAssessment::Unavailable
        );
    }

    #[test]
    fn unauthorized_and_forbidden_errors_are_classified_without_retry() {
        for (status, expected) in [
            (401, ConnectionFailureKind::CredentialsRejected),
            (403, ConnectionFailureKind::PermissionDenied),
        ] {
            let mock = MockApi::start(&[status]);
            let directory = TempDir::new().unwrap();
            let path = config_file(
                &directory,
                &mock.address,
                &format!("    token: {TEST_TOKEN}\n"),
            );
            let (imported, target) = imported_target(&path);
            let failure = run_connect(imported, target, policy(3)).unwrap_err();

            assert_eq!(failure.kind(), expected);
            assert_eq!(mock.request_count(), 1);
            assert!(!failure.to_string().contains(TEST_TOKEN));
            if status == 403 {
                assert!(failure.next_step().contains("do not request admin access"));
            }
            mock.join();
        }
    }

    #[test]
    fn transient_server_error_retries_only_to_configured_limit() {
        let mock = MockApi::start(&[503, 200]);
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);

        let connected = run_connect(imported, target, policy(1)).unwrap();
        assert_eq!(connected.attempts(), 2);
        assert_eq!(mock.request_count(), 2);
        mock.join();
    }

    #[test]
    fn delayed_api_response_times_out_without_exceeding_retry_limit() {
        let mock = MockApi::start_with_delay(&[200], Duration::from_millis(250));
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let short_policy = ConnectionPolicy {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(40),
            retry_limit: 0,
        };

        let failure = run_connect(imported, target, short_policy).unwrap_err();
        assert_eq!(failure.kind(), ConnectionFailureKind::TimedOut);
        assert_eq!(mock.request_count(), 1);
        mock.join();
    }

    #[test]
    fn cancellation_interrupts_in_flight_probe_without_retry() {
        let mock = MockApi::start_with_delay(&[200], Duration::from_millis(500));
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            &mock.address,
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let (imported, target) = imported_target(&path);
        let cancellation = CancellationToken::new();
        let cancellation_for_switch = cancellation.clone();
        let received_requests = mock.requests.clone();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let started = std::time::Instant::now();
        let result = runtime.block_on(async move {
            tokio::spawn(async move {
                loop {
                    if !received_requests.lock().unwrap().is_empty() {
                        cancellation_for_switch.cancel();
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            });
            connect(imported, target, policy(3), BTreeSet::new(), cancellation).await
        });

        assert_eq!(
            result.unwrap_err().kind(),
            ConnectionFailureKind::Cancelled
        );
        assert!(started.elapsed() < Duration::from_millis(400));
        assert_eq!(mock.request_count(), 1);
        mock.join();
    }

    #[test]
    fn exec_plugin_is_rejected_before_any_process_or_request() {
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            "http://127.0.0.1:9",
            "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: should-never-run\n      interactiveMode: Never\n",
        );
        let (imported, target) = imported_target(&path);
        let failure = run_connect(imported, target, policy(0)).unwrap_err();

        assert_eq!(failure.kind(), ConnectionFailureKind::ExecPluginRequiresTrust);
    }

    #[test]
    fn exec_plugin_requires_exact_persisted_trust_fingerprint() {
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            "http://127.0.0.1:9",
            "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: should-never-run\n      args:\n      - --profile\n      - SECRET_ARG\n      interactiveMode: Never\n",
        );
        let (imported, _) = imported_target(&path);
        let plugin = imported.contexts()[0].exec_plugin().unwrap();
        let trust_fingerprint = plugin.trust_fingerprint().to_owned();
        let kubeconfig = imported.load_raw_for_connection("local").unwrap();
        let untrusted = BTreeSet::new();

        assert_eq!(
            ensure_auth_requires_no_untrusted_plugin(
                &kubeconfig,
                "local",
                Some(&trust_fingerprint),
                &untrusted,
            )
            .unwrap_err()
            .kind(),
            ConnectionFailureKind::ExecPluginRequiresTrust
        );

        let trusted = BTreeSet::from([trust_fingerprint]);
        assert!(ensure_auth_requires_no_untrusted_plugin(
            &kubeconfig,
            "local",
            Some(plugin.trust_fingerprint()),
            &trusted,
        )
        .is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn trusted_exec_plugin_supplies_token_to_read_only_probe() {
        use std::os::unix::fs::PermissionsExt;

        let mock = MockApi::start(&[200]);
        let directory = TempDir::new().unwrap();
        let script_path = directory.path().join("credential-helper");
        let token = "TRUSTED_EXEC_TOKEN_SECRET";
        fs::write(
            &script_path,
            format!(
                "#!/bin/sh\nprintf '%s' '{{\"kind\":\"ExecCredential\",\"apiVersion\":\"client.authentication.k8s.io/v1\",\"status\":{{\"token\":\"{token}\"}}}}'\n"
            ),
        )
        .unwrap();
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o700)).unwrap();
        let auth = format!(
            "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: {}\n      interactiveMode: Never\n",
            serde_json::to_string(&script_path.to_string_lossy().to_string()).unwrap()
        );
        let path = config_file(&directory, &mock.address, &auth);
        let (imported, target) = imported_target(&path);
        let trust_key = imported.contexts()[0]
            .exec_plugin()
            .unwrap()
            .trust_fingerprint()
            .to_owned();
        let connected = run_connect_with_trust(
            imported,
            target,
            policy(0),
            BTreeSet::from([trust_key]),
        )
        .unwrap();
        assert_eq!(connected.attempts(), 1);
        assert!(format!("{connected:?}").contains("local"));
        let requests = mock.requests();
        mock.join();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {token}").to_ascii_lowercase()));
        assert!(!format!("{connected:?}").contains(token));
    }

    #[test]
    fn insecure_tls_and_unbounded_policies_are_rejected() {
        let directory = TempDir::new().unwrap();
        let path = config_file(
            &directory,
            "https://127.0.0.1:9",
            &format!("    token: {TEST_TOKEN}\n"),
        );
        let mut contents = fs::read_to_string(&path).unwrap();
        contents = contents.replace(
            "    server: https://127.0.0.1:9",
            "    server: https://127.0.0.1:9\n    insecure-skip-tls-verify: true",
        );
        fs::write(&path, contents).unwrap();
        let (imported, target) = imported_target(&path);
        let failure = run_connect(imported, target, policy(0)).unwrap_err();

        assert_eq!(failure.kind(), ConnectionFailureKind::TlsVerificationDisabled);
        assert!(ConnectionPolicy::from_seconds(0, 10, 0).is_err());
        assert!(ConnectionPolicy::from_seconds(10, 10, 4).is_err());
    }

    #[test]
    fn transport_errors_map_to_safe_certificate_and_timeout_messages() {
        assert_eq!(
            classify_error_chain(&io::Error::new(io::ErrorKind::TimedOut, "private detail")),
            ConnectionFailureKind::TimedOut
        );
        assert_eq!(
            classify_transport_message("invalid peer certificate: unknown issuer"),
            ConnectionFailureKind::CertificateProblem
        );
    }

    fn classify_transport_message(message: &str) -> ConnectionFailureKind {
        classify_error_chain(&io::Error::other(message))
    }
}