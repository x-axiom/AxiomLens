use sha2::{Digest, Sha256};
use std::{error::Error, fmt};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClusterKey(Uuid);

impl ClusterKey {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_opaque_id(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunId(Uuid);

impl RunId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ConnectionFingerprint([u8; 32]);

impl fmt::Debug for ConnectionFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_hex())
    }
}

impl ConnectionFingerprint {
    /// Hash only stable, non-secret connection metadata. Context names,
    /// credentials, tokens, and private keys are deliberately not inputs.
    pub fn from_non_secret_identity(
        api_server: &str,
        certificate_authority_fingerprint: Option<&str>,
        authentication_reference: Option<&str>,
    ) -> Result<Self, TargetModelError> {
        if api_server.trim().is_empty()
            || api_server.trim() != api_server
            || api_server.contains('@')
            || api_server.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(TargetModelError::InvalidApiServer);
        }
        if certificate_authority_fingerprint.is_some_and(|value| !is_safe_identity_part(value))
            || authentication_reference.is_some_and(|value| !is_safe_identity_part(value))
        {
            return Err(TargetModelError::InvalidIdentityMaterial);
        }

        let mut digest = Sha256::new();
        update_hash_part(&mut digest, Some(api_server));
        update_hash_part(&mut digest, certificate_authority_fingerprint);
        update_hash_part(&mut digest, authentication_reference);
        Ok(Self(digest.finalize().into()))
    }

    pub fn as_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(self.0.len() * 2);
        for byte in self.0 {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }
}

fn update_hash_part(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        None => digest.update([0]),
    }
}

fn is_safe_identity_part(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetModelError {
    EmptyContextName,
    InvalidApiServer,
    InvalidIdentityMaterial,
    InvalidNamespace,
    EmptyNamespaceSelection,
    DuplicateNamespace,
    InvalidResourceIdentity,
    ResourceOutsideNamespaceScope,
}

impl fmt::Display for TargetModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyContextName => "context name is empty or invalid",
            Self::InvalidApiServer => "API server identity is empty or invalid",
            Self::InvalidIdentityMaterial => "non-secret identity material is invalid",
            Self::InvalidNamespace => "namespace is not a valid DNS label",
            Self::EmptyNamespaceSelection => "selected namespace scope is empty",
            Self::DuplicateNamespace => "namespace scope contains duplicates",
            Self::InvalidResourceIdentity => "resource identity is incomplete",
            Self::ResourceOutsideNamespaceScope => "resource is outside the target namespace scope",
        };
        formatter.write_str(message)
    }
}

impl Error for TargetModelError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContextName(String);

impl ContextName {
    pub fn new(value: impl Into<String>) -> Result<Self, TargetModelError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(TargetModelError::EmptyContextName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamespaceName(String);

impl NamespaceName {
    pub fn new(value: impl Into<String>) -> Result<Self, TargetModelError> {
        let value = value.into();
        if !is_dns_label(&value) {
            return Err(TargetModelError::InvalidNamespace);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamespaceScope(NamespaceScopeKind);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NamespaceScopeKind {
    Single(NamespaceName),
    Selected(Vec<NamespaceName>),
    All,
}

impl NamespaceScope {
    pub fn single(namespace: impl Into<String>) -> Result<Self, TargetModelError> {
        Ok(Self(NamespaceScopeKind::Single(NamespaceName::new(namespace)?)))
    }

    pub fn selected(namespaces: Vec<String>) -> Result<Self, TargetModelError> {
        if namespaces.is_empty() {
            return Err(TargetModelError::EmptyNamespaceSelection);
        }
        let mut selected = namespaces
            .into_iter()
            .map(NamespaceName::new)
            .collect::<Result<Vec<_>, _>>()?;
        selected.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        if selected.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(TargetModelError::DuplicateNamespace);
        }
        Ok(Self(NamespaceScopeKind::Selected(selected)))
    }

    pub fn explicitly_all() -> Self {
        Self(NamespaceScopeKind::All)
    }

    pub fn is_all(&self) -> bool {
        matches!(self.0, NamespaceScopeKind::All)
    }

    fn contains(&self, namespace: &NamespaceName) -> bool {
        match &self.0 {
            NamespaceScopeKind::Single(selected) => selected == namespace,
            NamespaceScopeKind::Selected(selected) => selected.contains(namespace),
            NamespaceScopeKind::All => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionIdentity {
    cluster_key: ClusterKey,
    fingerprint: ConnectionFingerprint,
}

impl ConnectionIdentity {
    pub fn new(cluster_key: ClusterKey, fingerprint: ConnectionFingerprint) -> Self {
        Self {
            cluster_key,
            fingerprint,
        }
    }

    pub fn cluster_key(&self) -> ClusterKey {
        self.cluster_key
    }

    pub fn fingerprint(&self) -> &ConnectionFingerprint {
        &self.fingerprint
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetBinding {
    connection: ConnectionIdentity,
    session_id: SessionId,
    context_name: ContextName,
    namespace_scope: NamespaceScope,
}

impl TargetBinding {
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.connection
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn context_name(&self) -> &ContextName {
        &self.context_name
    }

    pub fn namespace_scope(&self) -> &NamespaceScope {
        &self.namespace_scope
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheScope {
    connection: ConnectionIdentity,
    session_id: SessionId,
    namespace_scope: NamespaceScope,
}

impl CacheScope {
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.connection
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn namespace_scope(&self) -> &NamespaceScope {
        &self.namespace_scope
    }
}

impl TargetBinding {
    pub fn cache_scope(&self) -> CacheScope {
        CacheScope {
            connection: self.connection.clone(),
            session_id: self.session_id,
            namespace_scope: self.namespace_scope.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct SessionRegistry {
    current: Option<TargetBinding>,
}

impl SessionRegistry {
    pub fn begin_session(
        &mut self,
        connection: ConnectionIdentity,
        context_name: ContextName,
        namespace_scope: NamespaceScope,
    ) -> TargetBinding {
        let target = TargetBinding {
            connection,
            session_id: SessionId::generate(),
            context_name,
            namespace_scope,
        };
        self.current = Some(target.clone());
        target
    }

    pub fn current(&self) -> Option<&TargetBinding> {
        self.current.as_ref()
    }

    pub fn is_current(&self, target: &TargetBinding) -> bool {
        self.current.as_ref() == Some(target)
    }

    pub fn accepts_response<T>(&self, response: &BoundResponse<T>) -> bool {
        self.is_current(&response.target)
    }

    pub fn end_session(&mut self) {
        self.current = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceIdentity {
    api_group: String,
    api_version: String,
    kind: String,
    namespace: Option<NamespaceName>,
    name: String,
    uid: String,
}

impl ResourceIdentity {
    pub fn new(
        api_group: impl Into<String>,
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<NamespaceName>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Result<Self, TargetModelError> {
        let api_group = api_group.into();
        let api_version = api_version.into();
        let kind = kind.into();
        let name = name.into();
        let uid = uid.into();
        if api_version.is_empty()
            || kind.is_empty()
            || name.is_empty()
            || uid.is_empty()
            || [api_group.as_str(), api_version.as_str(), kind.as_str(), name.as_str(), uid.as_str()]
                .iter()
                .any(|value| value.chars().any(char::is_control))
        {
            return Err(TargetModelError::InvalidResourceIdentity);
        }
        Ok(Self {
            api_group,
            api_version,
            kind,
            namespace,
            name,
            uid,
        })
    }

    pub fn api_group(&self) -> &str {
        &self.api_group
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn namespace(&self) -> Option<&NamespaceName> {
        self.namespace.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    target: TargetBinding,
    identity: ResourceIdentity,
    resource_version: Option<String>,
}

impl ResourceRef {
    pub fn new(
        target: TargetBinding,
        identity: ResourceIdentity,
        resource_version: Option<String>,
    ) -> Result<Self, TargetModelError> {
        if let Some(namespace) = &identity.namespace {
            if !target.namespace_scope.contains(namespace) {
                return Err(TargetModelError::ResourceOutsideNamespaceScope);
            }
        }
        Ok(Self {
            target,
            identity,
            resource_version,
        })
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn identity(&self) -> &ResourceIdentity {
        &self.identity
    }

    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRequest<T> {
    target: TargetBinding,
    run_id: RunId,
    payload: T,
}

impl<T> BoundRequest<T> {
    pub fn new(target: TargetBinding, payload: T) -> Self {
        Self {
            target,
            run_id: RunId::generate(),
            payload,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundResponse<T> {
    target: TargetBinding,
    run_id: RunId,
    payload: T,
}

impl<T> BoundResponse<T> {
    pub fn new(target: TargetBinding, run_id: RunId, payload: T) -> Self {
        Self {
            target,
            run_id,
            payload,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundEvidence<T> {
    target: TargetBinding,
    run_id: RunId,
    subject: Option<ResourceIdentity>,
    payload: T,
}

impl<T> BoundEvidence<T> {
    pub fn new(
        target: TargetBinding,
        run_id: RunId,
        subject: Option<ResourceIdentity>,
        payload: T,
    ) -> Self {
        Self {
            target,
            run_id,
            subject,
            payload,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn subject(&self) -> Option<&ResourceIdentity> {
        self.subject.as_ref()
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}

fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && is_ascii_lowercase_or_digit(bytes[0])
        && is_ascii_lowercase_or_digit(bytes[bytes.len() - 1])
        && bytes
            .iter()
            .all(|byte| is_ascii_lowercase_or_digit(*byte) || *byte == b'-')
}

fn is_ascii_lowercase_or_digit(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(server: &str, auth_ref: &str) -> ConnectionIdentity {
        ConnectionIdentity {
            cluster_key: ClusterKey::generate(),
            fingerprint: ConnectionFingerprint::from_non_secret_identity(
                server,
                Some("sha256:cluster-ca-fingerprint"),
                Some(auth_ref),
            )
            .unwrap(),
        }
    }

    fn target(
        connection: ConnectionIdentity,
        context: &str,
    ) -> TargetBinding {
        let mut sessions = SessionRegistry::default();
        sessions.begin_session(
            connection,
            ContextName::new(context).unwrap(),
            NamespaceScope::single("payment-platform").unwrap(),
        )
    }

    #[test]
    fn same_context_on_different_connections_has_distinct_cache_and_sessions() {
        let first = target(identity("https://cluster-a.example", "keychain-ref-a"), "prod");
        let second = target(identity("https://cluster-b.example", "keychain-ref-b"), "prod");

        assert_eq!(first.context_name, second.context_name);
        assert_ne!(first.connection.fingerprint, second.connection.fingerprint);
        assert_ne!(first.session_id, second.session_id);
        assert_ne!(first.cache_scope(), second.cache_scope());
    }

    #[test]
    fn old_session_response_is_rejected_after_target_switch() {
        let first_connection = identity("https://cluster-a.example", "keychain-ref-a");
        let second_connection = identity("https://cluster-b.example", "keychain-ref-b");
        let mut sessions = SessionRegistry::default();
        let old_target = sessions.begin_session(
            first_connection,
            ContextName::new("prod").unwrap(),
            NamespaceScope::single("payment-platform").unwrap(),
        );
        let old_request = BoundRequest::new(old_target.clone(), "list pods");
        let old_response = BoundResponse {
            target: old_target.clone(),
            run_id: RunId::generate(),
            payload: "old response",
        };
        let new_target = sessions.begin_session(
            second_connection,
            ContextName::new("prod").unwrap(),
            NamespaceScope::single("payment-platform").unwrap(),
        );

        assert!(!sessions.accepts_response(&old_response));
    assert_eq!(old_request.target(), &old_target);
        assert!(sessions.is_current(&new_target));
        sessions.end_session();
        assert!(!sessions.is_current(&new_target));
    }

    #[test]
    fn request_response_evidence_and_resource_ref_carry_target_binding() {
        let target = target(identity("https://cluster-a.example", "keychain-ref-a"), "prod");
        let run_id = RunId::generate();
        let request = BoundRequest {
            target: target.clone(),
            run_id,
            payload: "list pods",
        };
        let response = BoundResponse {
            target: target.clone(),
            run_id,
            payload: 2usize,
        };
        let resource = ResourceIdentity::new(
            "",
            "v1",
            "Pod",
            Some(NamespaceName::new("payment-platform").unwrap()),
            "api-0",
            "uid-123",
        )
        .unwrap();
        let reference = ResourceRef::new(target.clone(), resource.clone(), Some("rv-9".to_owned()))
            .unwrap();
        let evidence = BoundEvidence {
            target: target.clone(),
            run_id,
            subject: Some(resource.clone()),
            payload: "fixture observation",
        };

        assert_eq!(request.target, target);
        assert_eq!(response.target, target);
        assert_eq!(reference.target, target);
        assert_eq!(reference.identity.uid, "uid-123");
        assert_eq!(reference.resource_version.as_deref(), Some("rv-9"));
        assert_eq!(evidence.target, target);
        assert_eq!(evidence.subject, Some(resource));
    }

    #[test]
    fn resource_recreation_with_same_name_has_distinct_identity() {
        let first = ResourceIdentity::new("apps", "v1", "Deployment", None, "api", "uid-a")
            .unwrap();
        let recreated = ResourceIdentity::new("apps", "v1", "Deployment", None, "api", "uid-b")
            .unwrap();
        assert_ne!(first, recreated);
    }

    #[test]
    fn resource_reference_cannot_escape_bound_namespace_scope() {
        let target = target(identity("https://cluster-a.example", "keychain-ref-a"), "prod");
        let resource = ResourceIdentity::new(
            "",
            "v1",
            "Pod",
            Some(NamespaceName::new("kube-system").unwrap()),
            "api-0",
            "uid-123",
        )
        .unwrap();

        assert!(matches!(
            ResourceRef::new(target, resource, None),
            Err(TargetModelError::ResourceOutsideNamespaceScope)
        ));
    }

    #[test]
    fn namespace_scope_is_explicit_and_validated() {
        assert!(matches!(
            NamespaceScope::selected(Vec::new()),
            Err(TargetModelError::EmptyNamespaceSelection)
        ));
        assert!(matches!(
            NamespaceScope::selected(vec!["prod".to_owned(), "prod".to_owned()]),
            Err(TargetModelError::DuplicateNamespace)
        ));
        assert!(NamespaceScope::single("Prod").is_err());
        assert!(NamespaceScope::explicitly_all().is_all());
    }

    #[test]
    fn fingerprint_uses_only_non_secret_identity_parts() {
        let first = ConnectionFingerprint::from_non_secret_identity(
            "https://cluster.example",
            Some("sha256:ca-a"),
            Some("credential-ref-1"),
        )
        .unwrap();
        let second = ConnectionFingerprint::from_non_secret_identity(
            "https://cluster.example",
            Some("sha256:ca-a"),
            Some("credential-ref-2"),
        )
        .unwrap();

        assert_ne!(first, second);
        assert_eq!(first.as_hex().len(), 64);
        assert!(ConnectionFingerprint::from_non_secret_identity(
            "https://user:password@cluster.example",
            None,
            None,
        )
        .is_err());
    }
}