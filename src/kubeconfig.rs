use crate::target::{
    ClusterKey, ConnectionFingerprint, ConnectionIdentity, ContextName, NamespaceName,
    NamespaceScope, SessionRegistry, TargetBinding, TargetModelError,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use kube::config::{
    AuthInfo, Cluster, ExecConfig, Kubeconfig, KubeconfigError as KubeLibraryError,
};
use sha2::{Digest, Sha256};
use std::{
    env,
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

const KUBECONFIG_ENV: &str = "KUBECONFIG";
const MAX_KUBECONFIG_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CA_BYTES: u64 = 4 * 1024 * 1024;
const MAX_EXECUTABLE_FINGERPRINT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KubeconfigReferenceKind {
    CertificateAuthority,
    ClientCertificate,
    ClientKey,
    TokenFile,
}

impl KubeconfigReferenceKind {
    fn label(self) -> &'static str {
        match self {
            Self::CertificateAuthority => "certificate-authority",
            Self::ClientCertificate => "client-certificate",
            Self::ClientKey => "client-key",
            Self::TokenFile => "tokenFile",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KubeconfigImportError {
    MultipleConfigSources,
    HomeDirectoryUnavailable,
    MissingConfigFile,
    ConfigFileNotRegular,
    ConfigFileTooLarge,
    ConfigReadFailed,
    InvalidYaml,
    ConflictingDocuments,
    InvalidKubeconfig,
    NoContexts,
    InvalidContext,
    DuplicateContext,
    InvalidCurrentContext,
    MissingClusterReference,
    MissingUserReference,
    MissingApiServer,
    InvalidApiServer,
    InvalidContextNamespace,
    MissingReferencedFile(KubeconfigReferenceKind),
    UnreadableReferencedFile(KubeconfigReferenceKind),
    InvalidReferencedFile(KubeconfigReferenceKind),
    ReferencedFileTooLarge(KubeconfigReferenceKind),
    InvalidCertificateAuthorityData,
    IncompleteClientCertificatePair,
    InvalidExecConfiguration,
    ExecExecutableUnreadable,
    ExecExecutableTooLarge,
    UnknownContext,
    ConfigChangedSinceImport,
    InvalidTargetIdentity,
}

impl fmt::Display for KubeconfigImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MultipleConfigSources => {
                formatter.write_str("KUBECONFIG names multiple files; import one file explicitly")
            }
            Self::HomeDirectoryUnavailable => {
                formatter.write_str("home directory is unavailable for default kubeconfig")
            }
            Self::MissingConfigFile => formatter.write_str("kubeconfig file was not found"),
            Self::ConfigFileNotRegular => formatter.write_str("kubeconfig path is not a regular file"),
            Self::ConfigFileTooLarge => formatter.write_str("kubeconfig file exceeds size limit"),
            Self::ConfigReadFailed => formatter.write_str("kubeconfig file could not be read"),
            Self::InvalidYaml => formatter.write_str("kubeconfig YAML is invalid"),
            Self::ConflictingDocuments => {
                formatter.write_str("kubeconfig documents have conflicting metadata")
            }
            Self::InvalidKubeconfig => formatter.write_str("kubeconfig structure is invalid"),
            Self::NoContexts => formatter.write_str("kubeconfig contains no contexts"),
            Self::InvalidContext => formatter.write_str("kubeconfig context is incomplete or invalid"),
            Self::DuplicateContext => formatter.write_str("kubeconfig contains duplicate context names"),
            Self::InvalidCurrentContext => {
                formatter.write_str("current-context does not reference a listed context")
            }
            Self::MissingClusterReference => {
                formatter.write_str("context references a missing or incomplete cluster")
            }
            Self::MissingUserReference => {
                formatter.write_str("context references a missing or incomplete user")
            }
            Self::MissingApiServer => formatter.write_str("referenced cluster has no API server"),
            Self::InvalidApiServer => formatter.write_str("API server URL is invalid"),
            Self::InvalidContextNamespace => {
                formatter.write_str("context default Namespace is invalid")
            }
            Self::MissingReferencedFile(kind) => {
                write!(formatter, "referenced {} file was not found", kind.label())
            }
            Self::UnreadableReferencedFile(kind) => {
                write!(formatter, "referenced {} file could not be read", kind.label())
            }
            Self::InvalidReferencedFile(kind) => {
                write!(formatter, "referenced {} path is not a regular file", kind.label())
            }
            Self::ReferencedFileTooLarge(kind) => {
                write!(formatter, "referenced {} file exceeds size limit", kind.label())
            }
            Self::InvalidCertificateAuthorityData => {
                formatter.write_str("certificate-authority data is invalid")
            }
            Self::IncompleteClientCertificatePair => {
                formatter.write_str("client certificate and client key must be configured together")
            }
            Self::InvalidExecConfiguration => {
                formatter.write_str("exec authentication configuration is incomplete or invalid")
            }
            Self::ExecExecutableUnreadable => {
                formatter.write_str("exec authentication executable could not be fingerprinted")
            }
            Self::ExecExecutableTooLarge => {
                formatter.write_str("exec authentication executable exceeds fingerprint size limit")
            }
            Self::UnknownContext => formatter.write_str("selected context does not exist"),
            Self::ConfigChangedSinceImport => {
                formatter.write_str("kubeconfig target changed since import; import and select again")
            }
            Self::InvalidTargetIdentity => formatter.write_str("selected target identity is invalid"),
        }
    }
}

impl std::error::Error for KubeconfigImportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecPluginSummary {
    executable: String,
    api_version: Option<String>,
    argument_summary: Vec<String>,
    environment_names: Vec<String>,
    interactive_mode: Option<&'static str>,
    trust_fingerprint: String,
}

impl ExecPluginSummary {
    pub fn executable(&self) -> &str {
        &self.executable
    }

    pub fn api_version(&self) -> Option<&str> {
        self.api_version.as_deref()
    }

    pub fn argument_count(&self) -> usize {
        self.argument_summary.len()
    }

    pub fn argument_summary(&self) -> &[String] {
        &self.argument_summary
    }

    pub fn environment_names(&self) -> &[String] {
        &self.environment_names
    }

    pub fn interactive_mode(&self) -> Option<&'static str> {
        self.interactive_mode
    }

    pub fn trust_fingerprint(&self) -> &str {
        &self.trust_fingerprint
    }
}

fn summarize_exec_plugin(
    source_path: &Path,
    context_name: &str,
    user_name: &str,
    exec: &ExecConfig,
) -> Result<ExecPluginSummary, KubeconfigImportError> {
    let executable = exec
        .command
        .as_deref()
        .filter(|command| !command.trim().is_empty())
        .ok_or(KubeconfigImportError::InvalidExecConfiguration)?;
    if executable.chars().any(char::is_control) {
        return Err(KubeconfigImportError::InvalidExecConfiguration);
    }
    let serialized = serde_json::to_vec(exec)
        .map_err(|_| KubeconfigImportError::InvalidExecConfiguration)?;
    let executable_identity = executable_identity(executable)?;
    let source = source_path.to_string_lossy();
    let trust_fingerprint = fingerprint_parts(&[
        source.as_bytes(),
        context_name.as_bytes(),
        user_name.as_bytes(),
        executable_identity.as_bytes(),
        &serialized,
    ]);
    let mut environment_names = exec
        .env
        .iter()
        .flatten()
        .filter_map(|entry| entry.get("name"))
        .filter(|name| is_safe_environment_name(name))
        .cloned()
        .collect::<Vec<_>>();
    environment_names.sort();
    environment_names.dedup();
    let interactive_mode = exec.interactive_mode.as_ref().map(|mode| match mode {
        kube::config::ExecInteractiveMode::Never => "Never",
        kube::config::ExecInteractiveMode::IfAvailable => "IfAvailable",
        kube::config::ExecInteractiveMode::Always => "Always",
    });
    let argument_summary = exec
        .args
        .iter()
        .flatten()
        .map(|argument| summarize_argument(argument))
        .collect();

    Ok(ExecPluginSummary {
        executable: executable.to_owned(),
        api_version: exec.api_version.clone(),
        argument_summary,
        environment_names,
        interactive_mode,
        trust_fingerprint,
    })
}

fn summarize_argument(argument: &str) -> String {
    if let Some((name, _)) = argument.split_once('=')
        && name.starts_with("--")
        && name[2..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return format!("{name}=[value hidden]");
    }
    if argument.starts_with("--")
        && argument[2..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return argument.to_owned();
    }
    "[value hidden]".to_owned()
}

fn executable_identity(command: &str) -> Result<String, KubeconfigImportError> {
    let Some(path) = resolve_executable_path(command) else {
        return Ok(format!("missing:{command}"));
    };
    let path = fs::canonicalize(&path).unwrap_or(path);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(format!("missing:{}", path.to_string_lossy()));
        }
        Err(_) => return Err(KubeconfigImportError::ExecExecutableUnreadable),
    };
    if !metadata.is_file() {
        return Ok(format!("not-file:{}", path.to_string_lossy()));
    }
    if metadata.len() > MAX_EXECUTABLE_FINGERPRINT_BYTES {
        return Err(KubeconfigImportError::ExecExecutableTooLarge);
    }
    let mut file = File::open(&path).map_err(|_| KubeconfigImportError::ExecExecutableUnreadable)?;
    let mut digest = Sha256::new();
    let mut bytes_read = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| KubeconfigImportError::ExecExecutableUnreadable)?;
        if count == 0 {
            break;
        }
        bytes_read = bytes_read.saturating_add(count as u64);
        if bytes_read > MAX_EXECUTABLE_FINGERPRINT_BYTES {
            return Err(KubeconfigImportError::ExecExecutableTooLarge);
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!(
        "{}:{}:{}",
        path.to_string_lossy(),
        bytes_read,
        encode_hex(&digest.finalize())
    ))
}

fn resolve_executable_path(command: &str) -> Option<PathBuf> {
    let requested = Path::new(command);
    if requested.is_absolute() || requested.components().count() > 1 {
        return Some(requested.to_owned());
    }
    let search_paths = env::var_os("PATH")?;
    for directory in env::split_paths(&search_paths) {
        let candidate = directory.join(command);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        if candidate.extension().is_none() {
            let extensions = env::var_os("PATHEXT").unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".into());
            for extension in extensions.to_string_lossy().split(';') {
                let extension = extension.trim_start_matches('.');
                let candidate = candidate.with_extension(extension);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }
    None
}

fn fingerprint_parts(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    encode_hex(&digest.finalize())
}

fn is_safe_environment_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedContext {
    name: ContextName,
    cluster_name: String,
    api_server: String,
    default_namespace: NamespaceName,
    connection: ConnectionIdentity,
    exec_plugin: Option<ExecPluginSummary>,
}

impl ImportedContext {
    pub fn name(&self) -> &ContextName {
        &self.name
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn api_server(&self) -> &str {
        &self.api_server
    }

    /// Returns configured Namespace, or Kubernetes' `default` when omitted.
    pub fn default_namespace(&self) -> &NamespaceName {
        &self.default_namespace
    }

    pub(crate) fn connection_identity(&self) -> &ConnectionIdentity {
        &self.connection
    }

    pub fn exec_plugin(&self) -> Option<&ExecPluginSummary> {
        self.exec_plugin.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedKubeconfig {
    source_path: PathBuf,
    current_context: Option<ContextName>,
    contexts: Vec<ImportedContext>,
}

impl ImportedKubeconfig {
    /// Import exactly the selected file. Environment-based merging is never used here.
    pub fn import_path(path: impl AsRef<Path>) -> Result<Self, KubeconfigImportError> {
        let source_path = fs::canonicalize(path).map_err(map_io_error)?;
        let metadata = fs::metadata(&source_path).map_err(map_io_error)?;
        if !metadata.is_file() {
            return Err(KubeconfigImportError::ConfigFileNotRegular);
        }
        if metadata.len() > MAX_KUBECONFIG_BYTES {
            return Err(KubeconfigImportError::ConfigFileTooLarge);
        }

        let kubeconfig = Kubeconfig::read_from(&source_path).map_err(map_kube_library_error)?;
        Self::from_parsed(source_path, kubeconfig)
    }

    /// Resolve the conventional source, rejecting multi-file KUBECONFIG rather than merging it.
    pub fn import_from_environment() -> Result<Self, KubeconfigImportError> {
        let configured_paths = env::var_os(KUBECONFIG_ENV);
        let home = env::home_dir();
        let path = resolve_config_source(configured_paths.as_deref(), home.as_deref())?;
        Self::import_path(path)
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn current_context(&self) -> Option<&ContextName> {
        self.current_context.as_ref()
    }

    pub fn contexts(&self) -> &[ImportedContext] {
        &self.contexts
    }

    /// Selection requires an explicit context name and creates a fresh target session.
    pub fn select_context(
        &self,
        context_name: &str,
        sessions: &mut SessionRegistry,
    ) -> Result<TargetBinding, KubeconfigImportError> {
        let context = self
            .contexts
            .iter()
            .find(|context| context.name.as_str() == context_name)
            .ok_or(KubeconfigImportError::UnknownContext)?;
        let scope = NamespaceScope::single(context.default_namespace.as_str())
            .map_err(|_| KubeconfigImportError::InvalidTargetIdentity)?;
        Ok(sessions.begin_session(
            context.connection.clone(),
            context.name.clone(),
            scope,
        ))
    }

    pub fn select_context_in_namespace(
        &self,
        context_name: &str,
        namespace: &str,
        sessions: &mut SessionRegistry,
    ) -> Result<TargetBinding, KubeconfigImportError> {
        let context = self
            .contexts
            .iter()
            .find(|context| context.name.as_str() == context_name)
            .ok_or(KubeconfigImportError::UnknownContext)?;
        let scope = NamespaceScope::single(namespace)
            .map_err(|_| KubeconfigImportError::InvalidContextNamespace)?;
        Ok(sessions.begin_session(
            context.connection.clone(),
            context.name.clone(),
            scope,
        ))
    }

    pub(crate) fn load_raw_for_connection(
        &self,
        context_name: &str,
    ) -> Result<Kubeconfig, KubeconfigImportError> {
        let selected_context = self
            .contexts
            .iter()
            .find(|context| context.name.as_str() == context_name)
            .ok_or(KubeconfigImportError::UnknownContext)?;
        let metadata = fs::metadata(&self.source_path).map_err(map_io_error)?;
        if !metadata.is_file() {
            return Err(KubeconfigImportError::ConfigFileNotRegular);
        }
        if metadata.len() > MAX_KUBECONFIG_BYTES {
            return Err(KubeconfigImportError::ConfigFileTooLarge);
        }

        let kubeconfig = Kubeconfig::read_from(&self.source_path).map_err(map_kube_library_error)?;
        let refreshed = Self::from_parsed(self.source_path.clone(), kubeconfig.clone())?;
        let refreshed_context = refreshed
            .contexts
            .iter()
            .find(|context| context.name == selected_context.name)
            .ok_or(KubeconfigImportError::ConfigChangedSinceImport)?;
        if selected_context.cluster_name != refreshed_context.cluster_name
            || selected_context.api_server != refreshed_context.api_server
            || selected_context.default_namespace != refreshed_context.default_namespace
            || selected_context.connection.fingerprint()
                != refreshed_context.connection.fingerprint()
        {
            return Err(KubeconfigImportError::ConfigChangedSinceImport);
        }
        Ok(kubeconfig)
    }

    fn from_parsed(
        source_path: PathBuf,
        kubeconfig: Kubeconfig,
    ) -> Result<Self, KubeconfigImportError> {
        if kubeconfig.contexts.is_empty() {
            return Err(KubeconfigImportError::NoContexts);
        }

        let current_context = match kubeconfig.current_context.as_deref() {
            Some(name) => {
                if !kubeconfig.contexts.iter().any(|context| context.name == name) {
                    return Err(KubeconfigImportError::InvalidCurrentContext);
                }
                Some(ContextName::new(name).map_err(|_| KubeconfigImportError::InvalidContext)?)
            }
            None => None,
        };

        let mut contexts = Vec::with_capacity(kubeconfig.contexts.len());
        for named_context in &kubeconfig.contexts {
            let name = ContextName::new(named_context.name.clone())
                .map_err(|_| KubeconfigImportError::InvalidContext)?;
            if contexts.iter().any(|context: &ImportedContext| context.name == name) {
                return Err(KubeconfigImportError::DuplicateContext);
            }
            let context = named_context
                .context
                .as_ref()
                .ok_or(KubeconfigImportError::InvalidContext)?;
            if context.cluster.is_empty() {
                return Err(KubeconfigImportError::MissingClusterReference);
            }

            let named_cluster = kubeconfig
                .clusters
                .iter()
                .find(|cluster| cluster.name == context.cluster)
                .ok_or(KubeconfigImportError::MissingClusterReference)?;
            let cluster = named_cluster
                .cluster
                .as_ref()
                .ok_or(KubeconfigImportError::MissingClusterReference)?;
            let api_server = cluster
                .server
                .as_deref()
                .ok_or(KubeconfigImportError::MissingApiServer)?;
            if !is_valid_api_server(api_server) {
                return Err(KubeconfigImportError::InvalidApiServer);
            }

            let certificate_authority_fingerprint =
                certificate_authority_fingerprint(cluster)?;
            let mut exec_plugin = None;
            let authentication_reference = match context.user.as_deref() {
                Some(user_name) => {
                    let named_user = kubeconfig
                        .auth_infos
                        .iter()
                        .find(|user| user.name == user_name)
                        .ok_or(KubeconfigImportError::MissingUserReference)?;
                    let auth_info = named_user
                        .auth_info
                        .as_ref()
                        .ok_or(KubeconfigImportError::MissingUserReference)?;
                    validate_auth_references(auth_info)?;
                    if let Some(exec) = &auth_info.exec {
                        let summary = summarize_exec_plugin(
                            &source_path,
                            name.as_str(),
                            user_name,
                            exec,
                        )?;
                        let fingerprint = summary.trust_fingerprint.clone();
                        exec_plugin = Some(summary);
                        Some(fingerprint)
                    } else {
                        Some(opaque_auth_reference(&source_path, user_name))
                    }
                }
                None => None,
            };

            let fingerprint = ConnectionFingerprint::from_non_secret_identity(
                api_server,
                certificate_authority_fingerprint.as_deref(),
                authentication_reference.as_deref(),
            )
            .map_err(map_target_error)?;
            let default_namespace = NamespaceName::new(
                context.namespace.as_deref().unwrap_or("default"),
            )
            .map_err(|_| KubeconfigImportError::InvalidContextNamespace)?;

            contexts.push(ImportedContext {
                name,
                cluster_name: context.cluster.clone(),
                api_server: api_server.to_owned(),
                default_namespace,
                connection: ConnectionIdentity::new(ClusterKey::generate(), fingerprint),
                exec_plugin,
            });
        }

        Ok(Self {
            source_path,
            current_context,
            contexts,
        })
    }
}

fn resolve_config_source(
    configured_paths: Option<&OsStr>,
    home: Option<&Path>,
) -> Result<PathBuf, KubeconfigImportError> {
    let paths = configured_paths
        .map(env::split_paths)
        .into_iter()
        .flatten()
        .filter(|path| !path.as_os_str().is_empty())
        .collect::<Vec<_>>();
    match paths.as_slice() {
        [path] => Ok(path.clone()),
        [] => home
            .map(|home| home.join(".kube").join("config"))
            .ok_or(KubeconfigImportError::HomeDirectoryUnavailable),
        _ => Err(KubeconfigImportError::MultipleConfigSources),
    }
}

fn map_io_error(error: io::Error) -> KubeconfigImportError {
    if error.kind() == io::ErrorKind::NotFound {
        KubeconfigImportError::MissingConfigFile
    } else {
        KubeconfigImportError::ConfigReadFailed
    }
}

fn map_kube_library_error(error: KubeLibraryError) -> KubeconfigImportError {
    match error {
        KubeLibraryError::ReadConfig(source, _) => map_io_error(source),
        KubeLibraryError::Parse(_) => KubeconfigImportError::InvalidYaml,
        KubeLibraryError::KindMismatch | KubeLibraryError::ApiVersionMismatch => {
            KubeconfigImportError::ConflictingDocuments
        }
        _ => KubeconfigImportError::InvalidKubeconfig,
    }
}

fn map_target_error(_: TargetModelError) -> KubeconfigImportError {
    KubeconfigImportError::InvalidTargetIdentity
}

fn is_valid_api_server(value: &str) -> bool {
    if value.trim() != value || value.bytes().any(|byte| byte.is_ascii_control()) {
        return false;
    }
    let Some((scheme, remainder)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https") || value.contains('@') {
        return false;
    }
    let authority = remainder
        .split(|character| matches!(character, '/' | '?' | '#'))
        .next()
        .unwrap_or_default();
    !authority.is_empty() && !authority.bytes().any(|byte| byte.is_ascii_whitespace())
}

fn certificate_authority_fingerprint(
    cluster: &Cluster,
) -> Result<Option<String>, KubeconfigImportError> {
    let certificate = if let Some(data) = cluster.certificate_authority_data.as_deref() {
        let bytes = STANDARD
            .decode(data)
            .map_err(|_| KubeconfigImportError::InvalidCertificateAuthorityData)?;
        if bytes.is_empty() {
            return Err(KubeconfigImportError::InvalidCertificateAuthorityData);
        }
        Some(bytes)
    } else if let Some(path) = cluster.certificate_authority.as_deref() {
        Some(read_certificate_authority_file(path)?)
    } else {
        None
    };
    Ok(certificate.map(|bytes| format!("sha256:{}", encode_hex(&Sha256::digest(bytes)))))
}

fn read_certificate_authority_file(path: &str) -> Result<Vec<u8>, KubeconfigImportError> {
    let file = open_reference_file(path, KubeconfigReferenceKind::CertificateAuthority)?;
    let metadata = file
        .metadata()
        .map_err(|_| KubeconfigImportError::UnreadableReferencedFile(
            KubeconfigReferenceKind::CertificateAuthority,
        ))?;
    if metadata.len() > MAX_CA_BYTES {
        return Err(KubeconfigImportError::ReferencedFileTooLarge(
            KubeconfigReferenceKind::CertificateAuthority,
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| KubeconfigImportError::UnreadableReferencedFile(
            KubeconfigReferenceKind::CertificateAuthority,
        ))?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_CA_BYTES {
        return Err(KubeconfigImportError::InvalidCertificateAuthorityData);
    }
    Ok(bytes)
}

fn validate_auth_references(auth_info: &AuthInfo) -> Result<(), KubeconfigImportError> {
    let has_client_certificate =
        auth_info.client_certificate_data.is_some() || auth_info.client_certificate.is_some();
    let has_client_key = auth_info.client_key_data.is_some() || auth_info.client_key.is_some();
    if has_client_certificate != has_client_key {
        return Err(KubeconfigImportError::IncompleteClientCertificatePair);
    }

    if auth_info.client_certificate_data.is_none()
        && let Some(path) = auth_info.client_certificate.as_deref()
    {
        validate_reference_file(path, KubeconfigReferenceKind::ClientCertificate)?;
    }
    if auth_info.client_key_data.is_none()
        && let Some(path) = auth_info.client_key.as_deref()
    {
        validate_reference_file(path, KubeconfigReferenceKind::ClientKey)?;
    }
    if auth_info.token.is_none()
        && let Some(path) = auth_info.token_file.as_deref()
    {
        validate_reference_file(path, KubeconfigReferenceKind::TokenFile)?;
    }
    Ok(())
}

fn validate_reference_file(
    path: &str,
    kind: KubeconfigReferenceKind,
) -> Result<(), KubeconfigImportError> {
    open_reference_file(path, kind).map(|_| ())
}

fn open_reference_file(
    path: &str,
    kind: KubeconfigReferenceKind,
) -> Result<File, KubeconfigImportError> {
    let metadata = fs::metadata(path).map_err(|error| map_reference_io_error(error, kind))?;
    if !metadata.is_file() {
        return Err(KubeconfigImportError::InvalidReferencedFile(kind));
    }
    File::open(path).map_err(|error| map_reference_io_error(error, kind))
}

fn map_reference_io_error(
    error: io::Error,
    kind: KubeconfigReferenceKind,
) -> KubeconfigImportError {
    if error.kind() == io::ErrorKind::NotFound {
        KubeconfigImportError::MissingReferencedFile(kind)
    } else {
        KubeconfigImportError::UnreadableReferencedFile(kind)
    }
}

fn opaque_auth_reference(source_path: &Path, user_name: &str) -> String {
    let source = source_path.to_string_lossy();
    let mut identity = Vec::with_capacity(source.len() + user_name.len() + 16);
    identity.extend_from_slice(&(source.len() as u64).to_be_bytes());
    identity.extend_from_slice(source.as_bytes());
    identity.extend_from_slice(&(user_name.len() as u64).to_be_bytes());
    identity.extend_from_slice(user_name.as_bytes());
    format!("kubeconfig-user:{}", encode_hex(&Sha256::digest(identity)))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use tempfile::TempDir;

    const TOKEN: &str = "IMPORT_TEST_TOKEN_DO_NOT_EXPOSE";

    fn kubeconfig_yaml() -> String {
        format!(
            "apiVersion: v1\nkind: Config\ncurrent-context: production\n\
             clusters:\n- name: prod-cluster\n  cluster:\n    server: https://prod.example:6443\n\
             contexts:\n- name: production\n  context:\n    cluster: prod-cluster\n    user: readonly\n    namespace: payments\n\
             users:\n- name: readonly\n  user:\n    token: {TOKEN}\n"
        )
    }

    fn write_config(directory: &TempDir, contents: &str) -> PathBuf {
        let path = directory.path().join("config");
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn import_lists_context_cluster_server_and_effective_namespace() {
        let directory = TempDir::new().unwrap();
        let path = write_config(&directory, &kubeconfig_yaml());

        let imported = ImportedKubeconfig::import_path(&path).unwrap();
        let context = &imported.contexts()[0];
        assert_eq!(imported.current_context().unwrap().as_str(), "production");
        assert_eq!(context.name().as_str(), "production");
        assert_eq!(context.cluster_name(), "prod-cluster");
        assert_eq!(context.api_server(), "https://prod.example:6443");
        assert_eq!(context.default_namespace().as_str(), "payments");
        assert!(!format!("{imported:?}").contains(TOKEN));
    }

    #[test]
    fn context_without_namespace_uses_kubernetes_default() {
        let directory = TempDir::new().unwrap();
        let yaml = kubeconfig_yaml().replace("    namespace: payments\n", "");
        let path = write_config(&directory, &yaml);

        let imported = ImportedKubeconfig::import_path(path).unwrap();
        assert_eq!(
            imported.contexts()[0].default_namespace().as_str(),
            "default"
        );
    }

    #[test]
    fn context_selection_creates_explicit_session_without_modifying_source() {
        let directory = TempDir::new().unwrap();
        let path = write_config(&directory, &kubeconfig_yaml());
        let original = fs::read(&path).unwrap();
        let imported = ImportedKubeconfig::import_path(&path).unwrap();
        let mut sessions = SessionRegistry::default();

        let selected = imported
            .select_context("production", &mut sessions)
            .unwrap();
        assert_eq!(selected.context_name().as_str(), "production");
        assert_eq!(sessions.current(), Some(&selected));
        assert_eq!(fs::read(&path).unwrap(), original);

        let previous_session = selected.session_id();
        let selected_again = imported
            .select_context("production", &mut sessions)
            .unwrap();
        assert_ne!(selected_again.session_id(), previous_session);
    }

    #[test]
    fn explicit_authorized_namespace_override_stays_with_selected_target() {
        let directory = TempDir::new().unwrap();
        let path = write_config(&directory, &kubeconfig_yaml());
        let imported = ImportedKubeconfig::import_path(&path).unwrap();
        let mut sessions = SessionRegistry::default();

        let selected = imported
            .select_context_in_namespace("production", "team-a", &mut sessions)
            .unwrap();

        assert_eq!(
            selected
                .namespace_scope()
                .single_namespace()
                .unwrap()
                .as_str(),
            "team-a"
        );
        assert_eq!(sessions.current(), Some(&selected));
        assert!(matches!(
            imported.select_context_in_namespace("production", "Invalid_Name", &mut sessions),
            Err(KubeconfigImportError::InvalidContextNamespace)
        ));
        assert_eq!(sessions.current(), Some(&selected));
    }

    #[test]
    fn connection_rejects_target_changes_after_import() {
        let directory = TempDir::new().unwrap();
        let path = write_config(&directory, &kubeconfig_yaml());
        let imported = ImportedKubeconfig::import_path(&path).unwrap();
        let changed = kubeconfig_yaml().replace("prod.example", "other.example");
        fs::write(&path, changed).unwrap();

        assert!(matches!(
            imported.load_raw_for_connection("production"),
            Err(KubeconfigImportError::ConfigChangedSinceImport)
        ));
    }

    #[test]
    fn exec_summary_redacts_values_and_configuration_changes_rotate_trust_key() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("config");
        let original = kubeconfig_yaml().replace(
            &format!("    token: {TOKEN}\n"),
            "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: /usr/local/bin/credential-helper\n      args:\n      - --profile\n      - ARG_SECRET_VALUE\n      env:\n      - name: CLOUD_PROFILE\n        value: ENV_SECRET_VALUE\n      interactiveMode: Never\n",
        );
        fs::write(&path, &original).unwrap();
        let imported = ImportedKubeconfig::import_path(&path).unwrap();
        let context = &imported.contexts()[0];
        let plugin = context.exec_plugin().unwrap();
        assert_eq!(plugin.executable(), "/usr/local/bin/credential-helper");
        assert_eq!(plugin.api_version(), Some("client.authentication.k8s.io/v1"));
        assert_eq!(plugin.argument_count(), 2);
        assert_eq!(plugin.environment_names(), &["CLOUD_PROFILE"]);
        assert_eq!(plugin.interactive_mode(), Some("Never"));
        assert!(!format!("{plugin:?}").contains("ARG_SECRET_VALUE"));
        assert!(!format!("{plugin:?}").contains("ENV_SECRET_VALUE"));
        assert!(!format!("{imported:?}").contains("ARG_SECRET_VALUE"));
        assert!(!format!("{imported:?}").contains("ENV_SECRET_VALUE"));

        let first_trust_key = plugin.trust_fingerprint().to_owned();
        let first_connection_fingerprint = context.connection_identity().fingerprint().clone();
        fs::write(
            &path,
            original.replace("credential-helper", "replacement-helper"),
        )
        .unwrap();
        let changed = ImportedKubeconfig::import_path(&path).unwrap();
        let changed_context = &changed.contexts()[0];
        assert_ne!(
            changed_context.exec_plugin().unwrap().trust_fingerprint(),
            first_trust_key
        );
        assert_ne!(
            changed_context.connection_identity().fingerprint(),
            &first_connection_fingerprint
        );
    }

    #[test]
    fn replacing_executable_at_same_path_rotates_trust_fingerprint() {
        let directory = TempDir::new().unwrap();
        let executable = directory.path().join("credential-helper");
        fs::write(&executable, b"plugin version one").unwrap();
        let command = serde_json::to_string(executable.to_str().unwrap()).unwrap();
        let yaml = kubeconfig_yaml().replace(
            &format!("    token: {TOKEN}\n"),
            &format!(
                "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: {command}\n      interactiveMode: Never\n"
            ),
        );
        let path = write_config(&directory, &yaml);
        let initial = ImportedKubeconfig::import_path(&path).unwrap();
        let initial_fingerprint = initial.contexts()[0]
            .exec_plugin()
            .unwrap()
            .trust_fingerprint()
            .to_owned();

        fs::write(&executable, b"plugin version two").unwrap();
        let replaced = ImportedKubeconfig::import_path(&path).unwrap();
        assert_ne!(
            replaced.contexts()[0]
                .exec_plugin()
                .unwrap()
                .trust_fingerprint(),
            initial_fingerprint
        );
    }

    #[test]
    fn unknown_context_does_not_change_current_session() {
        let directory = TempDir::new().unwrap();
        let path = write_config(&directory, &kubeconfig_yaml());
        let imported = ImportedKubeconfig::import_path(path).unwrap();
        let mut sessions = SessionRegistry::default();
        let selected = imported
            .select_context("production", &mut sessions)
            .unwrap();

        assert_eq!(
            imported.select_context("missing", &mut sessions),
            Err(KubeconfigImportError::UnknownContext)
        );
        assert_eq!(sessions.current(), Some(&selected));
    }

    #[test]
    fn missing_files_invalid_yaml_and_dangling_references_are_explicit() {
        let directory = TempDir::new().unwrap();
        let missing = directory.path().join("missing-config");
        assert_eq!(
            ImportedKubeconfig::import_path(missing),
            Err(KubeconfigImportError::MissingConfigFile)
        );

        let invalid_yaml = write_config(&directory, "clusters: [");
        assert_eq!(
            ImportedKubeconfig::import_path(invalid_yaml),
            Err(KubeconfigImportError::InvalidYaml)
        );

        let missing_cluster = write_config(
            &directory,
            "apiVersion: v1\ncontexts:\n- name: prod\n  context:\n    cluster: absent\n",
        );
        assert_eq!(
            ImportedKubeconfig::import_path(missing_cluster),
            Err(KubeconfigImportError::MissingClusterReference)
        );

        let missing_user = write_config(
            &directory,
            "apiVersion: v1\nclusters:\n- name: prod-cluster\n  cluster:\n    server: https://prod.example\ncontexts:\n- name: prod\n  context:\n    cluster: prod-cluster\n    user: absent\n",
        );
        assert_eq!(
            ImportedKubeconfig::import_path(missing_user),
            Err(KubeconfigImportError::MissingUserReference)
        );
    }

    #[test]
    fn missing_credential_reference_is_reported_without_reading_credential_body() {
        let directory = TempDir::new().unwrap();
        let yaml = "apiVersion: v1\nclusters:\n- name: prod-cluster\n  cluster:\n    server: https://prod.example\ncontexts:\n- name: prod\n  context:\n    cluster: prod-cluster\n    user: readonly\nusers:\n- name: readonly\n  user:\n    tokenFile: missing-token\n";
        let path = write_config(&directory, yaml);

        assert_eq!(
            ImportedKubeconfig::import_path(path),
            Err(KubeconfigImportError::MissingReferencedFile(
                KubeconfigReferenceKind::TokenFile
            ))
        );
    }

    #[test]
    fn environment_source_rejects_multiple_files_instead_of_merging() {
        let paths = env::join_paths([Path::new("first-config"), Path::new("second-config")])
            .unwrap();
        assert_eq!(
            resolve_config_source(Some(paths.as_os_str()), Some(Path::new("/home/test"))),
            Err(KubeconfigImportError::MultipleConfigSources)
        );

        let empty = OsString::new();
        assert_eq!(
            resolve_config_source(Some(&empty), Some(Path::new("/home/test"))).unwrap(),
            PathBuf::from("/home/test/.kube/config")
        );
    }
}