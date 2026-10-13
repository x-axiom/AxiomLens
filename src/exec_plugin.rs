use base64::{Engine as _, engine::general_purpose::STANDARD};
use crate::cancellation::CancellationToken;
use jiff::Timestamp;
use kube::config::{AuthInfo, ExecAuthCluster, ExecConfig, ExecInteractiveMode};
use secrecy::{ExposeSecret, SecretString, zeroize::Zeroize};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    error::Error,
    fmt,
    io::{self, Read},
    process::{Command, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const MAX_PLUGIN_OUTPUT_BYTES: usize = 256 * 1024;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecPluginFailureKind {
    MissingCommand,
    StartFailed,
    TimedOut,
    NonZeroExit,
    OutputTooLarge,
    OutputReadFailed,
    InvalidOutput,
    ApiVersionMismatch,
    MissingCredentials,
    ConflictingCredentials,
    ExpiredCredentials,
    Cancelled,
    InteractiveModeUnsupported,
    InvalidEnvironment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecPluginFailure {
    kind: ExecPluginFailureKind,
}

impl ExecPluginFailure {
    fn new(kind: ExecPluginFailureKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> ExecPluginFailureKind {
        self.kind
    }

    pub fn error_code(self) -> &'static str {
        match self.kind {
            ExecPluginFailureKind::MissingCommand => "exec_missing_command",
            ExecPluginFailureKind::StartFailed => "exec_start_failed",
            ExecPluginFailureKind::TimedOut => "exec_timeout",
            ExecPluginFailureKind::NonZeroExit => "exec_nonzero_exit",
            ExecPluginFailureKind::OutputTooLarge => "exec_output_too_large",
            ExecPluginFailureKind::OutputReadFailed => "exec_output_read_failed",
            ExecPluginFailureKind::InvalidOutput => "exec_invalid_output",
            ExecPluginFailureKind::ApiVersionMismatch => "exec_api_version_mismatch",
            ExecPluginFailureKind::MissingCredentials => "exec_missing_credentials",
            ExecPluginFailureKind::ConflictingCredentials => "exec_conflicting_credentials",
            ExecPluginFailureKind::ExpiredCredentials => "exec_expired_credentials",
            ExecPluginFailureKind::Cancelled => "exec_cancelled",
            ExecPluginFailureKind::InteractiveModeUnsupported => "exec_interactive_unsupported",
            ExecPluginFailureKind::InvalidEnvironment => "exec_invalid_environment",
        }
    }

    pub fn summary(self) -> &'static str {
        match self.kind {
            ExecPluginFailureKind::MissingCommand => "Authentication plugin has no executable.",
            ExecPluginFailureKind::StartFailed => "Authentication plugin could not be started.",
            ExecPluginFailureKind::TimedOut => "Authentication plugin exceeded its time limit.",
            ExecPluginFailureKind::NonZeroExit => "Authentication plugin exited unsuccessfully.",
            ExecPluginFailureKind::OutputTooLarge => "Authentication plugin output exceeded its size limit.",
            ExecPluginFailureKind::OutputReadFailed => "Authentication plugin output could not be read.",
            ExecPluginFailureKind::InvalidOutput => "Authentication plugin returned invalid credentials.",
            ExecPluginFailureKind::ApiVersionMismatch => "Authentication plugin returned an unexpected API version.",
            ExecPluginFailureKind::MissingCredentials => "Authentication plugin returned no usable credentials.",
            ExecPluginFailureKind::ConflictingCredentials => "Authentication plugin returned conflicting credentials.",
            ExecPluginFailureKind::ExpiredCredentials => "Authentication plugin returned already-expired credentials.",
            ExecPluginFailureKind::Cancelled => "Authentication plugin execution was cancelled.",
            ExecPluginFailureKind::InteractiveModeUnsupported => "Interactive authentication is unavailable in this flow.",
            ExecPluginFailureKind::InvalidEnvironment => "Authentication plugin environment configuration is invalid.",
        }
    }
}

impl fmt::Display for ExecPluginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.summary())
    }
}

impl Error for ExecPluginFailure {}

#[derive(Debug)]
pub struct ExecPluginCredentials {
    token: Option<SecretString>,
    client_certificate: Option<String>,
    client_key: Option<SecretString>,
    expiration: Option<Timestamp>,
}

impl ExecPluginCredentials {
    pub fn expiration(&self) -> Option<Timestamp> {
        self.expiration
    }

    pub(crate) fn install(self, auth_info: &mut AuthInfo) {
        auth_info.exec = None;
        auth_info.auth_provider = None;
        auth_info.token_file = None;
        auth_info.client_certificate = None;
        auth_info.client_key = None;
        if let Some(token) = self.token {
            auth_info.token = Some(token);
            auth_info.client_certificate_data = None;
            auth_info.client_key_data = None;
        } else {
            auth_info.token = None;
            auth_info.client_certificate_data = self
                .client_certificate
                .map(|certificate| STANDARD.encode(certificate));
            auth_info.client_key_data = self.client_key.map(|key| {
                SecretString::from(STANDARD.encode(key.expose_secret()))
            });
        }
    }
}

struct CapturedOutput {
    bytes: Vec<u8>,
    exceeded_limit: bool,
}

impl Drop for CapturedOutput {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

#[derive(Deserialize)]
struct ExecCredentialOutput {
    kind: Option<String>,
    #[serde(rename = "apiVersion")]
    api_version: Option<String>,
    status: Option<ExecCredentialStatus>,
}

#[derive(Deserialize)]
struct ExecCredentialStatus {
    #[serde(default, deserialize_with = "deserialize_optional_secret")]
    token: Option<SecretString>,
    #[serde(rename = "clientCertificateData")]
    client_certificate_data: Option<String>,
    #[serde(
        rename = "clientKeyData",
        default,
        deserialize_with = "deserialize_optional_secret"
    )]
    client_key_data: Option<SecretString>,
    #[serde(rename = "expirationTimestamp")]
    expiration_timestamp: Option<String>,
}

fn deserialize_optional_secret<'de, D>(deserializer: D) -> Result<Option<SecretString>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|value| value.map(SecretString::from))
}

#[derive(Serialize)]
struct ExecCredentialRequest<'a> {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'a str,
    spec: ExecCredentialSpec<'a>,
}

#[derive(Serialize)]
struct ExecCredentialSpec<'a> {
    interactive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<&'a ExecAuthCluster>,
}

/// Execute explicitly trusted Kubernetes credential plugin with bounded time and output.
pub fn execute(
    config: &ExecConfig,
    expected_api_version: &str,
    cluster_info: Option<&ExecAuthCluster>,
    timeout: Duration,
) -> Result<ExecPluginCredentials, ExecPluginFailure> {
    execute_cancellable(
        config,
        expected_api_version,
        cluster_info,
        timeout,
        &CancellationToken::new(),
    )
}

pub fn execute_cancellable(
    config: &ExecConfig,
    expected_api_version: &str,
    cluster_info: Option<&ExecAuthCluster>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<ExecPluginCredentials, ExecPluginFailure> {
    if cancellation.is_cancelled() {
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::Cancelled));
    }
    let executable = config
        .command
        .as_deref()
        .filter(|command| !command.trim().is_empty())
        .ok_or_else(|| ExecPluginFailure::new(ExecPluginFailureKind::MissingCommand))?;
    if timeout.is_zero() || config.interactive_mode == Some(ExecInteractiveMode::Always) {
        return Err(ExecPluginFailure::new(
            ExecPluginFailureKind::InteractiveModeUnsupported,
        ));
    }
    let request = ExecCredentialRequest {
        kind: "ExecCredential",
        api_version: expected_api_version,
        spec: ExecCredentialSpec {
            interactive: false,
            cluster: if config.provide_cluster_info {
                Some(cluster_info.ok_or_else(|| {
                    ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput)
                })?)
            } else {
                None
            },
        },
    };
    let exec_info = serde_json::to_string(&request)
        .map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput))?;

    let mut command = Command::new(executable);
    if let Some(args) = &config.args {
        command.args(args);
    }
    if let Some(environment) = &config.env {
        for entry in environment {
            let name = entry.get("name").ok_or_else(|| {
                ExecPluginFailure::new(ExecPluginFailureKind::InvalidEnvironment)
            })?;
            let value = entry.get("value").ok_or_else(|| {
                ExecPluginFailure::new(ExecPluginFailureKind::InvalidEnvironment)
            })?;
            if name.is_empty() || name.chars().any(char::is_control) {
                return Err(ExecPluginFailure::new(
                    ExecPluginFailureKind::InvalidEnvironment,
                ));
            }
            command.env(name, value);
        }
    }
    command
        .env("KUBERNETES_EXEC_INFO", exec_info)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(drop_env) = &config.drop_env {
        for name in drop_env {
            command.env_remove(name);
        }
    }

    let mut child = command
        .spawn()
        .map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::StartFailed))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ExecPluginFailure::new(ExecPluginFailureKind::OutputReadFailed))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ExecPluginFailure::new(ExecPluginFailureKind::OutputReadFailed))?;
    let stdout_reader = spawn_output_reader(stdout);
    let stderr_reader = spawn_output_reader(stderr);
    let deadline = Instant::now() + timeout;
    let status = loop {
        if cancellation.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = join_output_reader(stdout_reader);
            let _ = join_output_reader(stderr_reader);
            return Err(ExecPluginFailure::new(ExecPluginFailureKind::Cancelled));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(PROCESS_POLL_INTERVAL),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_output_reader(stdout_reader);
                let _ = join_output_reader(stderr_reader);
                return Err(ExecPluginFailure::new(ExecPluginFailureKind::TimedOut));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_output_reader(stdout_reader);
                let _ = join_output_reader(stderr_reader);
                return Err(ExecPluginFailure::new(ExecPluginFailureKind::OutputReadFailed));
            }
        }
    };
    let mut stdout = join_output_reader(stdout_reader)?;
    let stderr = join_output_reader(stderr_reader)?;
    if stdout.exceeded_limit || stderr.exceeded_limit {
        stdout.bytes.zeroize();
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::OutputTooLarge));
    }
    if !status.success() {
        stdout.bytes.zeroize();
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::NonZeroExit));
    }

    let parsed = serde_json::from_slice::<ExecCredentialOutput>(&stdout.bytes);
    stdout.bytes.zeroize();
    let mut output = parsed.map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput))?;
    if output.kind.as_deref() != Some("ExecCredential") {
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput));
    }
    if output.api_version.as_deref() != Some(expected_api_version) {
        return Err(ExecPluginFailure::new(
            ExecPluginFailureKind::ApiVersionMismatch,
        ));
    }
    let status = output
        .status
        .take()
        .ok_or_else(|| ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput))?;
    let expiration = status
        .expiration_timestamp
        .as_deref()
        .map(str::parse::<Timestamp>)
        .transpose()
        .map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput))?;
    if expiration.is_some_and(|expires| expires <= Timestamp::now()) {
        return Err(ExecPluginFailure::new(
            ExecPluginFailureKind::ExpiredCredentials,
        ));
    }
    let has_token = status
        .token
        .as_ref()
        .is_some_and(|token| !token.expose_secret().trim().is_empty());
    let has_certificate = status
        .client_certificate_data
        .as_deref()
        .is_some_and(|certificate| !certificate.trim().is_empty());
    let has_key = status
        .client_key_data
        .as_ref()
        .is_some_and(|key| !key.expose_secret().trim().is_empty());
    if has_token && (has_certificate || has_key) {
        return Err(ExecPluginFailure::new(
            ExecPluginFailureKind::ConflictingCredentials,
        ));
    }
    if has_certificate != has_key {
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::InvalidOutput));
    }
    if !has_token && !has_certificate {
        return Err(ExecPluginFailure::new(ExecPluginFailureKind::MissingCredentials));
    }

    Ok(ExecPluginCredentials {
        token: if has_token { status.token } else { None },
        client_certificate: if has_certificate {
            status.client_certificate_data
        } else {
            None
        },
        client_key: if has_certificate {
            status.client_key_data
        } else {
            None
        },
        expiration,
    })
}

fn spawn_output_reader<R>(reader: R) -> JoinHandle<io::Result<CapturedOutput>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = reader;
        let mut bytes = Vec::new();
        let mut exceeded_limit = false;
        let mut buffer = [0_u8; 4096];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let remaining = MAX_PLUGIN_OUTPUT_BYTES.saturating_sub(bytes.len());
            let retained = count.min(remaining);
            bytes.extend_from_slice(&buffer[..retained]);
            exceeded_limit |= retained != count;
        }
        Ok(CapturedOutput {
            bytes,
            exceeded_limit,
        })
    })
}

fn join_output_reader(
    reader: JoinHandle<io::Result<CapturedOutput>>,
) -> Result<CapturedOutput, ExecPluginFailure> {
    reader
        .join()
        .map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::OutputReadFailed))?
        .map_err(|_| ExecPluginFailure::new(ExecPluginFailureKind::OutputReadFailed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const API_VERSION: &str = "client.authentication.k8s.io/v1";

    #[cfg(unix)]
    fn executable(directory: &TempDir, script: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = directory.path().join("credential-plugin");
        fs::write(&path, script).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    fn config(command: String) -> ExecConfig {
        ExecConfig {
            api_version: Some(API_VERSION.to_owned()),
            command: Some(command),
            interactive_mode: Some(ExecInteractiveMode::Never),
            ..ExecConfig::default()
        }
    }

    #[cfg(unix)]
    #[test]
    fn executes_plugin_and_returns_secret_token_without_debug_exposure() {
        let directory = TempDir::new().unwrap();
        let secret = "EXEC_TOKEN_SECRET_NOT_FOR_LOGS";
        let command = executable(
            &directory,
            &format!(
                "#!/bin/sh\nprintf '%s' '{{\"kind\":\"ExecCredential\",\"apiVersion\":\"{API_VERSION}\",\"status\":{{\"token\":\"{secret}\"}}}}'\n"
            ),
        );
        let credentials = execute(&config(command), API_VERSION, None, Duration::from_secs(2)).unwrap();
        assert!(!format!("{credentials:?}").contains(secret));
        assert!(credentials.token.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_plugin_and_errors_never_include_stderr() {
        let directory = TempDir::new().unwrap();
        let command = executable(&directory, "#!/bin/sh\nexec /bin/sleep 5\n");
        let started = Instant::now();
        let failure = execute(&config(command), API_VERSION, None, Duration::from_millis(80))
            .unwrap_err();
        assert_eq!(failure.kind(), ExecPluginFailureKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));

        let nonzero = executable(
            &directory,
            "#!/bin/sh\nprintf '%s' 'PRIVATE_STDERR' >&2\nexit 7\n",
        );
        let failure = execute(&config(nonzero), API_VERSION, None, Duration::from_secs(2))
            .unwrap_err();
        assert_eq!(failure.kind(), ExecPluginFailureKind::NonZeroExit);
        assert!(!failure.to_string().contains("PRIVATE_STDERR"));
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_kills_running_plugin_promptly() {
        let directory = TempDir::new().unwrap();
        let command = executable(&directory, "#!/bin/sh\nexec /bin/sleep 5\n");
        let cancellation = CancellationToken::new();
        let worker_token = cancellation.clone();
        let started = Instant::now();
        let worker = thread::spawn(move || {
            execute_cancellable(
                &config(command),
                API_VERSION,
                None,
                Duration::from_secs(5),
                &worker_token,
            )
        });

        thread::sleep(Duration::from_millis(80));
        cancellation.cancel();
        let failure = worker.join().unwrap().unwrap_err();
        assert_eq!(failure.kind(), ExecPluginFailureKind::Cancelled);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn validates_exec_credential_kind_version_and_status() {
        let directory = TempDir::new().unwrap();
        let command = executable(
            &directory,
            "#!/bin/sh\nprintf '%s' '{\"kind\":\"ExecCredential\",\"apiVersion\":\"client.authentication.k8s.io/v1beta1\",\"status\":{\"token\":\"test\"}}'\n",
        );
        assert_eq!(
            execute(&config(command), API_VERSION, None, Duration::from_secs(2))
                .unwrap_err()
                .kind(),
            ExecPluginFailureKind::ApiVersionMismatch
        );
    }

    #[cfg(unix)]
    #[test]
    fn malformed_json_output_is_rejected_without_echoing_payload() {
        let directory = TempDir::new().unwrap();
        let command = executable(
            &directory,
            "#!/bin/sh\nprintf '%s' 'PRIVATE_INVALID_PLUGIN_OUTPUT'\n",
        );
        let failure = execute(&config(command), API_VERSION, None, Duration::from_secs(10))
            .unwrap_err();
        assert_eq!(failure.kind(), ExecPluginFailureKind::InvalidOutput);
        assert!(!failure.to_string().contains("PRIVATE_INVALID_PLUGIN_OUTPUT"));
    }

    #[cfg(unix)]
    #[test]
    fn oversized_output_is_rejected() {
        let directory = TempDir::new().unwrap();
        let command = executable(
            &directory,
            "#!/bin/sh\nexec /usr/bin/head -c 300000 /dev/zero\n",
        );
        assert_eq!(
            execute(&config(command), API_VERSION, None, Duration::from_secs(2))
                .unwrap_err()
                .kind(),
            ExecPluginFailureKind::OutputTooLarge
        );
    }

    #[test]
    fn absent_executable_fails_with_redacted_error() {
        let config = ExecConfig {
            api_version: Some(API_VERSION.to_owned()),
            command: Some("/no/such/private-helper".to_owned()),
            ..ExecConfig::default()
        };
        let failure = execute(&config, API_VERSION, None, Duration::from_secs(1)).unwrap_err();
        assert_eq!(failure.kind(), ExecPluginFailureKind::StartFailed);
        assert!(!failure.to_string().contains("private-helper"));
    }

    #[test]
    fn output_key_values_are_deserialized_into_secret_wrappers() {
        let output = br#"{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1","status":{"clientCertificateData":"CERTIFICATE DATA","clientKeyData":"PRIVATE KEY DATA","expirationTimestamp":"2030-01-01T00:00:00Z"}}"#;
        let parsed: ExecCredentialOutput = serde_json::from_slice(output).unwrap();
        let status = parsed.status.unwrap();
        assert_eq!(status.client_key_data.unwrap().expose_secret(), "PRIVATE KEY DATA");
        assert!(status.client_certificate_data.is_some());
        assert!(status.expiration_timestamp.is_some());
    }
}