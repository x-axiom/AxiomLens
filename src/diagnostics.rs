use directories::ProjectDirs;
use serde::Serialize;
use std::{
    error::Error,
    ffi::OsString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

const CHANNEL_CAPACITY: usize = 256;
const MAX_LOG_BYTES: u64 = 1024 * 1024;
const RETAINED_ARCHIVES: usize = 3;
static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticEvent {
    ApplicationStarted,
    ApplicationWindowFailed,
    SettingsLoadFailed,
    SettingsSaveFailed,
    SimulatedTaskStarted,
    SimulatedTaskCancelRequested,
    SimulatedTaskFinished,
    SimulatedTaskFailed,
    SimulatedTaskCancelled,
    KubernetesConnectionStarted,
    KubernetesConnectionSucceeded,
    KubernetesConnectionFailed,
}

impl DiagnosticEvent {
    fn name(self) -> &'static str {
        match self {
            Self::ApplicationStarted => "application_started",
            Self::ApplicationWindowFailed => "application_window_failed",
            Self::SettingsLoadFailed => "settings_load_failed",
            Self::SettingsSaveFailed => "settings_save_failed",
            Self::SimulatedTaskStarted => "simulated_task_started",
            Self::SimulatedTaskCancelRequested => "simulated_task_cancel_requested",
            Self::SimulatedTaskFinished => "simulated_task_finished",
            Self::SimulatedTaskFailed => "simulated_task_failed",
            Self::SimulatedTaskCancelled => "simulated_task_cancelled",
            Self::KubernetesConnectionStarted => "kubernetes_connection_started",
            Self::KubernetesConnectionSucceeded => "kubernetes_connection_succeeded",
            Self::KubernetesConnectionFailed => "kubernetes_connection_failed",
        }
    }

    fn level(self) -> &'static str {
        match self {
            Self::ApplicationStarted
            | Self::SimulatedTaskStarted
            | Self::SimulatedTaskFinished
            | Self::KubernetesConnectionSucceeded => {
                "INFO"
            }
            Self::SimulatedTaskCancelRequested | Self::KubernetesConnectionStarted => "WARN",
            Self::ApplicationWindowFailed
            | Self::SettingsLoadFailed
            | Self::SettingsSaveFailed
            | Self::SimulatedTaskFailed
            | Self::SimulatedTaskCancelled
            | Self::KubernetesConnectionFailed => "ERROR",
        }
    }
}

#[derive(Debug, Serialize)]
struct DiagnosticRecord {
    timestamp_unix_ms: u64,
    level: &'static str,
    module: &'static str,
    correlation_id: Option<u64>,
    event: &'static str,
    error_code: Option<&'static str>,
    dropped_count: Option<usize>,
}

enum Command {
    Record(DiagnosticRecord),
    Flush(mpsc::SyncSender<()>),
}

#[derive(Debug)]
pub enum DiagnosticsError {
    ConfigDirectoryUnavailable,
    Io(io::Error),
    WorkerUnavailable,
    WriteFailed,
}

impl fmt::Display for DiagnosticsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigDirectoryUnavailable => {
                formatter.write_str("diagnostic log directory is unavailable")
            }
            Self::Io(error) => write!(formatter, "diagnostic log initialization failed: {error}"),
            Self::WorkerUnavailable => formatter.write_str("diagnostic writer is unavailable"),
            Self::WriteFailed => formatter.write_str("diagnostic log write failed"),
        }
    }
}

impl Error for DiagnosticsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for DiagnosticsError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone)]
pub struct Diagnostics {
    sender: SyncSender<Command>,
    write_failed: Arc<AtomicBool>,
    dropped_records: Arc<AtomicUsize>,
}

impl Diagnostics {
    pub fn for_current_user() -> Result<Self, DiagnosticsError> {
        let directories = ProjectDirs::from("com", "AxiomLens", "AxiomLens")
            .ok_or(DiagnosticsError::ConfigDirectoryUnavailable)?;
        Self::in_directory(directories.data_local_dir().join("diagnostics"))
    }

    pub fn in_directory(directory: impl AsRef<Path>) -> Result<Self, DiagnosticsError> {
        Self::with_limits(directory.as_ref(), MAX_LOG_BYTES, RETAINED_ARCHIVES)
    }

    fn with_limits(
        directory: &Path,
        max_log_bytes: u64,
        retained_archives: usize,
    ) -> Result<Self, DiagnosticsError> {
        fs::create_dir_all(directory)?;
        let path = directory.join("axiomlens.jsonl");
        let initial_size = open_log_file(&path)?.metadata()?.len();
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let write_failed = Arc::new(AtomicBool::new(false));
        let worker_failed = write_failed.clone();
        thread::Builder::new()
            .name("axiomlens-diagnostics".to_owned())
            .spawn(move || {
                writer_loop(
                    receiver,
                    path,
                    initial_size,
                    max_log_bytes,
                    retained_archives,
                    worker_failed,
                );
            })?;
        Ok(Self {
            sender,
            write_failed,
            dropped_records: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn record(
        &self,
        event: DiagnosticEvent,
        module: &'static str,
        correlation_id: Option<u64>,
        error_code: Option<&'static str>,
        dropped_count: Option<usize>,
    ) {
        let timestamp_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or_default();
        let record = DiagnosticRecord {
            timestamp_unix_ms,
            level: event.level(),
            module,
            correlation_id,
            event: event.name(),
            error_code,
            dropped_count,
        };

        match self.sender.try_send(Command::Record(record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_records.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.write_failed.store(true, Ordering::Release);
            }
        }
    }

    pub fn flush(&self) -> Result<(), DiagnosticsError> {
        let (sender, receiver) = mpsc::sync_channel(0);
        self.sender
            .send(Command::Flush(sender))
            .map_err(|_| DiagnosticsError::WorkerUnavailable)?;
        receiver
            .recv()
            .map_err(|_| DiagnosticsError::WorkerUnavailable)?;
        if self.write_failed.load(Ordering::Acquire) {
            Err(DiagnosticsError::WriteFailed)
        } else {
            Ok(())
        }
    }

    pub fn dropped_records(&self) -> usize {
        self.dropped_records.load(Ordering::Relaxed)
    }

    pub fn health_notice(&self) -> Option<&'static str> {
        if self.write_failed.load(Ordering::Acquire) {
            Some("Diagnostic logging is unavailable; errors remain visible in the workbench.")
        } else if self.dropped_records() > 0 {
            Some("Diagnostic log is busy; some records were dropped.")
        } else {
            None
        }
    }
}

pub fn next_correlation_id() -> u64 {
    NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed)
}

fn writer_loop(
    receiver: Receiver<Command>,
    path: PathBuf,
    mut current_size: u64,
    max_log_bytes: u64,
    retained_archives: usize,
    write_failed: Arc<AtomicBool>,
) {
    let mut file = match open_log_file(&path) {
        Ok(file) => file,
        Err(_) => {
            write_failed.store(true, Ordering::Release);
            return;
        }
    };

    while let Ok(command) = receiver.recv() {
        match command {
            Command::Record(record) => {
                if append_record(
                    &mut file,
                    &path,
                    &mut current_size,
                    max_log_bytes,
                    retained_archives,
                    &record,
                )
                .is_err()
                {
                    write_failed.store(true, Ordering::Release);
                }
            }
            Command::Flush(reply) => {
                if file.flush().is_err() {
                    write_failed.store(true, Ordering::Release);
                }
                let _ = reply.send(());
            }
        }
    }
    let _ = file.sync_all();
}

fn append_record(
    file: &mut File,
    path: &Path,
    current_size: &mut u64,
    max_log_bytes: u64,
    retained_archives: usize,
    record: &DiagnosticRecord,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
    bytes.push(b'\n');
    if *current_size > 0 && current_size.saturating_add(bytes.len() as u64) > max_log_bytes {
        file.sync_all()?;
        rotate(path, retained_archives)?;
        *file = open_truncated_log_file(path)?;
        *current_size = 0;
    }
    file.write_all(&bytes)?;
    *current_size = current_size.saturating_add(bytes.len() as u64);
    Ok(())
}

fn open_log_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_permissions(path)?;
    Ok(file)
}

fn open_truncated_log_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_permissions(path)?;
    Ok(file)
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_permissions(_: &Path) -> io::Result<()> {
    Ok(())
}

fn rotate(path: &Path, retained_archives: usize) -> io::Result<()> {
    for index in (1..retained_archives).rev() {
        let source = archive_path(path, index);
        if !source.exists() {
            continue;
        }
        let destination = archive_path(path, index + 1);
        if destination.exists() {
            fs::remove_file(&destination)?;
        }
        fs::rename(source, destination)?;
    }

    if path.exists() {
        if retained_archives == 0 {
            fs::remove_file(path)?;
        } else {
            let first_archive = archive_path(path, 1);
            if first_archive.exists() {
                fs::remove_file(&first_archive)?;
            }
            fs::rename(path, first_archive)?;
        }
    }
    Ok(())
}

fn archive_path(path: &Path, index: usize) -> PathBuf {
    let mut name: OsString = path
        .file_name()
        .unwrap_or_default()
        .to_os_string();
    name.push(format!(".{index}"));
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    #[test]
    fn records_are_structured_correlated_and_rotation_is_bounded() {
        let directory = TempDir::new().unwrap();
        let diagnostics = Diagnostics::with_limits(directory.path(), 260, 2).unwrap();
        let correlation_id = next_correlation_id();
        for _ in 0..12 {
            diagnostics.record(
                DiagnosticEvent::SettingsSaveFailed,
                "settings",
                Some(correlation_id),
                Some("permission_denied"),
                None,
            );
        }
        diagnostics.flush().unwrap();

        let files = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert!(files.len() <= 3);
        assert!(files.iter().any(|path| path.ends_with("axiomlens.jsonl.1")));
        let active = fs::read_to_string(directory.path().join("axiomlens.jsonl")).unwrap();
        let record: Value = serde_json::from_str(active.lines().next().unwrap()).unwrap();
        assert!(record["timestamp_unix_ms"].as_u64().is_some());
        assert_eq!(record["level"], "ERROR");
        assert_eq!(record["module"], "settings");
        assert_eq!(record["correlation_id"], correlation_id);
        assert_eq!(record["event"], "settings_save_failed");
        assert_eq!(record["error_code"], "permission_denied");
    }

    #[test]
    fn records_have_no_unstructured_message_field() {
        let directory = TempDir::new().unwrap();
        let diagnostics = Diagnostics::with_limits(directory.path(), MAX_LOG_BYTES, RETAINED_ARCHIVES)
            .unwrap();
        diagnostics.record(
            DiagnosticEvent::SettingsSaveFailed,
            "settings",
            Some(next_correlation_id()),
            Some("permission_denied"),
            None,
        );
        diagnostics.record(
            DiagnosticEvent::SimulatedTaskFailed,
            "simulation",
            Some(next_correlation_id()),
            Some("mock_failure"),
            None,
        );
        diagnostics.flush().unwrap();

        let content = fs::read_to_string(directory.path().join("axiomlens.jsonl")).unwrap();
    let record: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
    assert_eq!(record.as_object().unwrap().len(), 7);
    assert!(record.get("message").is_none());
    assert!(record.get("payload").is_none());
        assert!(content.contains("settings_save_failed"));
        assert!(content.contains("simulated_task_failed"));
    }

    #[cfg(unix)]
    #[test]
    fn log_file_uses_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TempDir::new().unwrap();
        let _diagnostics = Diagnostics::in_directory(directory.path()).unwrap();
        let mode = fs::metadata(directory.path().join("axiomlens.jsonl"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}