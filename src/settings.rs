use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

pub const CURRENT_SCHEMA_VERSION: u32 = 3;
const MAX_SETTINGS_BYTES: u64 = 1024 * 1024;
const MAX_CREDENTIAL_REFERENCES: usize = 128;
const MAX_WORKSPACE_LAYOUT_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SettingsDocument {
    pub schema_version: u32,
    #[serde(default)]
    pub preferences: SettingsPreferences,
    /// Opaque secure-store references only; this map must never contain credential bodies.
    #[serde(default)]
    pub credential_refs: BTreeMap<String, CredentialReference>,
    /// Serialized GPUI DockAreaState, validated again before the UI restores it.
    #[serde(default)]
    pub workspace_layout: Option<Value>,
}

impl Default for SettingsDocument {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            preferences: SettingsPreferences::default(),
            credential_refs: BTreeMap::new(),
            workspace_layout: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SettingsPreferences {
    #[serde(default)]
    pub appearance: AppearancePreference,
    #[serde(default)]
    pub default_namespace: Option<String>,
}

impl Default for SettingsPreferences {
    fn default() -> Self {
        Self {
            appearance: AppearancePreference::System,
            default_namespace: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppearancePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Pointer to a value held outside the ordinary settings file.
pub struct CredentialReference {
    pub provider: String,
    pub key: String,
}

#[derive(Debug)]
pub enum SettingsError {
    Io(io::Error),
    Json(serde_json::Error),
    ConfigDirectoryUnavailable,
    MissingSchemaVersion,
    UnsupportedSchemaVersion(u64),
    InvalidSettings(&'static str),
    FileTooLarge,
}

impl SettingsError {
    pub fn diagnostic_code(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Json(_) => "invalid_json",
            Self::ConfigDirectoryUnavailable => "config_directory_unavailable",
            Self::MissingSchemaVersion => "missing_schema_version",
            Self::UnsupportedSchemaVersion(_) => "unsupported_schema_version",
            Self::InvalidSettings(_) => "invalid_settings",
            Self::FileTooLarge => "file_too_large",
        }
    }
}

impl fmt::Display for SettingsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "settings I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "settings JSON is invalid: {error}"),
            Self::ConfigDirectoryUnavailable => {
                write!(formatter, "user configuration directory is unavailable")
            }
            Self::MissingSchemaVersion => write!(formatter, "settings schema_version is missing"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "settings schema version {version} is unsupported")
            }
            Self::InvalidSettings(field) => write!(formatter, "settings field {field} is invalid"),
            Self::FileTooLarge => write!(formatter, "settings file exceeds size limit"),
        }
    }
}

impl Error for SettingsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SettingsError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for SettingsError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Clone)]
pub struct SettingsStore {
    path: PathBuf,
}

impl SettingsStore {
    pub fn for_current_user() -> Result<Self, SettingsError> {
        let directories = ProjectDirs::from("com", "AxiomLens", "AxiomLens")
            .ok_or(SettingsError::ConfigDirectoryUnavailable)?;
        Ok(Self::at_path(directories.config_dir().join("settings.json")))
    }

    pub fn at_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<SettingsDocument, SettingsError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(SettingsDocument::default());
            }
            Err(error) => return Err(error.into()),
        };

        if file.metadata()?.len() > MAX_SETTINGS_BYTES {
            return Err(SettingsError::FileTooLarge);
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_SETTINGS_BYTES {
            return Err(SettingsError::FileTooLarge);
        }

        let (settings, migrated) = decode_settings(&bytes)?;
        settings.validate()?;
        if migrated {
            self.save(&settings)?;
        }
        Ok(settings)
    }

    pub fn save(&self, settings: &SettingsDocument) -> Result<(), SettingsError> {
        settings.validate()?;
        let contents = serde_json::to_vec_pretty(settings)?;
        if contents.len() as u64 > MAX_SETTINGS_BYTES {
            return Err(SettingsError::FileTooLarge);
        }

        let staged = stage_file(&self.path, &contents)?;
        staged
            .persist(&self.path)
            .map_err(|error| SettingsError::Io(error.error))?;
        Ok(())
    }

    pub fn update(
        &self,
        update: impl FnOnce(&mut SettingsDocument),
    ) -> Result<SettingsDocument, SettingsError> {
        let mut settings = self.load()?;
        update(&mut settings);
        self.save(&settings)?;
        Ok(settings)
    }
}

impl SettingsDocument {
    fn validate(&self) -> Result<(), SettingsError> {
        if self.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(SettingsError::UnsupportedSchemaVersion(u64::from(
                self.schema_version,
            )));
        }
        if self.credential_refs.len() > MAX_CREDENTIAL_REFERENCES {
            return Err(SettingsError::InvalidSettings("credential_refs"));
        }
        if let Some(layout) = &self.workspace_layout {
            if !layout.is_object() || serde_json::to_vec(layout)?.len() > MAX_WORKSPACE_LAYOUT_BYTES {
                return Err(SettingsError::InvalidSettings("workspace_layout"));
            }
        }
        if self
            .preferences
            .default_namespace
            .as_deref()
            .is_some_and(|namespace| !is_valid_namespace(namespace))
        {
            return Err(SettingsError::InvalidSettings("default_namespace"));
        }

        for (target, reference) in &self.credential_refs {
            if !is_safe_identifier(target, 128)
                || !is_safe_identifier(&reference.provider, 64)
                || !is_safe_identifier(&reference.key, 512)
            {
                return Err(SettingsError::InvalidSettings("credential_refs"));
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsV1 {
    schema_version: u32,
    #[serde(default)]
    theme: AppearancePreference,
    #[serde(default)]
    default_namespace: Option<String>,
    #[serde(default)]
    credential_reference: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsV2 {
    schema_version: u32,
    #[serde(default)]
    preferences: SettingsPreferences,
    #[serde(default)]
    credential_refs: BTreeMap<String, CredentialReference>,
}

fn decode_settings(bytes: &[u8]) -> Result<(SettingsDocument, bool), SettingsError> {
    let value: Value = serde_json::from_slice(bytes)?;
    let version = value
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or(SettingsError::MissingSchemaVersion)?;

    match version {
        1 => {
            let legacy: SettingsV1 = serde_json::from_value(value)?;
            if legacy.schema_version != 1 {
                return Err(SettingsError::UnsupportedSchemaVersion(version));
            }
            let mut credential_refs = BTreeMap::new();
            if let Some(key) = legacy.credential_reference {
                credential_refs.insert(
                    "default".to_owned(),
                    CredentialReference {
                        provider: "secure-store".to_owned(),
                        key,
                    },
                );
            }
            let settings = SettingsDocument {
                schema_version: CURRENT_SCHEMA_VERSION,
                preferences: SettingsPreferences {
                    appearance: legacy.theme,
                    default_namespace: legacy.default_namespace,
                },
                credential_refs,
                workspace_layout: None,
            };
            settings.validate()?;
            Ok((settings, true))
        }
        2 => {
            let legacy: SettingsV2 = serde_json::from_value(value)?;
            if legacy.schema_version != 2 {
                return Err(SettingsError::UnsupportedSchemaVersion(version));
            }
            let settings = SettingsDocument {
                schema_version: CURRENT_SCHEMA_VERSION,
                preferences: legacy.preferences,
                credential_refs: legacy.credential_refs,
                workspace_layout: None,
            };
            settings.validate()?;
            Ok((settings, true))
        }
        version if version == u64::from(CURRENT_SCHEMA_VERSION) => {
            let settings: SettingsDocument = serde_json::from_value(value)?;
            settings.validate()?;
            Ok((settings, false))
        }
        _ => Err(SettingsError::UnsupportedSchemaVersion(version)),
    }
}

fn stage_file(path: &Path, contents: &[u8]) -> Result<NamedTempFile, SettingsError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut staged = NamedTempFile::new_in(parent)?;
    staged.write_all(contents)?;
    staged.as_file().sync_all()?;
    Ok(staged)
}

fn is_valid_namespace(namespace: &str) -> bool {
    let bytes = namespace.as_bytes();
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

fn is_safe_identifier(identifier: &str, max_len: usize) -> bool {
    !identifier.is_empty()
        && identifier.len() <= max_len
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store(directory: &TempDir) -> SettingsStore {
        SettingsStore::at_path(directory.path().join("config").join("settings.json"))
    }

    #[test]
    fn defaults_round_trip_and_update_persist_versioned_preferences() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        assert_eq!(store.load().unwrap(), SettingsDocument::default());

        let updated = store
            .update(|settings| {
                settings.preferences.appearance = AppearancePreference::Dark;
                settings.preferences.default_namespace = Some("payment-platform".to_owned());
                settings.workspace_layout = Some(serde_json::json!({
                    "version": 1,
                    "center": {"panel_name": "ResourceListPanel"}
                }));
                settings.credential_refs.insert(
                    "primary-cluster".to_owned(),
                    CredentialReference {
                        provider: "os-keychain".to_owned(),
                        key: "item-7f4c".to_owned(),
                    },
                );
            })
            .unwrap();

        assert_eq!(updated.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(store.load().unwrap(), updated);
        let serialized = fs::read_to_string(store.path()).unwrap();
        assert!(serialized.contains("item-7f4c"));
        assert!(!serialized.contains("credential_value"));
    }

    #[test]
    fn schema_v1_migrates_and_rewrites_as_v3() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        fs::create_dir_all(store.path().parent().unwrap()).unwrap();
        fs::write(
            store.path(),
            br#"{
                "schema_version": 1,
                "theme": "dark",
                "default_namespace": "payment-platform",
                "credential_reference": "keychain-item-42"
            }"#,
        )
        .unwrap();

        let migrated = store.load().unwrap();
        assert_eq!(migrated.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(migrated.preferences.appearance, AppearancePreference::Dark);
        assert_eq!(
            migrated.preferences.default_namespace.as_deref(),
            Some("payment-platform")
        );
        assert_eq!(
            migrated.credential_refs["default"].key,
            "keychain-item-42"
        );
        let rewritten: Value = serde_json::from_slice(&fs::read(store.path()).unwrap()).unwrap();
        assert_eq!(rewritten["schema_version"], CURRENT_SCHEMA_VERSION);
        assert!(rewritten.get("credential_reference").is_none());
    }

    #[test]
    fn schema_v2_migrates_to_v3_with_default_workspace_layout() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        fs::create_dir_all(store.path().parent().unwrap()).unwrap();
        fs::write(
            store.path(),
            br#"{
                "schema_version": 2,
                "preferences": {"appearance": "light", "default_namespace": "payment-platform"},
                "credential_refs": {}
            }"#,
        )
        .unwrap();

        let migrated = store.load().unwrap();
        assert_eq!(migrated.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(migrated.preferences.appearance, AppearancePreference::Light);
        assert!(migrated.workspace_layout.is_none());
        let rewritten: Value = serde_json::from_slice(&fs::read(store.path()).unwrap()).unwrap();
        assert_eq!(rewritten["schema_version"], CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn unsupported_and_corrupt_files_are_preserved() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        fs::create_dir_all(store.path().parent().unwrap()).unwrap();

        for contents in [
            br#"{"schema_version":99}"#.as_slice(),
            b"{ invalid json",
        ] {
            fs::write(store.path(), contents).unwrap();
            assert!(store.load().is_err());
            assert_eq!(fs::read(store.path()).unwrap(), contents);
        }
    }

    #[test]
    fn invalid_update_leaves_previous_file_unchanged() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        store.save(&SettingsDocument::default()).unwrap();
        let original = fs::read(store.path()).unwrap();

        let result = store.update(|settings| {
            settings.preferences.default_namespace = Some("Not_A_Namespace".to_owned());
        });

        assert!(matches!(result, Err(SettingsError::InvalidSettings("default_namespace"))));
        assert_eq!(fs::read(store.path()).unwrap(), original);
    }

    #[test]
    fn interrupted_staging_and_directory_errors_preserve_existing_data() {
        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        store.save(&SettingsDocument::default()).unwrap();
        let original = fs::read(store.path()).unwrap();
        let replacement = serde_json::to_vec(&SettingsDocument {
            schema_version: CURRENT_SCHEMA_VERSION,
            preferences: SettingsPreferences {
                appearance: AppearancePreference::Light,
                default_namespace: None,
            },
            credential_refs: BTreeMap::new(),
            workspace_layout: None,
        })
        .unwrap();

        drop(stage_file(store.path(), &replacement).unwrap());
        assert_eq!(fs::read(store.path()).unwrap(), original);

        let blocking_file = directory.path().join("not-a-directory");
        fs::write(&blocking_file, b"preserve-me").unwrap();
        let failing_store = SettingsStore::at_path(blocking_file.join("settings.json"));
        assert!(failing_store.save(&SettingsDocument::default()).is_err());
        assert_eq!(fs::read(blocking_file).unwrap(), b"preserve-me");
    }

    #[cfg(unix)]
    #[test]
    fn permission_denied_preserves_previous_settings() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TempDir::new().unwrap();
        let store = store(&directory);
        store.save(&SettingsDocument::default()).unwrap();
        let original = fs::read(store.path()).unwrap();
        let parent = store.path().parent().unwrap();
        let original_mode = fs::metadata(parent).unwrap().permissions().mode();
        fs::set_permissions(parent, fs::Permissions::from_mode(original_mode & !0o222)).unwrap();

        let result = store.save(&SettingsDocument::default());
        fs::set_permissions(parent, fs::Permissions::from_mode(original_mode)).unwrap();

        assert!(matches!(
            result,
            Err(SettingsError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied
        ));
        assert_eq!(fs::read(store.path()).unwrap(), original);
    }
}