//! OS-backed credential storage boundary.
//!
//! Credential bodies are passed directly to the platform keyring and are never
//! written to `SettingsStore`, logs, or diagnostic messages.

use crate::settings::CredentialReference;
use keyring::{Entry, Error as KeyringError};
use std::{error::Error, fmt, sync::Arc};

const KEYRING_SERVICE: &str = "com.axiomlens.credentials";
pub const OS_KEYRING_PROVIDER: &str = "os-keyring";
const LEGACY_PROVIDER: &str = "secure-store";
const KEYCHAIN_PROVIDER_ALIAS: &str = "os-keychain";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureStoreCapability {
    MacOsKeychain,
    WindowsCredentialManager,
    SecretService,
    Unsupported,
}

/// Report selected keyring backend. Secret Service requires a user-session
/// service (typically D-Bus); iOS, Android and other unsupported targets have
/// no backend in keyring's `v1` configuration.
pub fn platform_capability() -> SecureStoreCapability {
    #[cfg(target_os = "macos")]
    {
        return SecureStoreCapability::MacOsKeychain;
    }
    #[cfg(target_os = "windows")]
    {
        return SecureStoreCapability::WindowsCredentialManager;
    }
    #[cfg(all(unix, not(any(target_os = "macos", target_os = "ios", target_os = "android"))))]
    {
        return SecureStoreCapability::SecretService;
    }
    #[allow(unreachable_code)]
    SecureStoreCapability::Unsupported
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureStoreError {
    InvalidReference,
    MissingEntry,
    AccessDenied,
    BackendUnavailable,
    OperationFailed,
}

impl fmt::Display for SecureStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidReference => "credential reference is invalid or unsupported",
            Self::MissingEntry => "credential entry was not found",
            Self::AccessDenied => "OS secure storage access was denied",
            Self::BackendUnavailable => "OS secure storage backend is unavailable",
            Self::OperationFailed => "OS secure storage operation failed",
        };
        formatter.write_str(message)
    }
}

impl Error for SecureStoreError {}

trait CredentialBackend: Send + Sync {
    fn set(&self, service: &str, key: &str, secret: &str) -> keyring::Result<()>;
    fn get(&self, service: &str, key: &str) -> keyring::Result<String>;
    fn delete(&self, service: &str, key: &str) -> keyring::Result<()>;
}

struct NativeKeyringBackend;

impl CredentialBackend for NativeKeyringBackend {
    fn set(&self, service: &str, key: &str, secret: &str) -> keyring::Result<()> {
        Entry::new(service, key)?.set_password(secret)
    }

    fn get(&self, service: &str, key: &str) -> keyring::Result<String> {
        Entry::new(service, key)?.get_password()
    }

    fn delete(&self, service: &str, key: &str) -> keyring::Result<()> {
        Entry::new(service, key)?.delete_credential()
    }
}

pub struct OsCredentialStore {
    backend: Arc<dyn CredentialBackend>,
}

impl Default for OsCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OsCredentialStore {
    pub fn new() -> Self {
        Self {
            backend: Arc::new(NativeKeyringBackend),
        }
    }

    pub fn store(
        &self,
        reference: &CredentialReference,
        secret: &str,
    ) -> Result<(), SecureStoreError> {
        let key = validate_reference(reference)?;
        self.backend
            .set(KEYRING_SERVICE, key, secret)
            .map_err(map_keyring_error)
    }

    pub fn retrieve(
        &self,
        reference: &CredentialReference,
    ) -> Result<String, SecureStoreError> {
        let key = validate_reference(reference)?;
        self.backend
            .get(KEYRING_SERVICE, key)
            .map_err(map_keyring_error)
    }

    pub fn delete(&self, reference: &CredentialReference) -> Result<(), SecureStoreError> {
        let key = validate_reference(reference)?;
        self.backend
            .delete(KEYRING_SERVICE, key)
            .map_err(map_keyring_error)
    }

}

fn validate_reference(reference: &CredentialReference) -> Result<&str, SecureStoreError> {
    if !matches!(
        reference.provider.as_str(),
        OS_KEYRING_PROVIDER | LEGACY_PROVIDER | KEYCHAIN_PROVIDER_ALIAS
    ) || reference.key.is_empty()
        || reference.key.len() > 512
        || !reference
            .key
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
    {
        return Err(SecureStoreError::InvalidReference);
    }
    Ok(&reference.key)
}

fn map_keyring_error(error: KeyringError) -> SecureStoreError {
    match error {
        KeyringError::NoEntry => SecureStoreError::MissingEntry,
        KeyringError::NoStorageAccess(_) => SecureStoreError::AccessDenied,
        KeyringError::NoDefaultStore | KeyringError::NotSupportedByStore(_) => {
            SecureStoreError::BackendUnavailable
        }
        KeyringError::Invalid(attribute, _) if attribute == "platform" => {
            SecureStoreError::BackendUnavailable
        }
        KeyringError::Invalid(_, _) | KeyringError::TooLong(_, _) => SecureStoreError::InvalidReference,
        _ => SecureStoreError::OperationFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[derive(Default)]
    struct FakeBackend {
        secrets: Mutex<BTreeMap<String, String>>,
        next_error: Mutex<Option<KeyringError>>,
    }

    impl FakeBackend {
        fn fail_next(&self, error: KeyringError) {
            *self.next_error.lock().unwrap() = Some(error);
        }

        fn take_error(&self) -> keyring::Result<()> {
            match self.next_error.lock().unwrap().take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
    }

    impl CredentialBackend for FakeBackend {
        fn set(&self, _: &str, key: &str, secret: &str) -> keyring::Result<()> {
            self.take_error()?;
            self.secrets
                .lock()
                .unwrap()
                .insert(key.to_owned(), secret.to_owned());
            Ok(())
        }

        fn get(&self, _: &str, key: &str) -> keyring::Result<String> {
            self.take_error()?;
            self.secrets
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or(KeyringError::NoEntry)
        }

        fn delete(&self, _: &str, key: &str) -> keyring::Result<()> {
            self.take_error()?;
            self.secrets
                .lock()
                .unwrap()
                .remove(key)
                .map(|_| ())
                .ok_or(KeyringError::NoEntry)
        }
    }

    fn reference(key: &str) -> CredentialReference {
        CredentialReference {
            provider: OS_KEYRING_PROVIDER.to_owned(),
            key: key.to_owned(),
        }
    }

    #[test]
    fn fake_backend_covers_crud_missing_and_denied_access() {
        let backend = Arc::new(FakeBackend::default());
        let store = OsCredentialStore {
            backend: backend.clone(),
        };
        let reference = reference("test-item-1");
        let secret = "test-only-value-not-a-user-credential";

        assert_eq!(store.retrieve(&reference), Err(SecureStoreError::MissingEntry));
        store.store(&reference, secret).unwrap();
        let settings = crate::settings::SettingsDocument {
            credential_refs: BTreeMap::from([("target".to_owned(), reference.clone())]),
            ..crate::settings::SettingsDocument::default()
        };
        let serialized_settings = serde_json::to_string(&settings).unwrap();
        assert!(!serialized_settings.contains(secret));
        assert!(store
            .retrieve(&reference)
            .is_ok_and(|retrieved| retrieved == secret));
        store.delete(&reference).unwrap();
        assert_eq!(store.retrieve(&reference), Err(SecureStoreError::MissingEntry));

        backend.fail_next(KeyringError::NoStorageAccess(Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "denied",
        ))));
        let error = store.store(&reference, secret).unwrap_err();
        assert_eq!(error, SecureStoreError::AccessDenied);
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }

    #[test]
    fn provider_errors_and_delete_failures_are_redacted() {
        let backend = Arc::new(FakeBackend::default());
        let store = OsCredentialStore {
            backend: backend.clone(),
        };
        let invalid = CredentialReference {
            provider: "plaintext-file".to_owned(),
            key: "credential-value".to_owned(),
        };
        assert_eq!(store.retrieve(&invalid), Err(SecureStoreError::InvalidReference));

        let reference = reference("test-item-2");
        let secret = "another-test-only-value";
        store.store(&reference, secret).unwrap();
        backend.fail_next(KeyringError::PlatformFailure(Box::new(std::io::Error::other(
            "simulated delete failure",
        ))));
        let error = store.delete(&reference).unwrap_err();
        assert_eq!(error, SecureStoreError::OperationFailed);
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
        assert!(store
            .retrieve(&reference)
            .is_ok_and(|retrieved| retrieved == secret));
    }

    #[test]
    fn unsupported_platform_is_reported_as_backend_unavailable() {
        let error = KeyringError::Invalid(
            "platform".to_owned(),
            "this platform has no v1 keyring backend".to_owned(),
        );
        assert_eq!(map_keyring_error(error), SecureStoreError::BackendUnavailable);
    }

    #[test]
    fn secure_references_do_not_serialize_secret_bodies() {
        let settings = crate::settings::SettingsDocument {
            credential_refs: BTreeMap::from([("target".to_owned(), reference("item-4"))]),
            ..crate::settings::SettingsDocument::default()
        };
        let serialized = serde_json::to_string(&settings).unwrap();
        assert!(serialized.contains("item-4"));
        assert!(!serialized.contains("test-only-secret-body"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_keychain_test_credential_can_be_stored_read_and_deleted() {
        struct Cleanup<'a> {
            store: &'a OsCredentialStore,
            reference: &'a CredentialReference,
        }
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                let _ = self.store.delete(self.reference);
            }
        }

        let store = OsCredentialStore::new();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let reference = reference(&format!("s1-014-test-{nonce}"));
        let _cleanup = Cleanup {
            store: &store,
            reference: &reference,
        };
        let test_secret = "axiomlens-s1-014-test-only-secret";

        store.store(&reference, test_secret).unwrap();
        assert!(store
            .retrieve(&reference)
            .is_ok_and(|retrieved| retrieved == test_secret));
        store.delete(&reference).unwrap();
        assert_eq!(store.retrieve(&reference), Err(SecureStoreError::MissingEntry));
    }
}