//! Portable contract for an owner's named credentials. Implementations keep
//! values in a private backing store and enumerate only names.
use anyhow::Result;

pub trait SecretStore: Send + Sync {
    /// Missing entries return None. Locked or unavailable stores return an error.
    fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;
    /// Create an entry or replace its value.
    fn set(&self, name: &str, value: &[u8]) -> Result<()>;
    fn delete(&self, name: &str) -> Result<()>;
    fn list(&self) -> Result<Vec<String>>;
}

/// A validated configuration reference, serialized as a keyring URI. It
/// contains only the logical name, never the stored value.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KeyringRef(String);

impl KeyringRef {
    pub fn named(name: &str) -> Result<Self> {
        let path = name.strip_prefix("fridica/").unwrap_or(name);
        anyhow::ensure!(
            !path.is_empty()
                && path.len() <= 240
                && path.split('/').all(|part| !matches!(part, "" | "." | "..")
                    && part.bytes().all(|b| b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || b"._-".contains(&b))),
            "expected a lowercase secret path such as slack/app-token"
        );
        Ok(Self(format!("fridica/{path}")))
    }

    pub fn name(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for KeyringRef {
    type Error = anyhow::Error;

    fn try_from(uri: String) -> Result<Self> {
        let name = uri
            .strip_prefix("keyring://")
            .ok_or_else(|| anyhow::anyhow!("expected a keyring:// reference"))?;
        Self::named(name)
    }
}

impl From<KeyringRef> for String {
    fn from(reference: KeyringRef) -> Self {
        format!(
            "keyring://{}",
            reference.0.strip_prefix("fridica/").unwrap()
        )
    }
}

#[cfg(feature = "native-secrets")]
mod native;
#[cfg(feature = "native-secrets")]
pub use native::NativeStore;
