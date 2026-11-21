use super::SecretStore;
use anyhow::Result;

#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "macos.rs"]
mod platform;

const SERVICE: &str = "fridica";
/// Current user’s OS-backed Fridica credential store.
pub struct NativeStore;

impl SecretStore for NativeStore {
    fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        platform::get(name)
    }
    fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        platform::set(name, value)
    }
    fn delete(&self, name: &str) -> Result<()> {
        platform::delete(name)
    }
    fn list(&self) -> Result<Vec<String>> {
        platform::list()
    }
}
