use super::SERVICE;
use anyhow::{ensure, Context, Result};
use secret_service::{
    blocking::{Collection, Item, SecretService},
    EncryptionType,
};
use std::collections::HashMap;

fn with_collection<T>(action: impl FnOnce(&Collection<'_>) -> Result<T>) -> Result<T> {
    let service = SecretService::connect(EncryptionType::Dh)
        .context("cannot connect to Secret Service in this user's D-Bus session")?;
    let collection = service
        .get_default_collection()
        .context("cannot open the default Secret Service collection")?;
    if collection.is_locked()? {
        collection.unlock()?;
    }
    action(&collection)
}

fn attributes(name: &str) -> HashMap<&str, &str> {
    HashMap::from([("service", SERVICE), ("account", name)])
}

fn item<'a>(collection: &'a Collection<'_>, name: &str) -> Result<Option<Item<'a>>> {
    let mut items = collection.search_items(attributes(name))?;
    ensure!(items.len() <= 1, "multiple matching Fridica entries");
    let Some(item) = items.pop() else {
        return Ok(None);
    };
    if item.is_locked()? {
        item.unlock()?;
    }
    Ok(Some(item))
}

pub fn get(name: &str) -> Result<Option<Vec<u8>>> {
    with_collection(|collection| {
        Ok(item(collection, name)?
            .map(|entry| entry.get_secret())
            .transpose()?)
    })
}
pub fn set(name: &str, value: &[u8]) -> Result<()> {
    with_collection(|collection| {
        collection.create_item(name, attributes(name), value, true, "text/plain")?;
        Ok(())
    })
}
pub fn delete(name: &str) -> Result<()> {
    with_collection(|collection| {
        Ok(item(collection, name)?
            .context("secret not found")?
            .delete()?)
    })
}
pub fn list() -> Result<Vec<String>> {
    with_collection(|collection| {
        let mut names = Vec::new();
        for item in collection.search_items(HashMap::from([("service", SERVICE)]))? {
            if let Some(name) = item.get_attributes()?.remove("account") {
                names.push(name);
            }
        }
        Ok(names)
    })
}
