use super::{account, SERVICE, SERVICE_PREFIX};
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

fn attributes<'a>(service: &'a str, account: &'a str) -> HashMap<&'a str, &'a str> {
    HashMap::from([("service", service), ("account", account)])
}

fn item<'a>(
    collection: &'a Collection<'_>,
    attributes: HashMap<&str, &str>,
) -> Result<Option<Item<'a>>> {
    let mut items = collection.search_items(attributes)?;
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
        let service = format!("{SERVICE_PREFIX}{name}");
        let entry = match item(collection, attributes(&service, &account()?))? {
            Some(entry) => Some(entry),
            None => item(collection, attributes(SERVICE, name))?,
        };
        Ok(entry.map(|entry| entry.get_secret()).transpose()?)
    })
}

pub fn set(name: &str, value: &[u8]) -> Result<()> {
    with_collection(|collection| {
        let service = format!("{SERVICE_PREFIX}{name}");
        let legacy = item(collection, attributes(SERVICE, name))?;
        collection.create_item(
            name,
            attributes(&service, &account()?),
            value,
            true,
            "text/plain",
        )?;
        if let Some(entry) = legacy {
            entry
                .delete()
                .context("saved password but could not remove the previous keyring entry")?;
        }
        Ok(())
    })
}

pub fn delete(name: &str) -> Result<()> {
    with_collection(|collection| {
        let service = format!("{SERVICE_PREFIX}{name}");
        let current = item(collection, attributes(&service, &account()?))?;
        let legacy = item(collection, attributes(SERVICE, name))?;
        ensure!(current.is_some() || legacy.is_some(), "secret not found");
        for entry in legacy.into_iter().chain(current) {
            entry.delete()?;
        }
        Ok(())
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
        for item in collection.search_items(HashMap::from([("account", account()?.as_str())]))? {
            let attrs = item.get_attributes()?;
            if let Some(name) = attrs
                .get("service")
                .and_then(|s| s.strip_prefix(SERVICE_PREFIX))
            {
                names.push(name.to_owned());
            }
        }
        names.sort_unstable();
        names.dedup();
        Ok(names)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires an unlocked Secret Service collection"]
    fn rotates_legacy_gnome_keyring_entry() -> Result<()> {
        let name = format!("fridica/test/{}", uuid::Uuid::new_v4());
        with_collection(|collection| {
            collection.create_item(
                &name,
                attributes(SERVICE, &name),
                b"old",
                false,
                "text/plain",
            )?;
            Ok(())
        })?;
        let result = (|| -> Result<()> {
            ensure!(get(&name)?.as_deref() == Some(b"old".as_slice()));
            ensure!(list()?.contains(&name));
            set(&name, b"new")?;
            set(&name, b"rotated")?;
            with_collection(|collection| {
                let service = format!("{SERVICE_PREFIX}{name}");
                let username = account()?;
                let entry =
                    item(collection, attributes(&service, &username))?.context("entry missing")?;
                ensure!(entry.get_secret()? == b"rotated");
                ensure!(item(collection, attributes(SERVICE, &name))?.is_none());
                Ok(())
            })?;
            ensure!(get(&name)?.as_deref() == Some(b"rotated".as_slice()));
            ensure!(list()?.iter().filter(|n| *n == &name).count() == 1);
            Ok(())
        })();
        delete(&name)?;
        ensure!(get(&name)?.is_none());
        result
    }
}
