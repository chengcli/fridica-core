//! A generic password identifies a token by service and local account, with no website.
use super::{account, SERVICE, SERVICE_PREFIX};
use anyhow::{ensure, Context, Result};
use security_framework::{
    item::{ItemClass, ItemSearchOptions, Limit},
    passwords::{delete_generic_password, get_generic_password, set_generic_password},
};

fn optional<T>(result: security_framework::base::Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.code() == -25300 => Ok(None), // errSecItemNotFound
        Err(error) => Err(error.into()),
    }
}

pub fn get(name: &str) -> Result<Option<Vec<u8>>> {
    let service = format!("{SERVICE_PREFIX}{name}");
    match optional(get_generic_password(&service, &account()?))? {
        Some(value) => Ok(Some(value)),
        None => optional(get_generic_password(SERVICE, name)),
    }
}

pub fn set(name: &str, value: &[u8]) -> Result<()> {
    let service = format!("{SERVICE_PREFIX}{name}");
    set_generic_password(&service, &account()?, value)?;
    optional(delete_generic_password(SERVICE, name))
        .context("saved password but could not remove the previous Keychain entry")?;
    Ok(())
}

pub fn delete(name: &str) -> Result<()> {
    let service = format!("{SERVICE_PREFIX}{name}");
    let account = account()?;
    let legacy = optional(delete_generic_password(SERVICE, name))?;
    let current = optional(delete_generic_password(&service, &account))?;
    ensure!(current.is_some() || legacy.is_some(), "secret not found");
    Ok(())
}

fn attributes(search: &mut ItemSearchOptions, key: &str) -> Result<Vec<String>> {
    let items = search
        .class(ItemClass::generic_password())
        .load_attributes(true)
        .limit(Limit::All)
        .search();
    Ok(optional(items)?
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| item.simplify_dict()?.remove(key))
        .collect())
}

pub fn list() -> Result<Vec<String>> {
    let mut names = attributes(ItemSearchOptions::new().service(SERVICE), "acct")?;
    for service in attributes(ItemSearchOptions::new().account(&account()?), "svce")? {
        if let Some(name) = service.strip_prefix(SERVICE_PREFIX) {
            names.push(name.to_owned());
        }
    }
    names.sort_unstable();
    names.dedup();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires an unlocked macOS Keychain"]
    fn rotates_legacy_entry_into_current_accounts_generic_password() -> Result<()> {
        let name = format!("fridica/test/{}", uuid::Uuid::new_v4());
        let username = account()?;
        set_generic_password(SERVICE, &name, b"old")?;
        let result = (|| -> Result<()> {
            ensure!(get(&name)?.as_deref() == Some(b"old".as_slice()));
            set(&name, b"new")?;
            ensure!(get_generic_password(&format!("{SERVICE_PREFIX}{name}"), &username)? == b"new");
            ensure!(optional(get_generic_password(SERVICE, &name))?.is_none());
            ensure!(list()?.contains(&name));
            Ok(())
        })();
        delete(&name)?;
        ensure!(get(&name)?.is_none());
        result
    }
}
