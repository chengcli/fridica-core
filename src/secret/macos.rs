use super::SERVICE;
use anyhow::Result;
use security_framework::{
    item::{ItemClass, ItemSearchOptions, Limit},
    passwords::{delete_generic_password, get_generic_password, set_generic_password},
};

pub fn get(name: &str) -> Result<Option<Vec<u8>>> {
    match get_generic_password(SERVICE, name) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.code() == -25300 => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub fn set(name: &str, value: &[u8]) -> Result<()> {
    Ok(set_generic_password(SERVICE, name, value)?)
}
pub fn delete(name: &str) -> Result<()> {
    Ok(delete_generic_password(SERVICE, name)?)
}
pub fn list() -> Result<Vec<String>> {
    let search = ItemSearchOptions::new()
        .class(ItemClass::generic_password())
        .service(SERVICE)
        .load_attributes(true)
        .limit(Limit::All)
        .search();
    match search {
        Ok(items) => Ok(items
            .into_iter()
            .filter_map(|item| item.simplify_dict()?.remove("acct"))
            .collect()),
        Err(error) if error.code() == -25300 => Ok(Vec::new()), // errSecItemNotFound
        Err(error) => Err(error.into()),
    }
}
