use super::RustyKV;
use std::collections::HashMap;

///
/// A simple in-memory key-value store using a HashMap.
///
pub struct MapRustyKV<T> {
    data_store: HashMap<String, T>,
}

///
/// Implementation of the RustyKV trait for MapRustyKV.
///
impl<T> RustyKV<T> for MapRustyKV<T> {
    ///
    /// Creates a new instance of the key-value store.
    ///
    /// # Examples
    /// let kv_store: MapRustyKV<String> = MapRustyKV::new();
    ///
    fn new() -> Self {
        MapRustyKV {
            data_store: HashMap::new(),
        }
    }

    ///
    /// Saves a key-value pair to the store. If the key already exists, its value is updated.
    ///
    /// # Arguments
    /// * `key` - A string slice that holds the key.
    /// * `value` - The value to be associated with the key.
    ///
    /// # Examples
    /// let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
    /// kv_store.save("key1", "value1".to_string());
    ///
    fn save(&mut self, key: &str, value: T) {
        self.data_store.insert(String::from(key), value);
    }

    ///
    /// Deletes a key-value pair from the store.
    ///
    /// # Arguments
    /// * `key` - A string slice that holds the key to be deleted.
    ///
    /// # Returns
    /// * `true` if the key was found and deleted, `false` otherwise.
    ///
    /// # Examples
    /// let mut kv_store: MapRustyKV<String> = MapRusty
    /// kv_store.save("key1", "value1".to_string());
    /// assert!(kv_store.delete("key1"));
    /// assert!(!kv_store.delete("key1")); // Deleting again should return false
    ///
    fn delete(&mut self, key: &str) -> bool {
        self.data_store.remove(key).is_some()
    }

    ///
    /// Retrieves the value associated with a given key.
    ///
    /// # Arguments
    /// * `key` - A string slice that holds the key to be retrieved.
    ///
    /// # Returns
    /// * `Some(&T)` if the key exists, `None` otherwise.
    ///
    /// # Examples
    /// let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
    /// kv_store.save("key1", "value1".to_string());
    /// assert_eq!(kv_store.get("key1").unwrap(), "value1");
    /// assert_eq!(kv_store.get("key2"), None); // Non-existent key
    ///
    fn get(&self, key: &str) -> Option<&T> {
        self.data_store.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.save("key1", "value1".to_string());
        assert_eq!(kv_store.get("key1").unwrap(), "value1");
    }

    #[test]
    fn test_create_existing_key() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.save("key1", "value1".to_string());
        kv_store.save("key1", "value2".to_string());
        assert_eq!(kv_store.get("key1").unwrap(), "value2");
    }

    #[test]
    fn test_delete() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.save("key1", "value1".to_string());
        assert!(kv_store.delete("key1"));
        assert!(!kv_store.delete("key1")); // Deleting again should return false
    }

    #[test]
    fn test_get_nonexistent_key() {
        let kv_store: MapRustyKV<String> = MapRustyKV::new();
        assert_eq!(kv_store.get("key1"), None);
    }
}
