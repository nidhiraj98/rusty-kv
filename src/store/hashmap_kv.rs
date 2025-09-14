use std::collections::HashMap;
use super::RustyKV;

pub struct MapRustyKV<T> {
    data_store: HashMap<String, T>,
}

impl<T> RustyKV<T> for MapRustyKV<T> {
    fn new() -> Self {
        MapRustyKV {
            data_store: HashMap::new()
        }
    }

    fn create(&mut self, key: &str, value: T) {
        if self.data_store.contains_key(key) {
            // TODO: Handle error properly
            panic!("Key already exists");
        }
        self.data_store.insert(
            String::from(key),
            value,
        );
    }

    fn update(&mut self, key: &str, value: T) {
        if !self. data_store.contains_key(key) {
            panic!("Key does not exist");
        }
        self.data_store.insert(
            String::from(key),
            value,
        );
    }

    fn delete(&mut self, key: &str) -> bool {
        self.data_store.remove(key).is_some()
    }

    fn get(&self, key: &str) -> &T {
        match self.data_store.get(key) {
            Some(value) => value,
            // TODO: Handle error properly. Don't panic.
            None => panic!("Key does not exist"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.create("key1", "value1".to_string());
        assert_eq!(kv_store.get("key1"), "value1");
    }

    #[test]
    #[should_panic(expected = "Key already exists")]
    fn test_create_existing_key() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.create("key1", "value1".to_string());
        kv_store.create("key1", "value2".to_string()); // This should panic
    }

    #[test]
    fn test_update() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.create("key1", "value1".to_string());
        kv_store.update("key1", "value2".to_string());
        assert_eq!(kv_store.get("key1"), "value2");
    }

    #[test]
    #[should_panic(expected = "Key does not exist")]
    fn test_update_nonexistent_key() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.update("key1", "value1".to_string()); // This should panic
    }

    #[test]
    fn test_delete() {
        let mut kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.create("key1", "value1".to_string());
        assert!(kv_store.delete("key1"));
        assert!(!kv_store.delete("key1")); // Deleting again should return false
    }

    #[test]
    #[should_panic(expected = "Key does not exist")]
    fn test_get_nonexistent_key() {
        let kv_store: MapRustyKV<String> = MapRustyKV::new();
        kv_store.get("key1"); // This should panic
    }
}
