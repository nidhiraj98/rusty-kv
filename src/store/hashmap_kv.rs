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

    fn save(&mut self, key: &str, value: T) {
        self.data_store.insert(String::from(key), value);
    }

    fn delete(&mut self, key: &str) -> bool {
        self.data_store.remove(key).is_some()
    }

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
