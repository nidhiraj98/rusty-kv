pub trait RustyKV<T> {
    fn new() -> Self;
    fn get(&self, key: &str) -> Option<&T>;
    fn save(&mut self, key: &str, value: T);
    fn delete(&mut self, key: &str) -> bool;
}

pub mod hashmap_kv;

pub use hashmap_kv::MapRustyKV;