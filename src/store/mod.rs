pub trait RustyKV<T> {
    fn new() -> Self;
    fn create(&mut self, key: &str, value: &T);
    fn get(&self, key: &str) -> T;
    fn update(&mut self, key: &str, value: &T);
    fn delete(&mut self, key: &str) -> bool;
}

pub mod hashmap_kv;

pub use hashmap_kv::MapRustyKV;