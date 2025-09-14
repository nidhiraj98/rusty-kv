pub trait RustyKV {
    fn new() -> Self;
    fn create(&mut self, key: &str, value: &str);
    fn get(&self, key: &str) -> String;
    fn update(&mut self, key: &str, value: &str);
    fn delete(&mut self, key: &str) -> bool;
}

pub mod hashmap_kv;

pub use hashmap_kv::MapRustyKV;