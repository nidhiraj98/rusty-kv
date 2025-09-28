///
/// A trait defining the basic operations for a key-value store.
/// 
pub trait RustyKV<T> {
    ///
    ///  Creates a new instance of the key-value store.
    ///
    fn new() -> Self;

    ///
    /// Saves a key-value pair to the store. If the key already exists, its value is updated.
    /// 
    /// # Arguments
    /// * `key` - A string slice that holds the key.
    /// * `value` - The value to be associated with the key.
    ///
    fn get(&self, key: &str) -> Option<&T>;

    ///
    /// Saves a key-value pair to the store. If the key already exists, its value
    /// is updated.
    /// 
    /// # Arguments
    /// * `key` - A string slice that holds the key.
    /// * `value` - The value to be associated with the key.
    ///
    fn save(&mut self, key: &str, value: T);

    ///
    /// Deletes a key-value pair from the store.
    /// 
    /// # Arguments
    /// * `key` - A string slice that holds the key to be deleted.
    /// 
    /// # Returns
    /// * `true` if the key was found and deleted, `false` otherwise.
    ///
    fn delete(&mut self, key: &str) -> bool;
}

pub mod hashmap_kv;
pub use hashmap_kv::MapRustyKV;

pub mod btree_kv;