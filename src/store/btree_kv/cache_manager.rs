use std::hash::Hash;

use linked_hash_set::LinkedHashSet;

pub trait ICacheManager<T: Eq + Hash + Clone> {
    fn evict_cache(&mut self) -> T;
    fn is_full(&self) -> bool;
    fn add(&mut self, item: &T);
}

struct LRUCacheManager<T: Eq + Hash + Clone> {
    cache: LinkedHashSet<T>,
    max_capacity: usize,
}

impl<T: Eq + Hash + Clone> LRUCacheManager<T> {
    pub fn new(capacity: usize) -> Self {
        LRUCacheManager {
            cache: LinkedHashSet::with_capacity(capacity),
            max_capacity: capacity,
        }
    }
}

impl<T: Eq + Hash + Clone> ICacheManager<T> for LRUCacheManager<T> {
    fn evict_cache(&mut self) -> T {
        self.cache.pop_front().unwrap()
    }

    fn is_full(&self) -> bool {
        self.cache.len() >= self.max_capacity
    }

    fn add(&mut self, item: &T) {
        if self.is_full() {
            // Evict an item if the cache is full.
            self.evict_cache();
        }
        // Add item to the back of the queue. If the item is already present,
        // insert() ensures it is removed from its current position and added
        // to the back.
        self.cache.insert(item.clone());
    }
}

pub enum EvictionPolicy {
    LRU,
    LFU
}

pub struct CacheManagerFactory {}

impl CacheManagerFactory {
    pub fn get_cache_manager<T: 'static + Eq + Hash + Clone>(
        eviction_policy: EvictionPolicy, 
        capacity: usize
    ) -> Box<dyn ICacheManager<T>> {
        match eviction_policy {
            EvictionPolicy::LRU => Box::new(LRUCacheManager::new(capacity)),
            EvictionPolicy::LFU => panic!("Not yet implemented. Use LRU")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_cache_manager_works() {
        let mut cache_manager: Box<dyn ICacheManager<usize>> =
            CacheManagerFactory::get_cache_manager(EvictionPolicy::LRU, 2);

        let first_item = 10;
        let second_item = 20;
        let third_item = 30;
        cache_manager.add(&first_item);
        cache_manager.add(&second_item);

        assert_eq!(cache_manager.is_full(), true);
        assert_eq!(cache_manager.evict_cache(), first_item);

        cache_manager.add(&third_item); // Add a new item
        // Add an existing item. This should move it to the front of the queue.
        cache_manager.add(&second_item);
        assert_eq!(cache_manager.evict_cache(), third_item);
    }
}
