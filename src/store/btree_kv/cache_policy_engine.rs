use std::hash::Hash;

use linked_hash_set::LinkedHashSet;

pub trait ICachePolicyEngine<T: Eq + Hash + Clone> {
    fn evict(&mut self) -> T;
    fn get_size(&self) -> usize;
    fn touch(&mut self, item: &T);
}

struct LRUCachePolicyEngine<T: Eq + Hash + Clone> {
    cache: LinkedHashSet<T>,
    max_capacity: usize,
}

impl<T: Eq + Hash + Clone> LRUCachePolicyEngine<T> {
    pub fn new(capacity: usize) -> Self {
        LRUCachePolicyEngine {
            cache: LinkedHashSet::with_capacity(capacity),
            max_capacity: capacity,
        }
    }
}

impl<T: Eq + Hash + Clone> ICachePolicyEngine<T> for LRUCachePolicyEngine<T> {
    fn evict(&mut self) -> T {
        self.cache.pop_front().unwrap()
    }

    fn get_size(&self) -> usize {
        self.cache.len()
    }

    fn touch(&mut self, item: &T) {
        // Add item to the back of the queue. If the item is already present,
        // insert() ensures it is removed from its current position and added
        // to the back.
        self.cache.insert(item.clone());
        assert!(self.cache.len() <= self.max_capacity);
    }
}

pub enum EvictionPolicy {
    LRU,
    LFU
}

pub struct CachePolicyEngineFactory {}

impl CachePolicyEngineFactory {
    pub fn get_engine<T: 'static + Eq + Hash + Clone>(
        eviction_policy: EvictionPolicy, 
        capacity: usize
    ) -> Box<dyn ICachePolicyEngine<T>> {
        match eviction_policy {
            EvictionPolicy::LRU => Box::new(LRUCachePolicyEngine::new(capacity)),
            EvictionPolicy::LFU => panic!("Not yet implemented. Use LRU")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_cache_manager_works() {
        let max_capacity = 2;
        let mut cache_manager: Box<dyn ICachePolicyEngine<usize>> =
            CachePolicyEngineFactory::get_engine(EvictionPolicy::LRU, max_capacity);

        let first_item = 10;
        let second_item = 20;
        let third_item = 30;
        cache_manager.touch(&first_item);
        cache_manager.touch(&second_item);

        assert_eq!(cache_manager.get_size(), max_capacity);
        assert_eq!(cache_manager.evict(), first_item);

        cache_manager.touch(&third_item); // Add a new item
        // Add an existing item. This should move it to the front of the queue.
        cache_manager.touch(&second_item);
        assert_eq!(cache_manager.evict(), third_item);
    }
}
