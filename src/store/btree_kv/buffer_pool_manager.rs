use std::{collections::HashMap, path::Path, sync::Arc};
use std::io::{Error};
use std::ops::Deref;
use crate::store::btree_kv::{
    cache_policy_engine::{CachePolicyEngineFactory, ICachePolicyEngine, EvictionPolicy},
    commons::{PageId, PAGE_SIZE},
    disk_manager::DiskManager};
use crate::store::btree_kv::frame::{Frame, FrameHandler, FrameMetadata};

pub struct BufferManager {
    // Capacity of the buffer pool. In bytes.
    capacity: usize,
    // Handles disk operations for the Buffer Pool Manager.
    disk_manager: DiskManager,
    // Buffer Pool. This contains Frames of data.
    pool: Vec<Frame>,
    // Metadata for the buffer pool frames.
    pool_metadata: Vec<FrameMetadata>,
    // A map of Page ID against the buffer pool slot index.
    pool_lookup: HashMap<PageId, usize>,
    // Handles cache operations for the buffer pool slots.
    cache_policy_engine: Box<dyn ICachePolicyEngine<PageId>>,
    // Indicates the slots in buffer pool that are vacant.
    vacant_slots: Vec<usize>,
}

impl BufferManager {
    pub(crate) fn new(size: usize) -> Result<Self, Error> {
        Self::new_with_path(size, Path::new("data.db"))
    }

    pub fn new_with_path(size: usize, path: &Path) -> Result<Self, Error> {
        let pool_slots = size / PAGE_SIZE;
        match DiskManager::new(path) {
            Ok(disk_manager) => {
                Ok(BufferManager {
                    capacity: size,
                    disk_manager,
                    pool: vec![Frame::default(); pool_slots],
                    pool_metadata: vec![FrameMetadata::default(); pool_slots],
                    pool_lookup: HashMap::new(),
                    cache_policy_engine: CachePolicyEngineFactory::get_engine(EvictionPolicy::LRU, pool_slots),
                    vacant_slots: (0..pool_slots).collect(),
                })
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn get(&mut self, page_id: PageId) -> Result<FrameHandler, Error> {
        let frame_index;
        if self.pool_lookup.contains_key(&page_id) {
            // Page already present in Buffer Pool.
            frame_index = *self.pool_lookup.get(&page_id).unwrap();

            // Update cache to indicate that this page has been accessed.
            self.cache_policy_engine.touch(&page_id);
        } else {
            // Page not present in Buffer Pool.
            // 1. Fetch page from Disk.
            let mut data: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(&page_id, &mut data)?;

            // 2. Find a vacant slot.
            match self.vacant_slots.pop() {
                None => {
                    frame_index = self.evict_slot();
                }
                Some(index) => {
                    frame_index = index;
                }
            };

            // 3. Update buffer pool.
            self.pool[frame_index].data = Arc::new(data);
            self.pool_metadata[frame_index].page_id = Some(page_id);
            self.pool_metadata[frame_index].is_dirty = false;
            self.pool_lookup.insert(page_id, frame_index);

            // 4. Update cache with the item.
            self.cache_policy_engine.touch(&page_id);
        }
        // Return the page.
        Ok(FrameHandler::new(
            &mut self.pool[frame_index],
            &mut self.pool_metadata[frame_index]
        ))
    }

    // TODO: Add reference counting to prevent eviction of active pages
    fn evict_slot(&mut self) -> usize {
        let evicted_index = self.cache_policy_engine.evict();

        // 1. Fetch evicted frame data and metadata.
        let evicted_frame_index = *self.pool_lookup.get(&evicted_index).unwrap();
        let evicted_frame = &mut self.pool[evicted_frame_index];
        let evicted_frame_metadata = &mut self.pool_metadata[evicted_frame_index];

        // 2. Fetch Page ID for the evicted slot.
        let evicted_page_id = evicted_frame_metadata.page_id.unwrap();

        // 3. Delete entry for that Page ID from buffer_pool_lookup.
        self.pool_lookup.remove(&evicted_page_id);

        // 4. Write entry to disk if the frame was dirty.
        // TODO: Make dirty check and write atomic to prevent race conditions
        if evicted_frame_metadata.is_dirty {
            self.disk_manager
                .write_page(&evicted_page_id, evicted_frame.data.deref())
                .expect("Failed to write to disk.");
            evicted_frame_metadata.is_dirty = false;
        }
        evicted_frame_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::env;

    #[test]
    fn test_buffer_pool_creation() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_creation");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let bpm = BufferManager::new_with_path(16000, &test_file).unwrap();
        assert_eq!(bpm.capacity, 16000);
        assert_eq!(bpm.pool.len(), 2); // 16000 / 8000 = 2 slots
        assert_eq!(bpm.vacant_slots.len(), 2);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_get_page_cache_miss() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_miss");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferManager::new_with_path(8000, &test_file).unwrap();
        let page_id = PageId::new(0);
        
        let result = bpm.get(page_id);
        assert!(result.is_ok());
        assert!(bpm.pool_lookup.contains_key(&page_id));
        assert_eq!(bpm.vacant_slots.len(), 0);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_get_page_cache_hit() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_hit");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferManager::new_with_path(8000, &test_file).unwrap();
        let page_id = PageId::new(0);
        
        // First access - cache miss
        let _frame1 = bpm.get(page_id).unwrap();
        
        // Second access - cache hit
        let _frame2 = bpm.get(page_id).unwrap();
        
        assert!(bpm.pool_lookup.contains_key(&page_id));
        assert_eq!(bpm.vacant_slots.len(), 0);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_eviction() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_eviction");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferManager::new_with_path(8000, &test_file).unwrap(); // Only 1 slot
        
        let page1 = PageId::new(0);
        let page2 = PageId::new(1);
        
        // Fill the buffer pool
        let _frame1 = bpm.get(page1).unwrap();
        assert!(bpm.pool_lookup.contains_key(&page1));
        
        // This should trigger eviction
        let _frame2 = bpm.get(page2).unwrap();
        assert!(bpm.pool_lookup.contains_key(&page2));
        assert!(!bpm.pool_lookup.contains_key(&page1));

        let _ = fs::remove_dir_all(&temp_dir);
    }
}