use std::{collections::HashMap, path::Path, sync::Arc};
use std::io::{Error};
use std::ops::Deref;
use crate::store::btree_kv::{
    cache_manager::{CacheManagerFactory, ICacheManager, EvictionPolicy},
    commons::{PageId, PAGE_SIZE},
    disk_manager::DiskManager};
use crate::store::btree_kv::frame::{Frame, FrameHandler, FrameMetadata};

pub struct BufferPoolManager {
    // Capacity of the buffer pool. In bytes.
    capacity: usize,
    // Handles disk operations for the Buffer Pool Manager.
    disk_manager: DiskManager,
    // Buffer Pool. This contains Frames of data.
    buffer_pool: Vec<Frame>,
    // Metadata for the buffer pool frames.
    buffer_pool_metadata: Vec<FrameMetadata>,
    // A map of Page ID against the buffer pool slot index.
    buffer_pool_lookup: HashMap<PageId, usize>,
    // Handles cache operations for the buffer pool slots.
    cache_manager: Box<dyn ICacheManager<usize>>,
    // Indicates the slots in buffer pool that are vacant.
    vacant_slots: Vec<usize>,
}

impl BufferPoolManager {
    pub(crate) fn new(size: usize) -> Result<Self, Error> {
        Self::new_with_path(size, Path::new("data.db"))
    }

    pub fn new_with_path(size: usize, path: &Path) -> Result<Self, Error> {
        let buffer_pool_slots = size / PAGE_SIZE;
        match DiskManager::new(path) {
            Ok(disk_manager) => {
                Ok(BufferPoolManager {
                    capacity: size,
                    disk_manager,
                    buffer_pool: vec![Frame::default(); buffer_pool_slots],
                    buffer_pool_metadata: vec![FrameMetadata::default(); buffer_pool_slots],
                    buffer_pool_lookup: HashMap::new(),
                    cache_manager: CacheManagerFactory::get_cache_manager(EvictionPolicy::LRU, buffer_pool_slots),
                    vacant_slots: (0..buffer_pool_slots).collect(),
                })
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn get(&mut self, page_id: PageId) -> Result<FrameHandler, Error> {
        if self.buffer_pool_lookup.contains_key(&page_id) {
            // Page already present in Buffer Pool.
            let frame_index = self.buffer_pool_lookup.get(&page_id).unwrap();

            // 1. Update cache to indicate that this page has been accessed.
            self.cache_manager.add(frame_index);

            // 2. Return the frame from buffer pool.
            Ok(FrameHandler::new(
                &mut self.buffer_pool[*frame_index],
                &mut self.buffer_pool_metadata[*frame_index]
            ))
        } else {
            // Page not present in Buffer Pool.
            // 1. Fetch page from Disk.
            let mut data: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(&page_id, &mut data)?;

            // 2. Find a vacant slot.
            let frame_index;
            match self.vacant_slots.pop() {
                None => {
                    frame_index = self.evict_slot();
                }
                Some(index) => {
                    frame_index = index;
                }
            };

            // 3. Update buffer pool.
            self.buffer_pool[frame_index].data = Arc::new(data);
            self.buffer_pool_metadata[frame_index].page_id = Some(page_id);
            self.buffer_pool_metadata[frame_index].is_dirty = false;
            self.buffer_pool_lookup.insert(page_id, frame_index);

            // 4. Update cache with the item.
            self.cache_manager.add(&frame_index);

            // 5. Return the page.
            Ok(FrameHandler::new(
                &mut self.buffer_pool[frame_index],
                &mut self.buffer_pool_metadata[frame_index]
            ))
        }
    }

    // TODO: Add reference counting to prevent eviction of active pages
    fn evict_slot(&mut self) -> usize {
        let evicted_index = self.cache_manager.evict_cache();
        println!("Evicted Index: {}", evicted_index);

        // 1. Fetch evicted frame data and metadata.
        let evicted_frame = &self.buffer_pool[evicted_index];
        let evicted_frame_metadata = &mut self.buffer_pool_metadata[evicted_index];

        // 2. Fetch Page ID for the evicted slot.
        let evicted_page_id = evicted_frame_metadata.page_id.unwrap();

        // 3. Delete entry for that Page ID from buffer_pool_lookup.
        self.buffer_pool_lookup.remove(&evicted_page_id);

        // 4. Write entry to disk if the frame was dirty.
        // TODO: Make dirty check and write atomic to prevent race conditions
        if evicted_frame_metadata.is_dirty {
            self.disk_manager
                .write_page(&evicted_page_id, evicted_frame.data.deref())
                .expect("Failed to write to disk.");
            evicted_frame_metadata.is_dirty = false;
        }
        evicted_index
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
        
        let bpm = BufferPoolManager::new_with_path(16000, &test_file).unwrap();
        assert_eq!(bpm.capacity, 16000);
        assert_eq!(bpm.buffer_pool.len(), 2); // 16000 / 8000 = 2 slots
        assert_eq!(bpm.vacant_slots.len(), 2);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_get_page_cache_miss() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_miss");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferPoolManager::new_with_path(8000, &test_file).unwrap();
        let page_id = PageId::new(0);
        
        let result = bpm.get(page_id);
        assert!(result.is_ok());
        assert!(bpm.buffer_pool_lookup.contains_key(&page_id));
        assert_eq!(bpm.vacant_slots.len(), 0);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_get_page_cache_hit() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_hit");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferPoolManager::new_with_path(8000, &test_file).unwrap();
        let page_id = PageId::new(0);
        
        // First access - cache miss
        let _frame1 = bpm.get(page_id).unwrap();
        
        // Second access - cache hit
        let _frame2 = bpm.get(page_id).unwrap();
        
        assert!(bpm.buffer_pool_lookup.contains_key(&page_id));
        assert_eq!(bpm.vacant_slots.len(), 0);
        
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_eviction() {
        let temp_dir = env::temp_dir().join("rusty_kv_test_eviction");
        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.db");
        
        let mut bpm = BufferPoolManager::new_with_path(8000, &test_file).unwrap(); // Only 1 slot
        
        let page1 = PageId::new(0);
        let page2 = PageId::new(1);
        
        // Fill the buffer pool
        let _frame1 = bpm.get(page1).unwrap();
        assert!(bpm.buffer_pool_lookup.contains_key(&page1));
        
        // This should trigger eviction
        let _frame2 = bpm.get(page2).unwrap();
        assert!(bpm.buffer_pool_lookup.contains_key(&page2));
        assert!(!bpm.buffer_pool_lookup.contains_key(&page1));
        
        let _ = fs::remove_dir_all(&temp_dir);
    }
}