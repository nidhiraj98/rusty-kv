use std::collections::{HashMap, VecDeque};

use crate::store::btree_kv::pager::{PageId, PagerTrait, PAGE_SIZE};

const SLOT_COUNT: usize = 10;
const BUFFER_POOL_SIZE: usize = SLOT_COUNT * PAGE_SIZE;

///
/// Metadata for a frame in buffer pool.
///
#[derive(Clone, Copy)]
struct FrameMetadata {
    ///
    /// ID of the Page stored in the Frame.
    /// 
    page_id: Option<PageId>,
    ///
    /// Indicates if the data has been modified after being read from the disk.
    /// true if it has been modified; false otherwise.
    ///
    is_dirty: bool,
}

impl Default for FrameMetadata {
    fn default() -> Self {
        FrameMetadata {
            page_id: None,
            is_dirty: false
        }
    }
}

///
/// Buffer Pool for the KV store. Maintains frames in memory for quick access.
/// 
struct BufferPoolManager<P: PagerTrait> {
    ///
    /// Frames of data for quick access. The data is stored as a contiguous
    /// array of pages.
    /// 
    frames: [u8; BUFFER_POOL_SIZE],
    ///
    /// Metadata for frames.
    /// 
    frame_metadata: [FrameMetadata; SLOT_COUNT],
    ///
    /// Lookup between PageID and slot index in the list of frames.
    /// 
    lookup: HashMap<PageId, usize>,
    ///
    /// Array of vacant frames.
    ///
    vacant_frames: Vec<usize>,
    ///
    /// Priority order for evicting frames.
    /// The eviction policy evicts the least recently used frame index.
    /// 
    lru_frames: VecDeque<usize>,
    ///
    /// Instance of Pager which implements methods for accessing the disk.
    /// 
    pager: P,
}

impl<P: PagerTrait> BufferPoolManager<P> {
    ///
    /// Constructor.
    /// 
    pub(crate) fn new(pager: P) -> Self {
        BufferPoolManager { 
            frames: [0u8; BUFFER_POOL_SIZE],
            frame_metadata: [FrameMetadata::default(); SLOT_COUNT],
            lookup: HashMap::new(),
            vacant_frames: (0..SLOT_COUNT).collect(),
            lru_frames: VecDeque::new(),
            pager,
        }
    }

    ///
    /// Returns a Page.
    /// 
    /// # Arguments
    /// * `id`: Page ID of the page to be fetched.
    /// 
    /// # Returns
    /// * `&[u8]`: Reference to the Page Data.
    /// 
    pub(crate) fn get_page(&mut self, id: &PageId) -> &[u8; PAGE_SIZE] {
        let frame_index = if let Some(&lookup_index) = self.lookup.get(id) {
            self.touch_frame(lookup_index);
            lookup_index
        } else {
            // Fetch data from disk.
            let page_data = self.pager.read_page(*id)
                .unwrap_or_else(|| panic!("Page ID doesn't exist."));

            // Allocate a frame for the page.
            self.allocate_frame(*id, &*page_data)
        };

        self.frames[frame_index * PAGE_SIZE..(frame_index + 1) * PAGE_SIZE]
            .try_into()
            .expect("Slice with incorrect length")
    }

    ///
    /// Updates the LRU array to indicate a frame was accessed.
    /// 
    /// # Arguments
    /// * `frame_index`: Index of the frame that was touched.
    /// 
    fn touch_frame(&mut self, frame_index: usize) {
        if let Some(pos) = self.lru_frames.iter().position(|&i| i == frame_index) {
            // Remove item from the queue.
            self.lru_frames.remove(pos);
        }
        // Push Frame Index to the front.
        self.lru_frames.push_front(frame_index);

    }

    ///
    /// Allocates a frame for the Page Data and returns it's index.
    /// 
    /// # Arguments
    /// * `id`: Page ID of the Page.
    /// * `data`: Page Data to be added to the frame.
    /// 
    /// # Returns
    /// * `usize`: Frame Index of the allocated frame.
    /// 
    fn allocate_frame(&mut self, id: PageId, data: &[u8; PAGE_SIZE]) -> usize {
        let vacant_index = self.vacant_frames.pop().unwrap_or_else(|| {
            // No space in Buffer Pool. Evict a frame.
            self.evict_lru_frame()
        });

        // Update frame data.
        let frame = &mut self.frames[vacant_index * PAGE_SIZE..(vacant_index + 1) * PAGE_SIZE];
        frame.copy_from_slice(&data[..]);

        // Update frame metadata
        self.frame_metadata[vacant_index].is_dirty = false;
        self.frame_metadata[vacant_index].page_id = Some(id);

        // Add frame to lookup.
        self.lookup.insert(id, vacant_index);

        // Update LRU
        self.touch_frame(vacant_index);

        vacant_index
    }

    ///
    /// Evicts the least recently used frame.
    /// 
    /// # Returns
    /// * `usize`: Index of the frame that was evicted.
    /// 
    fn evict_lru_frame(&mut self) -> usize {
        let evicted_index = self.lru_frames.pop_back()
            .unwrap_or_else(|| panic!("Nothing to evict."));

        let evicted_frame_metadata = &mut self.frame_metadata[evicted_index];
        if evicted_frame_metadata.is_dirty {
            // Evicted index is dirty.
            let evicted_frame = &mut self.frames[evicted_index * PAGE_SIZE..(evicted_index + 1) * PAGE_SIZE];
            self.pager.write_page(evicted_frame_metadata.page_id.unwrap(), &evicted_frame[..]);
        }

        // Remove Page ID from lookup.
        self.lookup.remove(&evicted_frame_metadata.page_id.unwrap());

        // Reset the evicted metadata.
        evicted_frame_metadata.page_id = None;
        evicted_frame_metadata.is_dirty = false;

        evicted_index
    }

    ///
    /// Checks if a Page is present in lookup.
    /// 
    /// Used only in tests.
    /// 
    /// # Arguments
    /// * `page_id`: Page ID of the page to check.
    /// 
    /// # Returns
    /// * `bool`: true if the Page ID is present in lookup; false, otherwise.
    /// 
    #[cfg(test)]
    fn is_page_in_lookup(&self, page_id: &PageId) -> bool {
        self.lookup.contains_key(page_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::btree_kv::pager::MockPagerTrait;
    use std::sync::Arc;

    use super::*;
    use mockall::predicate::*;

    #[test]
    fn test_get_page_from_disk() {
        // Setup test data
        let test_id = PageId::new(1);
        let test_data = [0u8; PAGE_SIZE];
        let test_data_clone = Arc::new(test_data);

        // Setup mock pager
        let mut mock_pager = MockPagerTrait::new();
        mock_pager.expect_read_page()
                    .with(eq(test_id))
                    .return_once(move |_id| Some(Arc::clone(&test_data_clone)));

        let mut buffer_pool = BufferPoolManager::new(mock_pager);

        // Fetch page from buffer pool.
        let actual_data= buffer_pool.get_page(&test_id);

        // Assert that the right data is returned.
        assert_eq!(*actual_data, test_data);
    }

    #[test]
    fn test_get_page_from_memory() {
        // Setup test data
        let test_id = PageId::new(1);
        let test_data = [0u8; PAGE_SIZE];
        let test_data_clone = Arc::new(test_data);

        // Setup mock pager
        let mut mock_pager = MockPagerTrait::new();

        // read_page should only be called once, first get_page call.
        mock_pager.expect_read_page()
                    .times(1)
                    .with(eq(test_id))
                    .returning(move |_id| Some(Arc::clone(&test_data_clone)));

        let mut buffer_pool = BufferPoolManager::new(mock_pager);

        // Fetch page from buffer pool.
        let actual_data = buffer_pool.get_page(&test_id);

        // Assert that the right data is returned.
        assert_eq!(*actual_data, test_data);

        // Fetch page from buffer pool, again.
        let actual_data = buffer_pool.get_page(&test_id);

        // Assert that the right data is returned.
        assert_eq!(*actual_data, test_data);
    }

    #[test]
    fn test_get_page_evicts_lru() {
        // Setup test data
        let test_id = PageId::new(SLOT_COUNT as u8);
        let test_data = Arc::new([0u8; PAGE_SIZE]);
        let test_data_clone = Arc::clone(&test_data);

        // Setup mock pager
        let mut mock_pager = MockPagerTrait::new();

        // read_page should only be called once, first get_page call.
        mock_pager.expect_read_page()
                    .with(always())
                    .returning(move |_id| Some(Arc::clone(&test_data_clone)));

        let mut buffer_pool = BufferPoolManager::new(mock_pager);

        // Add test_id to buffer pool
        buffer_pool.get_page(&test_id);

        // Fill-up Buffer Pool
        for id in 1..SLOT_COUNT {
            buffer_pool.get_page(&PageId::new(id as u8));
        }

        // Verify test page exists in lookup.
        buffer_pool.is_page_in_lookup(&test_id);

        // Add another entry to buffer pool.
        let test_overflow_id = PageId::new((SLOT_COUNT + 1) as u8);
        buffer_pool.get_page(&test_overflow_id);

        // The first page should be removed from lookup.
        assert_eq!(buffer_pool.is_page_in_lookup(&test_id), false);
    }
}