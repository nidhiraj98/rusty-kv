use std::{collections::HashMap, sync::Arc};

use mockall::automock;

pub const PAGE_SIZE: usize = 8000; // 8kb.

///
/// Defines the PageId of a Page of data in disk.
///
#[derive(Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub struct PageId {
    id: u8,
}

impl PageId {
    ///
    /// Creates a new instance of PageId.
    /// 
    pub fn new(id: u8) -> Self {
        PageId { id }
    }
}

///
/// Implements the methods to read from and write to files in disk.
///
#[automock]
pub trait PagerTrait {
    ///
    /// Fetches a page from disk and returns it.
    /// 
    /// # Arguments
    /// * `id`: Page ID which needs to be fetched.
    /// 
    /// # Returns
    /// * `Option<&[u8; PAGE_SIZE]>` containing the Page data.
    /// 
    fn read_page(&self, id: PageId) -> Option<Arc<[u8; PAGE_SIZE]>>;
    ///
    /// Writes data to a Page.
    /// 
    /// # Arguments
    /// * `id`: Page ID of the Page.
    /// * `data`: Data that needs to be written.
    /// 
    fn write_page(&mut self, id: PageId, data: &[u8]);
}
 
pub struct Pager {
    data: HashMap<PageId, [u8; PAGE_SIZE]>, // TODO: Currently an in-memory store. Update this to access File System.
}

impl Pager {
    ///
    /// Creates and returns an instance.
    /// 
    pub fn new() -> Self {
        Pager { 
            data: HashMap::new()
        }
    }
}

impl PagerTrait for Pager {
    /// See [`PagerTrait::read_page`].
    fn read_page(&self, id: PageId) -> Option<Arc<[u8; PAGE_SIZE]>> {
        // Arc::new currently creates a copy of the data in the hashmap. This will be fixed once
        // we move to storing data on disk.
        self.data.get(&id).map(|p| Arc::new(*p))
    }

    /// See [`PagerTrait::write_page`].
    fn write_page(&mut self, id: PageId, data: &[u8]) {
        if data.len() > PAGE_SIZE {
            // TODO: Handle this better.
            panic!("Page Limit Exceeded. Break data into smaller chunks.")
        }

        // Create a copy
        let mut page_data = [0u8; PAGE_SIZE];
        page_data[..data.len()].copy_from_slice(data);

        self.data.insert(id, page_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get() {
        let mut pager = Pager::new();

        let data = [10, 20, 30];
        let mut page = [0u8; PAGE_SIZE];
        page[..data.len()].copy_from_slice(&data);
        
        let id = PageId{ id: 10 };

        pager.write_page(id, &page);
        assert_eq!(&page, pager.read_page(id).unwrap().as_ref());
    }

    #[test]
    fn test_create_existing_key() {
        let mut pager = Pager::new();

        let data = [10, 20, 30];
        let mut page = [0u8; PAGE_SIZE];
        page[..data.len()].copy_from_slice(&data);

        let id = PageId{ id: 10 };

        pager.write_page(id, &page);
        
        let new_data = [2, 10, 36];
        let mut new_page = [0u8; PAGE_SIZE];
        new_page[..new_data.len()].copy_from_slice(&new_data);

        pager.write_page(id, &new_page);
        assert_eq!(&new_page, pager.read_page(id).unwrap().as_ref());
    }

    #[test]
    #[should_panic = "Page Limit Exceeded. Break data into smaller chunks."]
    fn test_create_large_page() {
        let mut pager = Pager::new();

        let data: Vec<u8> = vec![10; PAGE_SIZE + 1];
        let id = PageId{ id: 10 };

        pager.write_page(id, &data);
    }
}