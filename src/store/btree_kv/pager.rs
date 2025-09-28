use std::{collections::HashMap};

const PAGE_SIZE: usize = 8000; // 8kb.

///
/// Defines the PageId of a Page of data in disk.
///
#[derive(Eq, Hash, PartialEq, Copy, Clone)]
pub struct PageId {
    id: u8,
}

///
/// Implements the methods to read from and write to files in disk.
/// 
pub struct Pager {
    data: HashMap<PageId, Vec<u8>>, // TODO: Currently an in-memory store. Update this to access File System.
}

impl Pager {
    ///
    /// Creates a new instance of Pager.
    /// 
    pub fn new() -> Self {
        Pager { 
            data: HashMap::new()
        }
    }

    ///
    /// Fetches a page from disk and returns it.
    /// 
    /// # Arguments
    /// * `id`: Page ID which needs to be fetched.
    /// 
    /// # Returns
    /// * `Option<&Vec<u8>>` containing the Page data.
    /// 
    pub fn read_page(&self, id: PageId) -> Option<&Vec<u8>> {
        self.data.get(&id)
    }

    ///
    /// Writes data to a Page.
    /// 
    /// # Arguments
    /// * `id`: Page ID of the Page.
    /// * `data`: Data that needs to be written.
    /// 
    pub fn write_page(&mut self, id: PageId, data: &[u8]) {
        if data.len() > PAGE_SIZE {
            // TODO: Handle this better.
            panic!("Page Limit Exceeded. Break data into smaller chunks.")
        }
        self.data.insert(id, Vec::from(data));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get() {
        let mut pager = Pager::new();

        let data = vec![10, 20, 30];
        let id = PageId{ id: 10 };

        pager.write_page(id, &data);
        assert_eq!(&data, pager.read_page(id).unwrap());
    }

    #[test]
    fn test_create_existing_key() {
        let mut pager = Pager::new();

        let data = vec![10, 20, 30];
        let id = PageId{ id: 10 };

        pager.write_page(id, &data);
        
        let new_data = vec![2, 10, 36];
        pager.write_page(id, &new_data);
        assert_eq!(&new_data, pager.read_page(id).unwrap());
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