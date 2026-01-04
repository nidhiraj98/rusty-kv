use crate::store::btree_kv::commons::{PAGE_SIZE, PageId};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

///
/// Handles disk operations for the data.
///
/// TODO: Implement deallocation and reuse them.
///
pub struct DiskManager {
    file: File,
    num_pages: usize,
}

impl DiskManager {
    ///
    /// Creates and returns an instance of DiskManager.
    ///
    /// # Arguments
    /// * `path`: Path to the file that can be used for storing data.
    ///
    /// # Returns
    /// * `Ok(Self)` if the disk manager was initialised successfully.
    /// * `Err(std::io::Error)` if an error occurred while reading the file.
    ///
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;

        // Arrive at num_pages based on the current size of the file to prevent
        // overwriting it.
        let num_pages = (metadata.len() / PAGE_SIZE as u64) as usize;

        Ok(Self {
            file: file,
            num_pages: num_pages,
        })
    }

    ///
    /// Fetches a page from disk and populates the buffer.
    ///
    /// # Arguments
    /// * `id`: Page ID which needs to be fetched.
    /// * `data`: Buffer that needs to be populated.
    ///
    /// # Returns
    /// * `Ok(())` if the page was successfully read.
    /// * `Err(std::io::Error)` if an error occurred while reading from the disk.
    ///
    /// # Errors
    /// This function returns an error if:
    /// * The provided buffer length does not match the page size.
    /// * The underlying file I/O operation fails.
    ///
    pub fn read_page(
        &mut self,
        id: &PageId,
        buffer: &mut [u8; PAGE_SIZE],
    ) -> Result<(), std::io::Error> {
        let offset = id.value() * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut bytes_read = 0;
        while bytes_read < PAGE_SIZE {
            match self.file.read(&mut buffer[bytes_read..])? {
                0 => {
                    buffer[bytes_read..].fill(0);
                    break;
                }
                n => bytes_read += n,
            }
        }
        Ok(())
    }

    ///
    /// Writes data to a Page.
    ///
    /// # Arguments
    /// * `id`: Page ID of the Page.
    /// * `data`: Data that needs to be written.
    ///
    /// # Returns
    /// * `Ok(())` if the page was successfully written.
    /// * `Err(std::io::Error)` if an error occurred while writing to the disk.
    ///
    /// # Errors
    /// This function returns an error if:
    /// * The provided data length does not match the page size.
    /// * The underlying file I/O operation fails.
    ///
    pub fn write_page(
        &mut self,
        id: &PageId,
        buffer: &[u8; PAGE_SIZE],
    ) -> Result<(), std::io::Error> {
        let offset = id.value() * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buffer)?;
        self.file.flush()?;
        Ok(())
    }

    ///
    /// Allocates a Page of data in the file.
    ///
    /// # Returns
    /// * `PageId`: The PageID of the page allocated.
    ///
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = PageId::new(self.num_pages as u64);
        self.num_pages += 1;
        page_id
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_create_and_get() {
        let temp_file = NamedTempFile::new().unwrap();

        let mut disk_manager = DiskManager::new(temp_file.path()).unwrap();

        let data = [10, 20, 30];
        let mut page = [0u8; PAGE_SIZE];
        page[..data.len()].copy_from_slice(&data);

        let id = disk_manager.allocate_page();

        disk_manager.write_page(&id, &page).unwrap();
        let mut data_read = [0u8; PAGE_SIZE];
        disk_manager.read_page(&id, &mut data_read).unwrap();
        assert_eq!(&page, &data_read);
    }

    #[test]
    fn test_create_existing_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut disk_manager = DiskManager::new(temp_file.path()).unwrap();

        let data = [10, 20, 30];
        let mut page = [0u8; PAGE_SIZE];
        page[..data.len()].copy_from_slice(&data);

        let id = disk_manager.allocate_page();

        disk_manager.write_page(&id, &page).unwrap();

        let new_data = [2, 10, 36];
        let mut new_page = [0u8; PAGE_SIZE];
        new_page[..new_data.len()].copy_from_slice(&new_data);

        disk_manager.write_page(&id, &new_page).unwrap();

        let mut data_read = [0u8; PAGE_SIZE];
        disk_manager.read_page(&id, &mut data_read).unwrap();
        assert_eq!(&new_page, &data_read);
    }
}
