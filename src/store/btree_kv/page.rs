use crate::store::btree_kv::commons::PAGE_SIZE;
use crate::store::btree_kv::error::RustyKVError;
use crate::store::btree_kv::helpers::byte_ordering::cmp_le_bytes;
use std::cmp::Ordering;
use std::mem::size_of;
// TODO: Replace unwrap() with proper error handling.

// Header Sizes
const SLOT_COUNT_SIZE: usize = size_of::<u16>(); // 2 bytes
const SLOT_COUNT_OFFSET: usize = 0;
const PAGE_HEADER_SIZE: usize = SLOT_COUNT_SIZE;

// Data Sizes

// BTree Row Constants
const KEY_SIZE_SIZE: usize = size_of::<u16>(); // 2 bytes
const VALUE_SIZE_SIZE: usize = size_of::<u16>(); // 2 bytes
const ROW_HEADER_SIZE: usize = KEY_SIZE_SIZE + VALUE_SIZE_SIZE;
const PAGE_BODY_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

// Slot Map Sizes
const SLOT_MAP_ELEMENT_SIZE: usize = size_of::<u16>(); // 2 bytes

///
/// Header of the BTree Page.
///
struct BTreePageHeader<'a> {
    ///
    /// Bytes containing the BTree Page Header.
    ///
    data: &'a mut [u8],
}

impl<'a> BTreePageHeader<'a> {
    ///
    /// Creates an instance of BTreePageHeader.
    /// # Arguments:
    /// * `data`: byte array containing the header. The byte array needs to be exactly
    ///           PAGE_HEADER_SIZE long.
    /// # Returns:
    /// * `Self`: Returns an instance of BTreePageHeader.
    ///
    pub fn from(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len(), PAGE_HEADER_SIZE);
        Self { data }
    }

    ///
    /// Returns the number of slots in the BTree.
    /// # Returns:
    /// * `u16`: Number of slots in the BTree.
    pub fn get_slot_count(&self) -> u16 {
        u16::from_le_bytes(
            (&self.data[SLOT_COUNT_OFFSET..SLOT_COUNT_OFFSET + SLOT_COUNT_SIZE])
                .try_into()
                .unwrap(),
        )
    }

    ///
    /// Updates the number of slots in the BTree.
    /// # Arguments:
    /// * `count`: The updated number of slots.
    ///
    pub fn set_slot_count(&mut self, count: u16) {
        self.data[SLOT_COUNT_OFFSET..SLOT_COUNT_OFFSET + SLOT_COUNT_SIZE]
            .copy_from_slice(&count.to_le_bytes());
    }

    ///
    /// Increases the slot count by a fixed amount.
    /// # Arguments:
    /// * `increase_count`: The increment to increase the slot count by.
    ///
    pub fn increase_slot_count(&mut self, increase_count: u16) {
        let current_count = self.get_slot_count();
        self.set_slot_count(current_count + increase_count);
    }

    ///
    /// Decreases the slot count by a fixed amount.
    /// # Arguments:
    /// * `decrease_count`: The decrement to decrease the slot count by.
    ///
    pub fn decrease_slot_count(&mut self, decrease_count: u16) {
        let current_count = self.get_slot_count();
        self.set_slot_count(current_count - decrease_count);
    }
}

#[cfg(test)]
mod tests_page_header {
    use super::*;
    #[test]
    fn test_page_header_from() {
        let mut frame = [0u8; PAGE_SIZE];
        frame[0..2].copy_from_slice([10u8, 0u8].as_ref());

        let header = BTreePageHeader::from((&mut frame[0..PAGE_HEADER_SIZE]).try_into().unwrap());

        // Header values from BTreePageHeader view
        assert_eq!(header.get_slot_count(), 10);
    }

    #[test]
    fn test_page_header_get_slot_count() {
        let mut frame = [0u8; PAGE_SIZE];
        frame[0..2].copy_from_slice([10u8, 0u8].as_ref());
        let header = BTreePageHeader::from((&mut frame[0..PAGE_HEADER_SIZE]).try_into().unwrap());
        assert_eq!(header.get_slot_count(), 10);
    }

    #[test]
    fn test_page_header_set_slot_count() {
        let mut frame = [0u8; PAGE_SIZE];
        frame[0..2].copy_from_slice([10u8, 0u8].as_ref());
        let mut header =
            BTreePageHeader::from((&mut frame[0..PAGE_HEADER_SIZE]).try_into().unwrap());
        assert_eq!(header.get_slot_count(), 10);
        header.set_slot_count(20);
        assert_eq!(header.get_slot_count(), 20);
    }

    #[test]
    fn test_page_header_increase_slot_count() {
        let mut frame = [0u8; PAGE_SIZE];
        frame[0..2].copy_from_slice([10u8, 0u8].as_ref());
        let mut header =
            BTreePageHeader::from((&mut frame[0..PAGE_HEADER_SIZE]).try_into().unwrap());
        assert_eq!(header.get_slot_count(), 10);
        header.increase_slot_count(20);
        assert_eq!(header.get_slot_count(), 30);
    }
}

///
/// View representing a B-Tree row.
///
struct BTreeRow {
    offset: usize,
}

impl BTreeRow {
    const KEY_SIZE_OFFSET: usize = 0;
    const VALUE_SIZE_OFFSET: usize = Self::KEY_SIZE_OFFSET + KEY_SIZE_SIZE;

    ///
    /// Creates an instance of B-Tree row.
    /// # Arguments:
    /// * `offset`: Offset for the first element in the row.
    ///
    pub fn from(offset: usize) -> Self {
        Self { offset }
    }

    ///
    /// Fetches the size of the key stored in the row.
    /// # Arguments:
    /// * `data`: Byte array containing the row header bytes. The byte array should be
    ///           at least ROW_HEADER_SIZE long.
    /// # Returns:
    /// * `usize`: Size of the key.
    ///
    pub fn get_key_size(&self, data: &[u8]) -> usize {
        assert!(self.offset + Self::KEY_SIZE_OFFSET + KEY_SIZE_SIZE <= data.len());
        u16::from_le_bytes(
            data[self.offset + Self::KEY_SIZE_OFFSET
                ..self.offset + Self::KEY_SIZE_OFFSET + KEY_SIZE_SIZE]
                .try_into()
                .unwrap(),
        ) as usize
    }

    ///
    /// Sets the size of the key in the header.
    /// # Arguments:
    /// * `data`: Byte array containing the row header bytes. The byte array should be
    ///           atleast ROW_HEADER_SIZE long.
    /// * `key_size`: Size of the key to be set on the header.
    ///
    fn set_key_size(&mut self, key_size: u16, data: &mut [u8]) {
        assert!(self.offset + Self::KEY_SIZE_OFFSET + KEY_SIZE_SIZE <= data.len());
        data[self.offset + Self::KEY_SIZE_OFFSET
            ..self.offset + Self::KEY_SIZE_OFFSET + KEY_SIZE_SIZE]
            .copy_from_slice(&key_size.to_le_bytes());
    }

    ///
    /// Fetches the size of the value stored in the row.
    /// # Arguments:
    /// * `data`: Byte array containing the row header bytes. The byte array should be
    ///           atleast ROW_HEADER_SIZE long.
    /// # Returns:
    /// * `usize`: Size of the value.
    ///
    pub fn get_value_size(&self, data: &[u8]) -> usize {
        assert!(self.offset + Self::VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE <= data.len());
        u16::from_le_bytes(
            data[self.offset + Self::VALUE_SIZE_OFFSET
                ..self.offset + Self::VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE]
                .try_into()
                .unwrap(),
        ) as usize
    }

    ///
    /// Sets the size of the value in the header.
    /// # Arguments:
    /// * `data`: Byte array containing the row header bytes. The byte array should be
    ///           atleast ROW_HEADER_SIZE long.
    /// * `value_size`: Size of the value to be set on the header.
    ///
    fn set_value_size(&mut self, value_size: u16, data: &mut [u8]) {
        assert!(self.offset + Self::VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE <= data.len());
        data[self.offset + Self::VALUE_SIZE_OFFSET
            ..self.offset + Self::VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE]
            .copy_from_slice(&value_size.to_le_bytes());
    }

    ///
    /// Fetches the bytes representing the key in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// # Returns:
    /// * `&[u8]`: Byte array representing the key.
    ///
    pub fn get_key<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let key_size = self.get_key_size(data);
        assert!(self.offset + ROW_HEADER_SIZE + key_size <= data.len());
        &data[self.offset + ROW_HEADER_SIZE..self.offset + ROW_HEADER_SIZE + key_size]
    }

    ///
    /// Sets the key in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// * `key`: A byte array representing the key to be set in the row.
    ///
    pub fn set_key(&mut self, key: &[u8], data: &mut [u8]) {
        let key_size = key.len();
        assert!(self.offset + PAGE_HEADER_SIZE + key_size <= data.len());
        self.set_key_size(key_size as u16, data);
        data[self.offset + ROW_HEADER_SIZE..self.offset + ROW_HEADER_SIZE + key_size]
            .copy_from_slice(key);
    }

    ///
    /// Fetches the bytes representing the value in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// # Returns:
    /// * `&[u8]`: Byte array representing the value.
    ///
    pub fn get_value<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let key_size = self.get_key_size(data);
        let value_size = self.get_value_size(data);
        assert!(self.offset + ROW_HEADER_SIZE + key_size + value_size <= data.len());
        &data[self.offset + ROW_HEADER_SIZE + key_size
            ..self.offset + ROW_HEADER_SIZE + key_size + value_size]
    }

    ///
    /// Sets the value in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// * `value`: A byte array representing the value to be set in the row.
    ///
    pub fn set_value(&mut self, value: &[u8], data: &mut [u8]) {
        let key_size = self.get_key_size(data);
        let value_size = value.len();
        assert!(self.offset + ROW_HEADER_SIZE + key_size + value_size <= data.len());
        self.set_value_size(value_size as u16, data);
        data[self.offset + ROW_HEADER_SIZE + key_size
            ..self.offset + ROW_HEADER_SIZE + key_size + value_size]
            .copy_from_slice(value);
    }

    ///
    /// Fetches the slot size of the data array.
    /// # Arguments:
    /// * `data`: Byte array representing the row.
    /// # Returns:
    /// * `usize`: Size of the data stored in the row.
    ///
    pub(crate) fn get_size(&self, data: &[u8]) -> usize {
        self.get_key_size(data) + self.get_value_size(data) + ROW_HEADER_SIZE
    }

    ///
    /// Clears all the contents in the row.
    /// # Arguments:
    /// * `data`: A reference to the BTree Page data.
    ///
    pub(crate) fn clear_row(&mut self, data: &mut [u8]) {
        let slot_size = self.get_size(data);
        data[self.offset..self.offset + slot_size].fill(0);
    }
}

#[cfg(test)]
mod tests_row {
    use super::*;

    #[test]
    fn test_row_updates_in_place() {
        const KEY: [u8; 2] = 15u16.to_le_bytes();
        const VALUE: [u8; 2] = 20u16.to_le_bytes();

        let mut row = [0u8; KEY.len() + VALUE.len() + ROW_HEADER_SIZE];
        let mut btree_row = BTreeRow::from(0);

        btree_row.set_key(&KEY, &mut row);
        btree_row.set_value(&VALUE, &mut row);

        // Value from the view
        assert_eq!(btree_row.get_key_size(&row), KEY.len());
        assert_eq!(btree_row.get_value_size(&row), VALUE.len());
        assert_eq!(btree_row.get_key(&row), KEY);
        assert_eq!(btree_row.get_value(&row), VALUE);

        // Value from the byte array
        assert_eq!(
            u16::from_le_bytes(
                (&row[BTreeRow::KEY_SIZE_OFFSET..BTreeRow::KEY_SIZE_OFFSET + KEY_SIZE_SIZE])
                    .try_into()
                    .unwrap()
            ),
            KEY.len() as u16
        );
        assert_eq!(
            u16::from_le_bytes(
                (&row[BTreeRow::VALUE_SIZE_OFFSET..BTreeRow::VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE])
                    .try_into()
                    .unwrap()
            ),
            VALUE.len() as u16
        );
        assert_eq!(&row[ROW_HEADER_SIZE..ROW_HEADER_SIZE + KEY.len()], KEY);
        assert_eq!(
            &row[ROW_HEADER_SIZE + KEY.len()..ROW_HEADER_SIZE + KEY.len() + VALUE.len()],
            VALUE
        );
    }
}

///
/// View representing the slot map in the page.
///
struct BTreePageSlotMap {
    start: usize,
}

impl BTreePageSlotMap {
    ///
    /// Creates an instance of the slot map.
    /// # Arguments:
    /// * `start`: The offset in the page body from which the slot map starts.
    /// # Returns:
    /// * `Self`: Instance of BTreePageSlotMap.
    /// # Impl Note:
    /// Page Body refers to the entire page without the page header.
    ///
    pub fn from(start: usize) -> Self {
        BTreePageSlotMap { start }
    }

    ///
    /// Inserts an element in the slot map.
    /// # Arguments
    /// * `free_space`: A view of the free space.
    /// * `data`: Data array representing the body.
    /// * `element`: The slot map element to be inserted.
    /// * `index`: The index at which the slot map element should be inserted.
    ///
    pub fn insert_slot_element(
        &mut self,
        free_space: &mut BTreePageFreeSpace,
        data: &mut [u8],
        element: u16,
        index: usize,
    ) {
        // Allocate free space for the new element.
        let (slot_map_start, _) = free_space.allocate_slot_map_space();
        self.start = slot_map_start;

        // Shift all the bytes till the insertion point to the left.
        for i in self.start..self.start + (index * SLOT_MAP_ELEMENT_SIZE) {
            data[i] = data[i + SLOT_MAP_ELEMENT_SIZE];
        }

        let start_offset = self.start + (SLOT_MAP_ELEMENT_SIZE * index);
        data[start_offset..start_offset + SLOT_MAP_ELEMENT_SIZE]
            .copy_from_slice(element.to_le_bytes().as_ref());
    }

    ///
    /// Returns the bytes of the slot map element at the given index.
    /// # Arguments:
    /// * `index`: Index of the element in the slot map.
    /// * `data`: Byte array representing the page body.
    /// # Returns:
    /// * `&[u8]`: Bytes of the element in the index.
    ///
    pub fn get_slot_map_element<'a>(&self, index: usize, data: &'a [u8]) -> &'a [u8] {
        let slot_map_offset = self.start + (SLOT_MAP_ELEMENT_SIZE * index);
        assert!(slot_map_offset + SLOT_MAP_ELEMENT_SIZE <= data.len());
        &data[slot_map_offset..slot_map_offset + SLOT_MAP_ELEMENT_SIZE]
    }

    ///
    /// Deletes an entry from the slot map.
    ///
    /// # Arguments:
    /// * `index`: The index of the slot map element to be deleted.
    /// * `data`: A mutable reference to the page data.
    /// * `free_space`: A mutable reference to the free space view.
    ///
    pub fn delete_slot_map_element(
        &mut self,
        index: usize,
        data: &mut [u8],
        free_space: &mut BTreePageFreeSpace,
    ) {
        let new_start = self.start + SLOT_MAP_ELEMENT_SIZE;
        // 1. Shift the elements right by one index.
        for i in (new_start..self.start + (index * SLOT_MAP_ELEMENT_SIZE)).rev() {
            data[i + SLOT_MAP_ELEMENT_SIZE] = data[i];
        }

        // 2. Update the slot map start to the new start.
        self.start = new_start;

        // 3. Increase the free space.
        free_space.deallocate_slot_map_space();
    }
}

///
/// View representing the free space in the page.
///
struct BTreePageFreeSpace {
    start: usize,
    end: usize,
}

impl BTreePageFreeSpace {
    ///
    /// Creates an instance of the free space.
    /// # Arguments:
    /// * `start`: The offset in the page body from which the free space starts.
    /// * `end`: The offset in the page body from which the free space ends.
    /// # Returns:
    /// * `Self`: Instance of BTreePageFreeSpace.
    /// # Impl Note:
    /// Page Body refers to the entire page without the page header.
    ///
    pub fn from(start: usize, end: usize) -> Self {
        BTreePageFreeSpace { start, end }
    }

    ///
    /// Allocates space for a new row in the free space.
    /// # Arguments:
    /// * `size`: Bytes of data to be allocated.
    /// # Returns:
    /// `(usize, usize)`: The start and end of the space allocated for the new row.
    /// # Impl Note:
    /// Free space for a row is allocated at the start of the free space slice.
    ///
    pub fn allocate_row_space(&mut self, size: usize) -> (usize, usize) {
        assert!(self.start + size <= self.end); // Check that there's free space.

        let allocated_space = (self.start, self.start + size);
        self.start += size;
        allocated_space
    }

    ///
    /// Allocates space for a new slot map element in the free space.
    /// # Returns:
    /// `(usize, usize)`: The start and end of the space allocated for the new slot map element.
    /// # Impl Note:
    /// Free space for a slot map element is allocated at the end of the free space slice.
    ///
    pub fn allocate_slot_map_space(&mut self) -> (usize, usize) {
        assert!(self.start + SLOT_MAP_ELEMENT_SIZE <= self.end);

        let allocated_space = (self.end - SLOT_MAP_ELEMENT_SIZE, self.end);
        self.end -= SLOT_MAP_ELEMENT_SIZE;
        allocated_space
    }

    ///
    /// Reclaims a space from slot map.
    ///
    pub fn deallocate_slot_map_space(&mut self) {
        self.end += SLOT_MAP_ELEMENT_SIZE;
    }

    ///
    /// Returns the free space available in the page.
    /// # Returns:
    /// * `usize`: Free space available in the page.
    ///
    pub fn get_size(&self) -> usize {
        self.end - self.start
    }
}

///
/// View representing the body of the page.
///
struct BTreeBodyData<'a> {
    ///
    /// Byte array representing the body of the page.
    ///
    data: &'a mut [u8],
    ///
    /// View representing the free space in the page.
    ///
    free_space: BTreePageFreeSpace,
    ///
    /// View representing the slot map in the page.
    ///
    slot_map: BTreePageSlotMap,
}

// TODO: Create a model for errors instead of returning error messages directly.
impl<'a> BTreeBodyData<'a> {
    ///
    /// Creates an instance of BTree Body.
    /// # Arguments:
    /// * `data`: Byte array representing the body of the data. The byte array should be exactly
    ///           PAGE_BODY_SIZE long.
    /// * `header`: A view of the header corresponding to the body.
    /// # Returns:
    /// `Self`: An instance of BTreeBodyData.
    ///
    pub fn from(data: &'a mut [u8], header: &BTreePageHeader) -> Self {
        assert_eq!(data.len(), PAGE_BODY_SIZE);
        assert!(header.get_slot_count() as usize * SLOT_MAP_ELEMENT_SIZE <= PAGE_BODY_SIZE);

        let slot_map_start =
            PAGE_BODY_SIZE - (header.get_slot_count() as usize * SLOT_MAP_ELEMENT_SIZE);

        let mut count = 0;
        let mut index = 0;

        while count < header.get_slot_count() as usize {
            let btree_row = BTreeRow::from(index);
            index += btree_row.get_size(data);
            count += 1;
        }

        let free_space = BTreePageFreeSpace::from(index, slot_map_start);
        let slot_map = BTreePageSlotMap::from(slot_map_start);

        BTreeBodyData {
            data,
            free_space,
            slot_map,
        }
    }

    ///
    /// Fetches the row corresponding to the key.
    /// # Arguments:
    /// * `key`: Key of the row to be fetched.
    /// * `header`: Header of the BTree page.
    /// # Returns:
    /// * `Result<&[u8], String>`: Result containing the row data if found. If not, the reason.
    ///
    pub(crate) fn get(&self, key: &[u8], header: &BTreePageHeader) -> Result<&[u8], RustyKVError> {
        match self.search(key, 0, header.get_slot_count() as usize) {
            Ok(index) => {
                let row_offset = u16::from_le_bytes(
                    self.slot_map
                        .get_slot_map_element(index, &self.data)
                        .try_into()
                        .unwrap(),
                ) as usize;
                let btree_row = BTreeRow::from(row_offset);
                let slot_size = btree_row.get_size(self.data);
                Ok(&self.data[row_offset..row_offset + slot_size])
            }
            Err(_) => Err(RustyKVError::ItemNotFound),
        }
    }

    ///
    /// Updates the row corresponding to the key, with a new value.
    /// # Arguments:
    /// * `value`: Value to be updated
    /// * `slot_map_index`: Index of the slot_map element which points to the row.
    /// # Returns:
    /// * `Result<(), String>`: Void result if the updation was successful. Reason otherwise.
    /// # Impl Note:
    ///   If the value doesn't match the size of the existing value present in the row,
    ///   the updation will be unsuccessful, with a corresponding error.
    ///
    /// TODO: Make this safer. It may lead to us performing a search again to validate, but probably
    ///       worth it? It also improves the method signature. Passing the slot_map_index isn't
    ///       ideal.
    ///
    pub(crate) fn update(
        &mut self,
        value: &[u8],
        slot_map_index: usize,
    ) -> Result<(), RustyKVError> {
        let row_offset = u16::from_le_bytes(
            self.slot_map
                .get_slot_map_element(slot_map_index, self.data)
                .try_into()
                .unwrap(),
        ) as usize;
        let mut btree_row = BTreeRow::from(row_offset);
        let value_size = btree_row.get_value_size(self.data);

        // Validate that the new value fits in the existing space.
        // TODO: If the new value is smaller, we can fit in the new value and update the value
        //       size in the header.
        if value_size != value.len() {
            return Err(RustyKVError::InsufficientSpace);
        }

        // Re-Use the existing slot.
        btree_row.set_value(value, self.data);
        Ok(())
    }

    ///
    /// Inserts a new key-value pair in the page body.
    /// # Arguments:
    /// * `key`: Key to be inserted.
    /// * `value`: Value to be inserted.
    /// * `slot_map_index`: The index of the slot map element in the slot map where the new offset
    ///                     can be inserted.
    /// # Returns:
    /// * `Result<(), String>`: Void result if the insertion was successful. Reason otherwise.
    ///
    pub(crate) fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        slot_map_index: usize,
    ) -> Result<(), RustyKVError> {
        // Insert element in row data.
        let key_size = key.len();
        let value_size = value.len();
        let slot_size = ROW_HEADER_SIZE + key_size + value_size;

        // Each slot needs to store the data and also an element in the slot map.
        // TODO: Move this check to allocate_row_space
        if slot_size + SLOT_MAP_ELEMENT_SIZE > self.free_space.get_size() {
            return Err(RustyKVError::InsufficientSpace);
        }

        let (new_row_start, _) = self.free_space.allocate_row_space(slot_size);
        let mut btree_row = BTreeRow::from(new_row_start);
        btree_row.set_key(key, self.data);
        btree_row.set_value(value, self.data);

        // Insert offset in slot map
        self.slot_map.insert_slot_element(
            &mut self.free_space,
            &mut self.data,
            new_row_start as u16,
            slot_map_index,
        );

        Ok(())
    }

    ///
    /// Removes a key from the BTree Page.
    /// # Arguments:
    /// * `header`: A reference to the Page header for this page.
    /// * `slot_map_index`: The index of the element to delete in the slot map.
    ///
    /// # Returns:
    /// * `Result<(), String>`: Void if the item was successfully deleted. Reason otherwise.
    ///
    pub(crate) fn remove(
        &mut self,
        header: &mut BTreePageHeader,
        slot_map_index: usize,
    ) -> Result<(), RustyKVError> {
        // 1. Find the row offset of the entry.
        let row_offset = u16::from_le_bytes(
            self.slot_map
                .get_slot_map_element(slot_map_index, &self.data)
                .try_into()
                .unwrap(),
        ) as usize;

        // 2. Delete the entry from the data.
        let mut btree_row = BTreeRow::from(row_offset);
        btree_row.clear_row(self.data);

        // 3. Delete the mapping in slot map.
        self.slot_map
            .delete_slot_map_element(slot_map_index, self.data, &mut self.free_space);

        // 4. Update slot count in header.
        header.decrease_slot_count(1);

        Ok(())
    }

    ///
    /// Function to search if a key exists in the page. If the key exists, the method returns the
    /// index in slot_map to which the data is mapped. If it doesn't exist, the method returns the
    /// index at which the slot_map can map the new key.
    ///
    fn search(&self, key: &[u8], start: usize, end: usize) -> Result<usize, usize> {
        if start == end {
            return Err(start);
        }

        let pivot_index: usize = start + (end - start) / 2;

        let row_offset = u16::from_le_bytes(
            self.slot_map
                .get_slot_map_element(pivot_index, self.data)
                .try_into()
                .unwrap(),
        ) as usize;
        let btree_row = BTreeRow::from(row_offset);
        let key_pivot = btree_row.get_key(self.data);

        match cmp_le_bytes(key, key_pivot) {
            Ordering::Equal => Ok(pivot_index),
            Ordering::Less => self.search(key, start, pivot_index),
            Ordering::Greater => self.search(key, pivot_index + 1, end),
        }
    }
}

///
/// View representing the row.
///
struct RowResult<'r> {
    ///
    /// Byte array for the row data
    ///
    data: &'r [u8],
}

impl<'r> RowResult<'r> {
    ///
    /// Creates an instance of RowResult.
    /// # Arguments:
    /// * `data`: Byte array representing the row.
    /// # Returns:
    /// * `Self`: An instance of RowResult.
    ///
    fn from(data: &'r [u8]) -> Self {
        let btree_row = BTreeRow::from(0);

        // Verify that the data passed only contains the row.
        assert_eq!(data.len(), btree_row.get_size(data));

        RowResult { data }
    }

    ///
    /// Fetches the key of the row.
    /// # Returns:
    /// * `&[u8]`: Key of the row.
    ///
    fn get_key(&self) -> &[u8] {
        let btree_row = BTreeRow::from(0);
        btree_row.get_key(&self.data)
    }

    ///
    /// Fetches the value of the row.
    /// # Returns:
    /// * `&[u8]`: Value of the row.
    ///
    fn get_value(&self) -> &[u8] {
        let btree_row = BTreeRow::from(0);
        btree_row.get_value(&self.data)
    }
}

///
/// View of the BTree Page.
///
struct BTreePage<'a> {
    body: BTreeBodyData<'a>,
    header: BTreePageHeader<'a>,
}

impl<'a> BTreePage<'a> {
    pub fn from(data: &'a mut [u8; PAGE_SIZE]) -> Self {
        let (header_bytes, body_bytes) = data.split_at_mut(PAGE_HEADER_SIZE);
        let header = BTreePageHeader::from(header_bytes);
        let body = BTreeBodyData::from(body_bytes, &header);
        Self { body, header }
    }

    ///
    /// Gets a read-only view of the BTree row.
    /// # Arguments:
    /// * `key`: Key of the row to view.
    /// # Returns:
    /// * `Option<RowResult>`: Ok(RowResult) if the row is present. None if not.
    ///
    pub fn get(&self, key: &[u8]) -> Option<RowResult<'_>> {
        match self.body.get(key, &self.header) {
            Err(..) => None,
            Ok(row) => Some(RowResult::from(row)),
        }
    }

    ///
    /// Saves a key value. If the key already exists, it updates the value. If not, it creates
    /// a new row.
    /// # Arguments:
    /// * `key`: Key of the row to insert.
    /// * `value`: Value of the row to insert.
    /// # Returns:
    /// * `Result<(), String>`: Void if the row is inserted. If not, the reason.
    ///
    pub fn save(&mut self, key: &[u8], value: &[u8]) -> Result<(), RustyKVError> {
        match self
            .body
            .search(key, 0, self.header.get_slot_count() as usize)
        {
            Ok(index) => {
                // Key already exists. Update the value.
                self.body.update(value, index)
            }
            Err(index) => {
                // Key doesn't exist. A new one needs to be created.
                let result = self.body.insert(key, value, index);
                self.header.increase_slot_count(1);
                result
            }
        }
    }

    ///
    /// Deletes a key from the page if it exists.
    /// # Arguments:
    /// * `key`: Key to be deleted.
    ///
    /// # Returns
    /// * `Result<(), String>`: Ok() if the deletion succeeded. Err(reason) otherwise.
    ///
    pub fn delete(&mut self, key: &[u8]) -> Result<(), RustyKVError> {
        let result = self
            .body
            .search(key, 0, self.header.get_slot_count() as usize);
        if result.is_ok() {
            return self.body.remove(&mut self.header, result.unwrap());
        }
        Ok(())
    }
}

// TODO: Add more tests to increase the coverage.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_page_inplace() {
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut page = BTreePage::from(&mut data);

        page.save(b"def", b"bar").unwrap();
        page.save(b"abc", b"baz").unwrap();
        page.save(b"abc", b"qux").unwrap();

        let page = BTreePage::from(&mut data);
        assert_eq!(page.get(b"abc").unwrap().get_value(), b"qux");
        assert_eq!(page.get(b"def").unwrap().get_value(), b"bar");
    }

    #[test]
    fn test_btree_page_delete() {
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut page = BTreePage::from(&mut data);
        page.save(b"def", b"bar").unwrap();
        page.save(b"abc", b"baz").unwrap();
        page.delete(b"abc").unwrap();
        assert!(page.get(b"abc").is_none());
        assert_eq!(page.get(b"def").unwrap().get_value(), b"bar");
    }
}
