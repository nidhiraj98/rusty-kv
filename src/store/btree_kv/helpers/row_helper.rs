///
/// Helper for BTree row operations.
/// TODO: Update all assertions with error handling.
///
pub(crate) mod btree_row {
    use crate::store::btree_kv::constants::page_constants::{KEY_SIZE_OFFSET, KEY_SIZE_SIZE, ROW_HEADER_SIZE, VALUE_SIZE_OFFSET, VALUE_SIZE_SIZE};

    ///
    /// Fetches the size of the key stored in the row.
    /// # Arguments:
    /// * `data`: Byte array containing the row header bytes. The byte array should be
    ///           atleast ROW_HEADER_SIZE long.
    /// # Returns:
    /// * `usize`: Size of the key.
    ///
    pub fn get_key_size(data: &[u8]) -> usize {
        assert!(data.len() >= ROW_HEADER_SIZE);
        u16::from_le_bytes(
            data[KEY_SIZE_OFFSET..KEY_SIZE_OFFSET + KEY_SIZE_SIZE]
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
    pub fn set_key_size(data: &mut [u8], key_size: u16) {
        assert!(data.len() >= ROW_HEADER_SIZE);
        data[KEY_SIZE_OFFSET..KEY_SIZE_OFFSET + KEY_SIZE_SIZE]
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
    pub fn get_value_size(data: &[u8]) -> usize {
        assert!(data.len() >= ROW_HEADER_SIZE);
        u16::from_le_bytes(
            data[VALUE_SIZE_OFFSET..VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE]
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
    pub fn set_value_size(data: &mut [u8], value_size: u16) {
        assert!(data.len() >= ROW_HEADER_SIZE);
        data[VALUE_SIZE_OFFSET..VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE]
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
    pub fn get_key(data: &[u8]) -> &[u8] {
        assert_eq!(data.len(), get_slot_size(data));
        let key_size = get_key_size(data);
        &data[ROW_HEADER_SIZE..ROW_HEADER_SIZE + key_size]
    }

    ///
    /// Sets the key in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// * `key`: A byte array representing the key to be set in the row.
    ///
    pub fn set_key(data: &mut [u8], key: &[u8]) {
        assert_eq!(data.len(), get_slot_size(data));

        let key_size = get_key_size(data);
        assert_eq!(key_size, key.len());
        data[ROW_HEADER_SIZE..ROW_HEADER_SIZE + key_size].copy_from_slice(key);
    }

    ///
    /// Fetches the bytes representing the value in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// # Returns:
    /// * `&[u8]`: Byte array representing the value.
    ///
    pub fn get_value(data: &[u8]) -> &[u8] {
        assert_eq!(data.len(), get_slot_size(data));
        let key_size = get_key_size(data);
        let value_size = get_value_size(data);
        &data[ROW_HEADER_SIZE + key_size..ROW_HEADER_SIZE + key_size + value_size]
    }

    ///
    /// Sets the value in the row.
    /// # Arguments:
    /// * `data`: A byte array representing the row. The byte array should contain both the row
    ///           header and the data.
    /// * `value`: A byte array representing the value to be set in the row.
    ///
    pub fn set_value(data: &mut [u8], value: &[u8]) {
        assert_eq!(data.len(), get_slot_size(data));

        let key_size = get_key_size(data);
        let value_size = get_value_size(data);
        assert_eq!(value_size, value.len());
        data[ROW_HEADER_SIZE + key_size..ROW_HEADER_SIZE + key_size + value_size]
            .copy_from_slice(value);
    }

    ///
    /// Fetches the slot size of the data array.
    /// # Arguments:
    /// * `data`: Byte array representing the row.
    /// # Returns:
    /// * `usize`: Size of the data stored in the row.
    ///
    fn get_slot_size(data: &[u8]) -> usize {
        get_key_size(data) + get_value_size(data) + ROW_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests_row_header {
    use crate::store::btree_kv::constants::page_constants::{KEY_SIZE_OFFSET, KEY_SIZE_SIZE, ROW_HEADER_SIZE, VALUE_SIZE_OFFSET, VALUE_SIZE_SIZE};
    use crate::store::btree_kv::helpers::row_helper::btree_row;

    #[test]
    fn test_row_updates_in_place() {
        const KEY: [u8; 2] = 15u16.to_le_bytes();
        const VALUE: [u8; 2] = 20u16.to_le_bytes();

        let mut row = [0u8; KEY.len() + VALUE.len() + ROW_HEADER_SIZE];
        let mut header_bytes = &mut row[0..ROW_HEADER_SIZE];
        assert_eq!(btree_row::get_key_size(&header_bytes), 0);
        assert_eq!(btree_row::get_value_size(&header_bytes), 0);

        btree_row::set_key_size(&mut header_bytes, KEY.len() as u16);
        btree_row::set_value_size(&mut header_bytes, VALUE.len() as u16);

        // Value from the view
        assert_eq!(btree_row::get_key_size(&header_bytes), KEY.len());
        assert_eq!(btree_row::get_value_size(&header_bytes), VALUE.len());

        // Value from the byte array
        assert_eq!(
            u16::from_le_bytes(
                (&header_bytes[KEY_SIZE_OFFSET..KEY_SIZE_OFFSET + KEY_SIZE_SIZE])
                    .try_into()
                    .unwrap()
            ),
            KEY.len() as u16
        );
        assert_eq!(
            u16::from_le_bytes(
                (&header_bytes[VALUE_SIZE_OFFSET..VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE])
                    .try_into()
                    .unwrap()
            ),
            VALUE.len() as u16
        );

        btree_row::set_key(&mut row, &KEY);
        btree_row::set_value(&mut row, &VALUE);

        // Value from the view
        assert_eq!(btree_row::get_key(&row), KEY);
        assert_eq!(btree_row::get_value(&row), VALUE);

        // Value from the byte array
        assert_eq!(
            &row[ROW_HEADER_SIZE..ROW_HEADER_SIZE + KEY.len()],
            KEY
        );
        assert_eq!(
            &row[ROW_HEADER_SIZE + KEY.len()..ROW_HEADER_SIZE + KEY.len() + VALUE.len()],
            VALUE
        );
    }
}