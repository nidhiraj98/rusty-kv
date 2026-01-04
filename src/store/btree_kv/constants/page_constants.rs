use std::mem::size_of;

pub(crate) const KEY_SIZE_OFFSET: usize = 0;
pub(crate) const KEY_SIZE_SIZE: usize = size_of::<u16>(); // 2 bytes
pub(crate) const VALUE_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
pub(crate) const VALUE_SIZE_SIZE: usize = size_of::<u16>(); // 2 bytes
pub(crate) const ROW_HEADER_SIZE: usize = KEY_SIZE_SIZE + VALUE_SIZE_SIZE;