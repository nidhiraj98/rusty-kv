pub const PAGE_SIZE: usize = 8000; // 8kb.

///
/// Defines the PageId of a Page of data in disk.
///
/// TODO: Data is assumed to be stored in a single file. Handle multiple
/// files.
///
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct PageId(u64);

impl PageId {
    pub const INVALID: PageId = PageId(u64::MAX);

    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}
