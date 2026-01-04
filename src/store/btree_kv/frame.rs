use crate::store::btree_kv::commons::{PAGE_SIZE, PageId};
use std::ops::Deref;
use std::sync::Arc;

///
/// Frame should only contain the data since we'd like to
/// allocate the entire capacity of the buffer pool for
/// storing the data alone.
///
#[derive(Clone)]
pub struct Frame {
    pub data: Arc<[u8; PAGE_SIZE]>,
}

impl Default for Frame {
    fn default() -> Self {
        Frame {
            data: Arc::new([0u8; PAGE_SIZE]),
        }
    }
}

#[derive(Clone, Copy)]
pub struct FrameMetadata {
    pub(crate) page_id: Option<PageId>,
    pub(crate) is_dirty: bool,
}

impl Default for FrameMetadata {
    fn default() -> Self {
        FrameMetadata {
            page_id: None,
            is_dirty: false,
        }
    }
}

///
/// Wrapper for a Frame.
///
pub struct FrameHandler<'a> {
    frame: &'a mut Frame,
    frame_metadata: &'a mut FrameMetadata,
}

impl<'a> FrameHandler<'a> {
    ///
    /// Creates a new instance of FrameHandler.
    ///
    pub(crate) fn new(frame: &'a mut Frame, frame_metadata: &'a mut FrameMetadata) -> Self {
        FrameHandler {
            frame,
            frame_metadata,
        }
    }

    ///
    /// Checks if a frame is dirty.
    /// # Returns
    /// * `true` if frame is dirty, `false` otherwise.
    ///
    fn is_dirty(&self) -> bool {
        self.frame_metadata.is_dirty
    }

    ///
    /// Fetches data from the frame.
    /// # Returns
    /// * `[u8; PAGE_SIZE]` containing the frame data.
    ///
    fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.frame.data.deref()
    }

    ///
    /// Updates the data in the frame.
    ///
    /// # Arguments
    /// * `data`: New data to be updated into the buffer pool.
    ///
    fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        // TODO: Ensure there aren't any other references to this data.
        self.frame.data = Arc::from(data);
        self.frame_metadata.is_dirty = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_works() {
        let mut frame = Frame::default();

        assert_eq!(frame.data.deref().len(), PAGE_SIZE);

        let test_data: [u8; PAGE_SIZE] = [100u8; PAGE_SIZE];
        frame.data = Arc::from(test_data);

        assert_eq!(*frame.data.deref(), test_data);
    }

    #[test]
    fn frame_metadata_works() {
        let mut frame_metadata = FrameMetadata::default();
        assert_eq!(frame_metadata.page_id.is_none(), true);
        assert_eq!(frame_metadata.is_dirty, false);

        frame_metadata.page_id = Some(PageId::new(1));
        frame_metadata.is_dirty = true;
    }

    #[test]
    fn frame_handler_works() {
        let mut frame = Frame::default();
        let mut frame_metadata = FrameMetadata::default();

        let mut frame_handler = FrameHandler::new(&mut frame, &mut frame_metadata);

        let new_data = [100u8; PAGE_SIZE];
        frame_handler.set_data(new_data);

        assert_eq!(*frame_handler.get_data(), new_data);
        assert_eq!(frame_handler.is_dirty(), true);
    }
}
