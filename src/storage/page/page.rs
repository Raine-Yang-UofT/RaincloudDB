use crate::types::{PAGE_SIZE, PageId};

pub trait Page: Send + Sync {

    /// Create a new empty page with given ID
    fn new(id: PageId) -> Self;

    /// Serialize page to byte array
    fn serialize(&self) -> [u8; PAGE_SIZE];

    /// Deserialize page from byte array
    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Self> where Self: Sized;

    /// Return page id
    fn get_id(&self) -> PageId;

    /// Return the amount of free space in page
    /// Unreclaimed deleted space is not counted towards free space
    fn get_free_space(&self) -> usize;

    /// Return whether page is empty
    fn is_empty(&self) -> bool;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PageError {
    InvalidPage,         // the page id is invalid
    InvalidSlot,         // the slot id is invalid
    RecordSizeChanged,   // the record size is updated to a different size
    PageLatched,         // the page is used by other connections
    PageAlreadyUnpinned,        // an unpinned page is attempted to be unpin again
}