use crate::types::{PAGE_SIZE, PageId};

pub trait Page {

    // create a new empty page with given ID
    fn new(id: PageId) -> Self;

    // serialize page to byte array
    fn serialize(&self) -> [u8; PAGE_SIZE];

    // deserialize page from byte array
    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Self> where Self: Sized;

    // return page id
    fn get_id(&self) -> PageId;

    // return the amount of free space in page
    // unreclaimed deleted space is not counted towards free space
    fn get_free_space(&self) -> usize;

    // return whether page is empty
    fn is_empty(&self) -> bool;

    // clear the page
    fn clear(&mut self);
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PageError {
    InvalidSlot,
    RecordSizeChanged
}