use crate::types::{PAGE_SIZE, PageId};
use crate::storage::page::page::{Page, PageError};

#[derive(Copy, Clone, Debug)]
pub struct HeaderPage {
    id: PageId,
    next: PageId,   // next header page in free list

}