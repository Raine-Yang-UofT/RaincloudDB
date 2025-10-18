// global constants
// data page configs
pub const PAGE_SIZE: usize = 4096;
pub const MAX_SLOTS: usize = 255;

// global types
pub type PageId = u32;
pub type SlotId = u8;

// defined constats
pub const FLUSH: bool = true;
pub const NO_FLUSH: bool = false;