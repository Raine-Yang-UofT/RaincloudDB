use crate::{bitmap_get, bitmap_set};
use crate::types::{PAGE_SIZE, PageId};
use crate::storage::page::page::Page;

const MAX_HEADERS: usize = 2 * size_of::<PageId>() + size_of::<u32>();
pub const FREE_HEADER_SIZE: usize = PAGE_SIZE - MAX_HEADERS;  // free slot bitmap size in bytes

#[derive(Copy, Clone, Debug)]
pub struct HeaderPage {
    id: PageId,     // header page
    next: PageId,   // next header page in free list
    offset: u32,    // start offset of page id
    free_slot: [u8; FREE_HEADER_SIZE],   // bitmap of free page headers
}

impl HeaderPage {
    pub fn get_next(self) -> Option<PageId> {
        if self.next != 0 { Some(self.next) } else { None }
    }

    pub fn set_next(&mut self, next: PageId) {
        self.next = next;

    }

    pub fn get_offset(&self) -> usize {
        self.offset as usize
    }

    /// Given index of page in free list, set header offset3
    pub fn set_offset(&mut self, index: usize) {
        self.offset = (index * FREE_HEADER_SIZE * 8) as u32;
    }

    /// Allocate a page header, mark the location as used
    /// Return None if the page is full
    /// The page header index starts from 1
    pub fn allocate_header(&mut self) -> Option<PageId> {
        if let Some(index) = self.get_slot() {
            bitmap_set!(self.free_slot, index, true);
            Some(((self.offset as usize) + index + 1) as PageId)
        } else {
            None
        }
    }

    /// Deallocate a page header, mark the location as unused
    /// The page header index starts from 1
    pub fn deallocate_header(&mut self, page_id: usize) {
        if bitmap_get!(self.free_slot, page_id - self.offset as usize - 1) == true {
            bitmap_set!(self.free_slot, page_id - self.offset as usize - 1, false)
        } else {
            panic!("attempt to free a header that is already freed");
        }
    }

    /// Return index of first free slot (0-bit) if exists
    fn get_slot(&self) -> Option<usize> {
        let full_words = FREE_HEADER_SIZE / 8;
        let tail_bytes = FREE_HEADER_SIZE % 8;

        // reinterpret bitmap as u64 slice for faster processing
        let (head, tail) = self.free_slot.split_at(full_words * 8);

        // scan full words by assembling into u64
        for i in 0..full_words {
            let base = i * 8;
            let mut w = 0;
            for j in 0..8 {
                w |= (head[base + j] as u64) << (j * 8);
            }
            if w != u64::MAX {
                let inv = !w;
                let tz = inv.trailing_zeros() as usize;
                return Some(i * 64 + tz);
            }
        }

        // scan tail
        if tail_bytes > 0 {
            let mut last = 0;
            for (i, &b) in tail.iter().enumerate() {
                last |= (b as u64) << (i * 8);
            }
            let valid_bits = (FREE_HEADER_SIZE - full_words * 8) * 8;
            let mask = if valid_bits == 64 { !0u64 } else { (1u64 << valid_bits) - 1 };
            let v = last | !mask;
            if v != u64::MAX {
                let inv = !v;
                let tz = inv.trailing_zeros() as usize;
                return Some(full_words * 64 + tz);
            }
        }
        None
    }
}

impl Page for HeaderPage {
    fn new(id: PageId) -> Self {
        HeaderPage { id, next: 0, offset: 0, free_slot: [0; FREE_HEADER_SIZE] }
    }

    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut cursor = 0;

        // serialize page header
        buf[cursor..cursor + size_of::<PageId>()].copy_from_slice(&self.id.to_le_bytes());
        cursor += size_of::<PageId>();
        buf[cursor..cursor + size_of::<PageId>()].copy_from_slice(&self.next.to_le_bytes());
        cursor += size_of::<PageId>();
        buf[cursor..cursor + size_of::<u32>()].copy_from_slice(&self.offset.to_le_bytes());
        cursor += size_of::<u32>();

        // serialize free slot
        buf[cursor..].copy_from_slice(&self.free_slot);

        buf
    }

    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Self> {
        let mut cursor = 0;

        // deserialize page header
        let id = PageId::from_le_bytes(buf[cursor..cursor + size_of::<PageId>()].try_into().ok()?);
        cursor += size_of::<PageId>();
        let next = PageId::from_le_bytes(buf[cursor..cursor + size_of::<PageId>()].try_into().ok()?);
        cursor += size_of::<PageId>();
        let offset = u32::from_le_bytes(buf[cursor..cursor + size_of::<u32>()].try_into().ok()?);
        cursor += size_of::<u32>();

        // deserialize free slot
        let free_slot = buf[cursor..cursor + FREE_HEADER_SIZE].try_into().ok()?;

        Some(HeaderPage{
            id,
            next,
            offset,
            free_slot
        })
    }

    fn get_id(&self) -> PageId {
        self.id
    }

    fn get_free_space(&self) -> usize {
        self.get_slot().unwrap_or(usize::MAX)
    }

    fn is_empty(&self) -> bool {
        self.get_slot().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_slot_all_ones() {
        let mut page = HeaderPage::new(0);
        // set all bits to 1
        for i in 0..FREE_HEADER_SIZE * 8 {
            bitmap_set!(page.free_slot, i, true);
        }
        // get_slot should return None
        assert_eq!(page.get_slot(), None);
    }

    #[test]
    fn test_get_slot_middle_zero() {
        let mut page = HeaderPage::new(0);
        // set all bits to 1
        for i in 0..FREE_HEADER_SIZE * 8 {
            bitmap_set!(page.free_slot, i, true);
        }
        // clear one bit in the middle
        let mid = (FREE_HEADER_SIZE * 8) / 2;
        bitmap_set!(page.free_slot, mid, false);

        let slot = page.get_slot().expect("should find a free slot");
        assert_eq!(slot, mid);
    }

    #[test]
    fn test_get_slot_tail_zero() {
        let mut page = HeaderPage::new(0);
        // set all bits to 1
        for i in 0..FREE_HEADER_SIZE * 8 {
            bitmap_set!(page.free_slot, i, true);
        }
        // clear the last bit (tail)
        let last = FREE_HEADER_SIZE * 8 - 1;
        bitmap_set!(page.free_slot, last, false);

        let slot = page.get_slot().expect("should find a free slot");
        assert_eq!(slot, last);
    }

    #[test]
    fn test_allocate_deallocate_header() {
        let mut page = HeaderPage::new(0);
        page.offset = 100; // example offset

        // allocate first free slot
        let alloc1 = page.allocate_header().expect("should allocate a slot");
        // the first free slot index is 0, so global index = offset + 0
        assert_eq!(alloc1, 101);

        // allocate second free slot
        let alloc2 = page.allocate_header().expect("should allocate a slot");
        assert_eq!(alloc2, 102);

        // deallocate the first slot
        page.deallocate_header(101);
        // bit should now be cleared
        assert!(!bitmap_get!(page.free_slot, 101));

        // deallocate the second slot
        page.deallocate_header(102);
        assert!(!bitmap_get!(page.free_slot, 102));

        // allocating again should return the first slot again
        let alloc3 = page.allocate_header().expect("should allocate a slot");
        assert_eq!(alloc3, 101);
    }

    #[test]
    fn test_get_free_space_and_is_empty_all_ones() {
        let mut page = HeaderPage::new(7);

        // mark all bits as occupied (all ones)
        page.free_slot = [0xFFu8; FREE_HEADER_SIZE];

        // no free slot should exist
        assert_eq!(page.get_slot(), None);
        // get_free_space uses unwrap_or(usize::MAX)
        assert_eq!(page.get_free_space(), usize::MAX);
        assert!(page.is_empty());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut page = HeaderPage::new(123);

        // set fields to non-default values
        page.next = 5;
        page.offset = 0xDEADBEEF_u32;

        // fill free_slot with a distinct, non-trivial pattern
        for i in 0..FREE_HEADER_SIZE {
            page.free_slot[i] = (i as u8).wrapping_mul(37).rotate_left((i % 8) as u32);
        }

        // serialize and deserialize
        let buf = page.serialize();
        let parsed = HeaderPage::deserialize(&buf)
            .expect("deserialize returned None");

        // fields should match after deserialization
        assert_eq!(parsed.id, page.id, "id mismatch after deserialize");
        assert_eq!(parsed.next, page.next, "next mismatch after deserialize");
        assert_eq!(parsed.offset, page.offset, "offset mismatch after deserialize");
        assert_eq!(parsed.free_slot, page.free_slot, "free_slot mismatch after deserialize");
    }

    #[test]
    fn test_partial_free_slots() {
        let mut page = HeaderPage::new(11);

        // set first byte to 0b1111_1110 so bit 0 is free (0 means free in get_slot)
        page.free_slot[0] = 0b1111_1110;
        // rest are occupied
        for b in page.free_slot.iter_mut().skip(1) {
            *b = 0xFF;
        }

        // first free bit is at index 0
        assert_eq!(page.get_slot(), Some(0));
        assert_eq!(page.get_free_space(), 0);
        assert!(!page.is_empty());
    }
}
