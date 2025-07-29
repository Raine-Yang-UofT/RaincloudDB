use crate::types::{PAGE_SIZE, MAX_SLOTS, PageId, SlotId};

const SLOT_SIZE: usize = 5;
const PAGE_ID_SIZE: usize = size_of::<u32>();
const SLOT_ID_SIZE: usize = size_of::<u16>();
const FREE_START_SIZE: usize = size_of::<u16>();
const fn get_page_header_size() -> usize {
    PAGE_ID_SIZE + SLOT_ID_SIZE + FREE_START_SIZE + MAX_SLOTS * SLOT_SIZE
}
const PAYLOAD_SIZE: usize = PAGE_SIZE - get_page_header_size();

#[derive(Copy, Clone, Debug)]
pub struct Slot {
    offset: u16, // record offset
    length: u16, // record length
    is_valid: bool, // if the data pointed by the slot has been deleted
}

impl Slot {
    pub fn is_valid(&self) -> bool { self.is_valid }
}

#[derive(Copy, Clone, Debug)]
pub struct Page {
    pub id: PageId,
    data: [u8; PAYLOAD_SIZE], // payload data, excluding page header and slot array
    slots: [Option<Slot>; MAX_SLOTS], // page slot array
    next_slot: SlotId, // next available slot index, grow from top to bottom
    free_start: u16, // offset of free space, grow from bottom to top
}

impl Page {

    // create new empty page
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
            next_slot: 0,
            free_start: PAYLOAD_SIZE as u16,
        }
    }

    // display slot array for testing purpose
    pub fn slots(&self) -> &[Option<Slot>; MAX_SLOTS] { &self.slots }

    // serialize page
    /*
    Layout:
    [id: 4][next_slot: 2][free_start: 2][slot array: 5 * MAX_SLOTS][data]
     */
    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut cursor = 0;

        // serialize page header
        buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        cursor += PAGE_ID_SIZE;
        buf[cursor..cursor + SLOT_ID_SIZE].copy_from_slice(&self.next_slot.to_le_bytes());
        cursor += SLOT_ID_SIZE;
        buf[cursor..cursor + FREE_START_SIZE].copy_from_slice(&self.free_start.to_le_bytes());
        cursor += FREE_START_SIZE;

        // serialize slot array
        for slot in &self.slots {
            if let Some(slot) = slot {
                buf[cursor..cursor + size_of::<u16>()].copy_from_slice(&slot.offset.to_le_bytes());
                cursor += size_of::<u16>();
                buf[cursor..cursor + size_of::<u16>()].copy_from_slice(&slot.length.to_le_bytes());
                cursor += size_of::<u16>();
                buf[cursor] = slot.is_valid as u8;
                cursor += 1;
            } else {
                buf[cursor..cursor + 5].fill(0);
                cursor += 5;
            }
        }

        // serialize data
        buf[cursor..].copy_from_slice(&self.data);
        buf
    }

    // deserialize page
    pub fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Page> {
        let mut cursor = 0;

        // deserialize page header
        let id = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
        cursor += PAGE_ID_SIZE;
        let next_slot = SlotId::from_le_bytes(buf[cursor..cursor + SLOT_ID_SIZE].try_into().ok()?);
        cursor += SLOT_ID_SIZE;
        let free_start = u16::from_le_bytes(buf[cursor..cursor + FREE_START_SIZE].try_into().ok()?);
        cursor += FREE_START_SIZE;

        // deserialize slot array
        let mut slots = [None; MAX_SLOTS];
        for i in 0..MAX_SLOTS {
            let offset = u16::from_le_bytes(buf[cursor..cursor + size_of::<u16>()].try_into().ok()?);
            cursor += size_of::<u16>();
            let length = u16::from_le_bytes(buf[cursor..cursor + size_of::<u16>()].try_into().ok()?);
            cursor += size_of::<u16>();
            let is_valid = buf[cursor] != 0;
            cursor += 1;

            if !(offset == 0 && length == 0 && !is_valid) {
                slots[i] = Some(Slot {
                    offset,
                    length,
                    is_valid,
                });
            }
        }

        // deserialize payload data
        let mut data = [0u8; PAYLOAD_SIZE];
        data.copy_from_slice(&buf[get_page_header_size()..]);

        Some(Page {
            id,
            data,
            slots,
            next_slot,
            free_start,
        })
    }

    // insert record to page
    pub fn insert_record(&mut self, record: &[u8]) -> Option<SlotId> {
        // check for available slot
        if self.next_slot as usize >= MAX_SLOTS {
            return None;
        }

        // check for available page space
        let record_len = record.len() as u16;
        if record_len as u16 > self.free_start {
            return None;
        }

        // copy data to page
        self.free_start -= record_len;
        let offset = self.free_start;
        self.data[offset as usize..(offset + record_len) as usize].copy_from_slice(record);

        // update slot array
        let slot = Slot {
            offset,
            length: record_len,
            is_valid: true,
        };
        self.slots[self.next_slot as usize] = Some(slot);
        self.next_slot += 1;

        Some(self.next_slot - 1)
    }

    // get a record by SlotId
    pub fn get_record(&self, slot_id: SlotId) -> Option<&[u8]> {
        if let Some(slot) = self.slots.get(slot_id as usize)?.as_ref() {
            if !slot.is_valid {
                return None;
            }
            let start = slot.offset as usize;
            let end = start + slot.length as usize;
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    // mark a record as deleted
    pub fn delete_record(&mut self, slot_id: SlotId) -> bool {
        if let Some(slot) = self.slots.get_mut(slot_id as usize) {
            if let Some(s) = slot {
                s.is_valid = false;
                return true;
            }
        }
        false
    }



    // TODO: perform page compaction
}

/*
Unit tests
*/
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page_initialization() {
        let page = Page::new(42);
        assert_eq!(page.id, 42);
        assert_eq!(page.next_slot, 0);
        assert_eq!(page.free_start, PAYLOAD_SIZE as u16);
        assert!(page.slots.iter().all(|slot| slot.is_none()));
    }

    #[test]
    fn test_insert_and_get_record() {
        let mut page = Page::new(0);
        let record = b"hello world";

        let slot_id = page.insert_record(record).expect("insert should succeed");
        let fetched = page.get_record(slot_id).expect("record should exist");

        assert_eq!(fetched, record);
    }

    #[test]
    fn test_insert_too_large_record_fails() {
        let mut page = Page::new(0);
        let record = vec![0u8; PAGE_SIZE]; // too large to fit
        assert!(page.insert_record(&record).is_none());
    }

    #[test]
    fn test_multiple_insert_and_fetch() {
        let mut page = Page::new(0);
        let records = vec![b"e1", b"e2", b"e3"];

        let mut ids = vec![];
        for rec in &records {
            let id = page.insert_record(*rec).expect("insert");
            ids.push(id);
        }

        for (i, rec) in records.iter().enumerate() {
            let got = page.get_record(ids[i]).expect("should exist");
            assert_eq!(got, *rec);
        }
    }

    #[test]
    fn test_delete_record() {
        let mut page = Page::new(0);
        let record = b"to be deleted";

        let slot_id = page.insert_record(record).expect("insert ok");
        let deleted = page.delete_record(slot_id);
        assert!(deleted);
        assert!(page.get_record(slot_id).is_none());
    }

    #[test]
    fn test_delete_invalid_slot() {
        let mut page = Page::new(0);
        assert!(!page.delete_record(99)); // out of range
    }

    #[test]
    fn test_get_invalid_slot() {
        let page = Page::new(0);
        assert!(page.get_record(0).is_none());
    }

    #[test]
    fn test_fill_page_until_full() {
        let mut page = Page::new(0);
        let record = vec![1u8; 64];

        let mut count = 0;
        while let Some(_) = page.insert_record(&record) {
            count += 1;
        }

        assert!(page.insert_record(&record).is_none());
        assert_eq!(count, PAYLOAD_SIZE / 64);
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut page = Page {
            id: 42,
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
            next_slot: 0,
            free_start: 800, // arbitrary
        };

        let content = b"hello world!";
        let slot_id = page.insert_record(content);

        let serialized = page.serialize();
        let deserialized = Page::deserialize(&serialized).expect("Failed to deserialize");

        // Check header
        assert_eq!(deserialized.id, page.id);
        assert_eq!(deserialized.free_start, page.free_start);
        assert_eq!(deserialized.next_slot, page.next_slot);

        // Check slot
        let original_slot = page.slots[0].unwrap();
        let deserialized_slot = deserialized.slots[0].unwrap();
        assert_eq!(original_slot.offset, deserialized_slot.offset);
        assert_eq!(original_slot.length, deserialized_slot.length);
        assert_eq!(original_slot.is_valid, deserialized_slot.is_valid);

        // Check payload
        let recovered = page.get_record(slot_id.unwrap()).expect("should exist");
        assert_eq!(recovered, content);
    }

    #[test]
    fn test_serialize_with_empty_slots() {
        let page = Page {
            id: 99,
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
            next_slot: 42,
            free_start: 1000,
        };

        let serialized = page.serialize();
        let deserialized = Page::deserialize(&serialized).expect("Failed to deserialize empty page");

        assert_eq!(deserialized.id, 99);
        assert_eq!(deserialized.free_start, 1000);
        assert_eq!(deserialized.next_slot, 42);
        for slot in deserialized.slots.iter() {
            assert!(slot.is_none());
        }
    }
}
