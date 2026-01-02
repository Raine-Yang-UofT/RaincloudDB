use crate::{bitmap_get, bitmap_set};
use crate::types::{PAGE_SIZE, MAX_SLOTS, PageId, SlotId};
use crate::storage::page::page::{Page, PageError};

const SLOT_SIZE: usize = 4;
const PAGE_ID_SIZE: usize = size_of::<PageId>();
const SLOT_ID_SIZE: usize = size_of::<SlotId>();
const FREE_START_SIZE: usize = size_of::<u16>();
const VALID_SLOT_BITMAP_SIZE: usize = 32;
pub const fn get_page_header_size() -> usize {
    2 * PAGE_ID_SIZE + SLOT_ID_SIZE + FREE_START_SIZE + VALID_SLOT_BITMAP_SIZE + MAX_SLOTS * SLOT_SIZE
}
pub const PAYLOAD_SIZE: usize = PAGE_SIZE - get_page_header_size();

#[derive(Copy, Clone, Debug)]
pub struct Slot {
    offset: u16, // record offset
    length: u16, // record length
}

#[derive(Copy, Clone, Debug)]
pub struct DataPage {
    id: PageId,
    next_id: PageId, // next page id in table heap file, 0 when no next id exists
    next_slot: SlotId, // next available slot index, grow from top to bottom
    free_start: u16, // offset of free space, grow from bottom to top
    slots: [Option<Slot>; MAX_SLOTS], // page slot array
    valid_slots: [u8; VALID_SLOT_BITMAP_SIZE], // bitmap to represent whether the slot value is valid (not deleted)
    data: [u8; PAYLOAD_SIZE], // payload data, excluding page header and slot array
}

impl Page for DataPage {

    fn new(id: PageId) -> Self {
        debug_assert_ne!(id, 0);    // page id must be greater than 0
        Self {
            id,
            next_id: 0,
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
            next_slot: 0,
            valid_slots: [0; VALID_SLOT_BITMAP_SIZE],
            free_start: PAYLOAD_SIZE as u16,
        }
    }


    /// Layout:
    /// [id: 4][next_slot: 2][free_start: 2][valid_slots: 32][slot array: 4 * MAX_SLOTS][data]
    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut cursor = 0;

        // serialize page header
        buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        cursor += PAGE_ID_SIZE;
        buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&self.next_id.to_le_bytes());
        cursor += PAGE_ID_SIZE;
        buf[cursor..cursor + SLOT_ID_SIZE].copy_from_slice(&self.next_slot.to_le_bytes());
        cursor += SLOT_ID_SIZE;
        buf[cursor..cursor + FREE_START_SIZE].copy_from_slice(&self.free_start.to_le_bytes());
        cursor += FREE_START_SIZE;
        buf[cursor..cursor + VALID_SLOT_BITMAP_SIZE].copy_from_slice(&self.valid_slots);
        cursor += VALID_SLOT_BITMAP_SIZE;

        // serialize slot array
        for slot in &self.slots {
            if let Some(slot) = slot {
                buf[cursor..cursor + size_of::<u16>()].copy_from_slice(&slot.offset.to_le_bytes());
                cursor += size_of::<u16>();
                buf[cursor..cursor + size_of::<u16>()].copy_from_slice(&slot.length.to_le_bytes());
                cursor += size_of::<u16>();
            } else {
                buf[cursor..cursor + SLOT_SIZE].fill(0);
                cursor += SLOT_SIZE;
            }
        }

        // serialize data
        buf[cursor..].copy_from_slice(&self.data);
        buf
    }

    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Self> {
        let mut cursor = 0;

        // deserialize page header
        let id = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
        cursor += PAGE_ID_SIZE;
        let next_id = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
        cursor += PAGE_ID_SIZE;
        let next_slot = SlotId::from_le_bytes(buf[cursor..cursor + SLOT_ID_SIZE].try_into().ok()?);
        cursor += SLOT_ID_SIZE;
        let free_start = u16::from_le_bytes(buf[cursor..cursor + FREE_START_SIZE].try_into().ok()?);
        cursor += FREE_START_SIZE;
        let valid_slots = buf[cursor..cursor + VALID_SLOT_BITMAP_SIZE].try_into().ok()?;
        cursor += VALID_SLOT_BITMAP_SIZE;

        // deserialize slot array
        let mut slots = [None; MAX_SLOTS];
        for i in 0..MAX_SLOTS {
            let offset = u16::from_le_bytes(buf[cursor..cursor + size_of::<u16>()].try_into().ok()?);
            cursor += size_of::<u16>();
            let length = u16::from_le_bytes(buf[cursor..cursor + size_of::<u16>()].try_into().ok()?);
            cursor += size_of::<u16>();

            if !(offset == 0 && length == 0) {
                slots[i] = Some(Slot {
                    offset,
                    length,
                });
            }
        }

        // deserialize payload data
        let mut data = [0u8; PAYLOAD_SIZE];
        data.copy_from_slice(&buf[get_page_header_size()..]);

        Some(DataPage {
            id,
            next_id,
            next_slot,
            free_start,
            valid_slots,
            data,
            slots,
        })
    }

    #[inline]
    fn get_id(&self) -> PageId {
        self.id
    }

    #[inline]
    fn get_free_space(&self) -> usize {
        self.free_start as usize
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.next_slot == 0
    }
}

impl DataPage {

    #[inline]
    pub fn get_next_id(&self) -> PageId { self.next_id }

    #[inline]
    pub fn set_next_id(&mut self, id: PageId) { self.next_id = id; }

    /// Insert record to page, return Some(SlotId) if successful
    /// Return None when there's insufficient page space or slot array
    pub fn insert_record(&mut self, record: &[u8]) -> Option<SlotId> {
        // check for available slot
        if self.next_slot as usize >= MAX_SLOTS {
            return None;
        }

        // check for available page space
        let record_len = record.len() as u16;
        if record_len > self.free_start {
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
        };
        bitmap_set!(self.valid_slots, self.next_slot as usize, true);
        self.slots[self.next_slot as usize] = Some(slot);
        self.next_slot += 1;

        Some(self.next_slot - 1)
    }

    /// Get a record by SlotId
    /// Return None if the slot is empty
    pub fn get_record(&self, slot_id: SlotId) -> Option<&[u8]> {
        // return None if slot is invalid
        if !bitmap_get!(self.valid_slots, slot_id as usize) {
            return None;
        }

        // retrieve data by slot offset and length
        if let Some(slot) = self.slots.get(slot_id as usize)?.as_ref() {
            let start = slot.offset as usize;
            let end = start + slot.length as usize;
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    /// Update record with given SlotId and record data
    /// Return Err(PageError::RecordSizeChanged) if the record size is not the same
    /// Return Err(PageError::InvalidSlot) if the slot is empty
    pub fn update_record(&mut self, slot_id: SlotId, new_record: &[u8]) -> Result<(), PageError> {
        // check if record exists
        if !bitmap_get!(self.valid_slots, slot_id as usize) {
            return Err(PageError::InvalidSlot);
        }

        if let Some(slot) = &mut self.slots[slot_id as usize] {
            if new_record.len() == slot.length as usize {
                // in-place update if size is enough
                self.data[slot.offset as usize..(slot.offset + slot.length) as usize]
                    .copy_from_slice(new_record);
                Ok(())
            } else {
                // TODO: implement overflow page for oversized records
                Err(PageError::RecordSizeChanged)
            }
        } else {
            Err(PageError::InvalidSlot)
        }
    }

    /// Mark a record as deleted
    pub fn delete_record(&mut self, slot_id: SlotId) -> Result<(), PageError> {
        if bitmap_get!(self.valid_slots, slot_id as usize) {
            bitmap_set!(self.valid_slots, slot_id as usize, false);
            return Ok(());
        }
        Err(PageError::InvalidSlot)
    }

    /// Iterate through records on page
    pub fn iter_record(&self) -> impl Iterator<Item = (SlotId, &[u8])> {
        self.slots.iter().enumerate()
            .filter(move |(i, _)| bitmap_get!(self.valid_slots, *i))
            .filter_map(|(i, slot)| {
                slot.as_ref().map(|s| {
                    let start = s.offset as usize;
                    let end = start + s.length as usize;
                    (i as SlotId, &self.data[start..end])
                })
            })
    }
}

/*
Unit tests
*/
#[cfg(test)]
mod tests {
    use super::*;

    const SMALL_RECORD: [u8; 3] = [1, 2, 3];
    const LARGE_RECORD: [u8; 12] = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    fn create_page_with_records() -> (DataPage, SlotId, SlotId) {
        let mut page = DataPage::new(1);
        let slot1 = page.insert_record(&SMALL_RECORD).unwrap();
        let slot2 = page.insert_record(&LARGE_RECORD).unwrap();
        (page, slot1, slot2)
    }

    #[test]
    fn test_new_page_initialization() {
        let page = DataPage::new(42);
        assert_eq!(page.get_id(), 42);
        assert!(page.is_empty());
        assert_eq!(page.get_free_space(), PAYLOAD_SIZE);
        assert_eq!(page.next_slot, 0);
        assert_eq!(page.free_start, PAYLOAD_SIZE as u16);
        assert!(page.slots.iter().all(|slot| slot.is_none()));
    }

    #[test]
    fn test_insert_record() {
        let mut page = DataPage::new(1);
        let initial_free_space = page.get_free_space();

        let slot_id = page.insert_record(&SMALL_RECORD).unwrap();

        assert!(!page.is_empty());
        assert_eq!(page.get_free_space(), initial_free_space - SMALL_RECORD.len());
        assert!(bitmap_get!(page.valid_slots, slot_id as usize));
        assert_eq!(page.get_record(slot_id).unwrap(), SMALL_RECORD);
    }

    #[test]
    fn test_insert_and_get_record() {
        let mut page = DataPage::new(1);
        let record = b"hello world";

        let slot_id = page.insert_record(record).expect("insert should succeed");
        let fetched = page.get_record(slot_id).expect("record should exist");

        assert_eq!(fetched, record);
    }

    #[test]
    fn test_insert_too_large_record_fails() {
        let mut page = DataPage::new(1);
        let record = vec![0u8; PAGE_SIZE]; // too large to fit
        assert!(page.insert_record(&record).is_none());
    }

    #[test]
    fn test_multiple_insert_and_fetch() {
        let mut page = DataPage::new(1);
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
        let (mut page, slot1, slot2) = create_page_with_records();

        assert!(page.delete_record(slot1).is_ok());
        assert!(!bitmap_get!(page.valid_slots, slot1 as usize));
        assert!(page.get_record(slot1).is_none());

        // slot2 should still be valid
        assert!(bitmap_get!(page.valid_slots, slot2 as usize));
        assert_eq!(page.get_record(slot2).unwrap(), LARGE_RECORD);
    }

    #[test]
    fn test_delete_invalid_slot() {
        let mut page = DataPage::new(1);
        assert_eq!(page.delete_record(99), Err(PageError::InvalidSlot)); // out of range
    }

    #[test]
    fn test_get_invalid_slot() {
        let page = DataPage::new(1);
        assert!(page.get_record(0).is_none());
    }

    #[test]
    fn test_fill_page_until_full() {
        let mut page = DataPage::new(1);
        let record = vec![1u8; 64];

        let mut count = 0;
        while let Some(_) = page.insert_record(&record) {
            count += 1;
        }

        assert!(page.insert_record(&record).is_none());
        assert_eq!(count, PAYLOAD_SIZE / 64);
    }

    #[test]
    fn test_update_record_same_size() {
        let (mut page, slot, _) = create_page_with_records();
        let initial_free_space = page.get_free_space();
        let new_data = [10, 11, 12];

        assert!(page.update_record(slot, &new_data).is_ok());

        assert_eq!(page.get_record(slot).unwrap(), new_data);
        assert_eq!(page.get_free_space(), initial_free_space);
    }

    #[test]
    fn test_update_record_different_size() {
        let (mut page, slot_id, _) = create_page_with_records();
        let new_data = [10, 11, 12, 13]; // larger than original

        let result = page.update_record(slot_id, &new_data);
        assert!(result.is_err());
        assert_eq!(page.get_record(slot_id).unwrap(), SMALL_RECORD);
    }

    #[test]
    fn test_iter_records() {
        let (page, slot1, slot2) = create_page_with_records();
        let records: Vec<_> = page.iter_record().collect();

        assert_eq!(records.len(), 2);
        assert!(records.contains(&(slot1, &SMALL_RECORD[..])));
        assert!(records.contains(&(slot2, &LARGE_RECORD[..])));
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut page = DataPage {
            id: 42,
            next_id: 0,
            next_slot: 0,
            free_start: 800,
            valid_slots: [0u8; VALID_SLOT_BITMAP_SIZE],
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
        };

        let content = b"hello world!";
        let slot_id = page.insert_record(content);

        let serialized = page.serialize();
        let deserialized = DataPage::deserialize(&serialized).expect("Failed to deserialize");

        // Check header
        assert_eq!(deserialized.id, page.id);
        assert_eq!(deserialized.free_start, page.free_start);
        assert_eq!(deserialized.next_slot, page.next_slot);

        // Check slot
        let original_slot = page.slots[0].unwrap();
        let deserialized_slot = deserialized.slots[0].unwrap();
        assert_eq!(original_slot.offset, deserialized_slot.offset);
        assert_eq!(original_slot.length, deserialized_slot.length);
        assert_eq!(bitmap_get!(page.valid_slots, 0), bitmap_get!(deserialized.valid_slots, 0));

        // Check payload
        let recovered = page.get_record(slot_id.unwrap()).expect("should exist");
        assert_eq!(recovered, content);
    }

    #[test]
    fn test_serialize_with_empty_slots() {
        let page = DataPage {
            id: 99,
            next_id: 100,
            next_slot: 42,
            free_start: 1000,
            valid_slots: [0u8; VALID_SLOT_BITMAP_SIZE],
            data: [0u8; PAYLOAD_SIZE],
            slots: [None; MAX_SLOTS],
        };

        let serialized = page.serialize();
        let deserialized = DataPage::deserialize(&serialized).expect("Failed to deserialize empty page");

        assert_eq!(deserialized.id, 99);
        assert_eq!(deserialized.free_start, 1000);
        assert_eq!(deserialized.next_slot, 42);
        for (i, slot) in deserialized.slots.iter().enumerate() {
            assert!(slot.is_none());
            assert!(!bitmap_get!(page.valid_slots, i))
        }
    }
}
