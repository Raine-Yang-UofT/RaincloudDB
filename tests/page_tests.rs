use raincloud_db::bitmap_get;
use raincloud_db::storage::page::data_page::{DataPage, PAYLOAD_SIZE};
use raincloud_db::storage::page::page::Page;
use raincloud_db::types::MAX_SLOTS;

#[test]
fn test_insert_and_get_record() {
    let mut page = DataPage::new(1);
    let data = b"hello db";
    let slot = page.insert_record(data).unwrap();
    let got = page.get_record(slot).unwrap();
    assert_eq!(got, data);
}

#[test]
fn test_insert_serialize_deserialize() {
    let mut page = DataPage::new(0);
    let payloads: [&[u8]; 3] = [
        b"slot0 data",
        b"slot1 payload",
        b"slot2 content",
    ];

    let mut slot_ids = Vec::new();
    for payload in &payloads {
        let id = page.insert_record(payload).expect("insert failed");
        slot_ids.push(id);
    }

    let serialized = page.serialize();
    let deserialized = DataPage::deserialize(&serialized).expect("deserialization failed");

    assert_eq!(deserialized.get_id(), 0);
    for (i, expected) in payloads.iter().enumerate() {
        let slot_id = slot_ids[i];
        let actual = deserialized.get_record(slot_id).expect("record missing");
        assert_eq!(actual, *expected);
    }
}

#[test]
fn test_deletion_persists_through_serialization() {
    let mut page = DataPage::new(0);
    let payload = b"delete me please";
    let slot_id = page.insert_record(payload).expect("insert failed");

    assert!(page.delete_record(slot_id).is_ok());

    let serialized = page.serialize();
    let deserialized = DataPage::deserialize(&serialized).expect("deserialization failed");

    assert!(deserialized.get_record(slot_id).is_none());
}

#[test]
fn test_multiple_insertions_and_deletions() {
    let mut page = DataPage::new(1);
    let initial_payloads: [&[u8]; 5] = [
        b"alpha",
        b"bravo",
        b"charlie",
        b"delta",
        b"echo",
    ];

    let mut slot_ids = Vec::new();
    for payload in &initial_payloads {
        let id = page.insert_record(payload).expect("Insert failed");
        slot_ids.push(id);
    }

    assert!(page.delete_record(slot_ids[1]).is_ok());
    assert!(page.delete_record(slot_ids[3]).is_ok());

    let serialized = page.serialize();
    let deserialized = DataPage::deserialize(&serialized).expect("Deserialize failed");

    for (i, &payload) in initial_payloads.iter().enumerate() {
        let slot_id = slot_ids[i];
        match i {
            1 | 3 => {
                assert!(deserialized.get_record(slot_id).is_none(), "Expected deleted record at slot {}", i);
            }
            _ => {
                let actual = deserialized.get_record(slot_id).expect("Expected valid record");
                assert_eq!(actual, payload, "Payload mismatch at slot {}", i);
            }
        }
    }

    let new_payloads: [&[u8]; 2] = [b"foxtrot", b"golf"];
    for payload in &new_payloads {
        page.insert_record(payload).expect("Insert after delete failed");
    }

    let result = DataPage::deserialize(&page.serialize()).expect("Final deserialize failed");
    let total_valid = (0..MAX_SLOTS)
        .filter(|&i| bitmap_get!(result.valid_slots, i))
        .count();

    assert_eq!(total_valid, 5, "Expected 5 valid records after deletions and insertions");
}

#[test]
fn test_full_page_lifecycle() {
    const RECORD_SIZE: usize = 16; // Use a reasonable record size
    let mut page = DataPage::new(42);

    // insert records until we hit either slot limit or space limit
    let mut slot_ids = vec![];
    let mut record_id = 0;

    while page.get_free_space() >= RECORD_SIZE && slot_ids.len() < MAX_SLOTS {
        let record = vec![record_id; RECORD_SIZE];
        let slot_id = page.insert_record(&record).unwrap();
        slot_ids.push(slot_id);
        record_id += 1;
    }

    // verify we filled the page appropriately
    assert!(
        page.get_free_space() < RECORD_SIZE || slot_ids.len() == MAX_SLOTS,
        "Should have filled either space or slots"
    );

    // update some records (first 10%)
    let update_count = (slot_ids.len() / 10).max(1);
    for &slot_id in &slot_ids[0..update_count] {
        let new_data = vec![200; RECORD_SIZE];
        page.update_record(slot_id, &new_data).unwrap();
    }

    // delete some records (next 10%)
    let delete_count = (slot_ids.len() / 10).max(1);
    let delete_start = update_count;
    let delete_end = delete_start + delete_count;
    for &slot_id in &slot_ids[delete_start..delete_end] {
        assert!(page.delete_record(slot_id).is_ok());
    }

    // verify all records
    for (i, &slot_id) in slot_ids.iter().enumerate() {
        if i >= delete_start && i < delete_end {
            // deleted records
            assert!(page.get_record(slot_id).is_none());
        } else if i < update_count {
            // updated records
            assert_eq!(page.get_record(slot_id).unwrap(), vec![200; RECORD_SIZE]);
        } else {
            // original records
            let expected = vec![i as u8; RECORD_SIZE];
            assert_eq!(page.get_record(slot_id).unwrap(), expected);
        }
    }

    // test serialization and deserialization
    let serialized = page.serialize();
    let deserialized = DataPage::deserialize(&serialized).unwrap();

    // verify all records survived serialization
    for &slot_id in slot_ids.iter() {
        assert_eq!(
            page.get_record(slot_id),
            deserialized.get_record(slot_id)
        );
    }

    // test clear
    page.clear();
    assert!(page.is_empty());
    assert_eq!(page.get_free_space(), PAYLOAD_SIZE);
}