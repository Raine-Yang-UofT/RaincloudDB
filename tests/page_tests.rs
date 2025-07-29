use raincloud_db::storage::page::Page;

#[test]
fn test_insert_and_get_record() {
    let mut page = Page::new(1);
    let data = b"hello db";
    let slot = page.insert_record(data).unwrap();
    let got = page.get_record(slot).unwrap();
    assert_eq!(got, data);
}

#[test]
fn test_insert_serialize_deserialize() {
    let mut page = Page::new(0);
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
    let deserialized = Page::deserialize(&serialized).expect("deserialization failed");

    assert_eq!(deserialized.id, 0);
    for (i, expected) in payloads.iter().enumerate() {
        let slot_id = slot_ids[i];
        let actual = deserialized.get_record(slot_id).expect("record missing");
        assert_eq!(actual, *expected);
    }
}

#[test]
fn test_deletion_persists_through_serialization() {
    let mut page = Page::new(0);
    let payload = b"delete me please";
    let slot_id = page.insert_record(payload).expect("insert failed");

    assert!(page.delete_record(slot_id));

    let serialized = page.serialize();
    let deserialized = Page::deserialize(&serialized).expect("deserialization failed");

    assert!(deserialized.get_record(slot_id).is_none());
}

#[test]
fn test_multiple_insertions_and_deletions() {
    let mut page = Page::new(1);
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

    assert!(page.delete_record(slot_ids[1]));
    assert!(page.delete_record(slot_ids[3]));

    let serialized = page.serialize();
    let deserialized = Page::deserialize(&serialized).expect("Deserialize failed");

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

    let result = Page::deserialize(&page.serialize()).expect("Final deserialize failed");
    let total_valid = result
        .slots()
        .iter()
        .filter(|slot| slot.is_some() && slot.as_ref().unwrap().is_valid())
        .count();

    assert_eq!(total_valid, 5, "Expected 5 valid records after deletions and insertions");
}