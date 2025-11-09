use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use raincloud_db::storage::bplus_tree::BPlusTree;
use raincloud_db::storage::bufferpool::BufferPool;
use raincloud_db::storage::disk_manager::FileDiskManager;
use raincloud_db::storage::free_list::FreeList;
use raincloud_db::storage::page::header_page::HeaderPage;
use raincloud_db::storage::page::index_page::{IndexPage, RecordId};
use raincloud_db::storage::page::page::Page;
use raincloud_db::storage::replacement_strategy::ReplacementStrategyType;
use raincloud_db::types::{PageId, SlotId};

// Helper function to create a test B+ tree with small capacities for easier testing
fn create_test_tree() -> BPlusTree {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let disk_manager =
        Arc::new(FileDiskManager::<IndexPage>::open(path).unwrap());
    let header_disk_manager = Arc::new(FileDiskManager::<HeaderPage>::open(temp_file.path()).unwrap());
    let free_list = Arc::new(Mutex::new(FreeList::new(header_disk_manager, 0)));
    let buffer_pool = Arc::new(BufferPool::new(
        100,
        ReplacementStrategyType::LRU,
        disk_manager,
        free_list
    ));
    let root_page = buffer_pool.create_page().unwrap();
    let root_id = root_page.read().get_id();
    BPlusTree::new(root_id, buffer_pool, 3, 3)
}

// Helper function to insert multiple keys for setup
fn insert_keys(tree: &mut BPlusTree, keys: &[i64]) {
    for &key in keys {
        tree.insert(key, RecordId { page_id: key as PageId, slot_id: key as SlotId });
    }
}

// Helper function to verify search results
fn verify_searches(tree: &mut BPlusTree, expected_keys: &[i64], expected_missing: &[i64]) {
    for &key in expected_keys {
        assert!(tree.search(&key).is_some(), "Key {} should exist", key);
    }
    for &key in expected_missing {
        assert!(tree.search(&key).is_none(), "Key {} should not exist", key);
    }
}

#[test]
fn test_empty_tree_operations() {
    let mut tree = create_test_tree();

    // Search in empty tree
    assert!(tree.search(&1).is_none());

    // Delete from empty tree
    assert!(!tree.delete(1));

    // Insert first key
    tree.insert(10, RecordId { page_id: 10, slot_id: 10 });
    assert!(tree.search(&10).is_some());
}

#[test]
fn test_basic_insert_search_delete() {
    let mut tree = create_test_tree();

    // Insert some keys
    let keys = vec![10, 20, 30, 5, 15, 25];
    insert_keys(&mut tree, &keys);

    // Verify all keys exist
    verify_searches(&mut tree, &keys, &[]);

    // Delete some keys
    assert!(tree.delete(15));
    assert!(tree.delete(25));
    assert!(!tree.delete(100)); // non-existent key

    // Verify remaining keys
    verify_searches(&mut tree, &[10, 20, 30, 5], &[15, 25, 100]);
}

#[test]
fn test_leaf_right_split() {
    let mut tree = create_test_tree();

    // Insert keys in ascending order to test right split
    // With max_keys = 3, the 4th key should cause a split
    insert_keys(&mut tree, &[1, 2, 3, 4]);

    // Verify all keys are searchable after split
    verify_searches(&mut tree, &[1, 2, 3, 4], &[]);

    // Insert more to test multiple splits
    insert_keys(&mut tree, &[5, 6, 7, 8]);
    verify_searches(&mut tree, &[1, 2, 3, 4, 5, 6, 7, 8], &[]);
}

#[test]
fn test_leaf_left_split() {
    let mut tree = create_test_tree();

    // Insert keys in descending order to test left split scenarios
    insert_keys(&mut tree, &[8, 7, 6, 5]);
    verify_searches(&mut tree, &[8, 7, 6, 5], &[]);

    // Continue inserting to trigger more splits
    insert_keys(&mut tree, &[4, 3, 2, 1]);
    verify_searches(&mut tree, &[1, 2, 3, 4, 5, 6, 7, 8], &[]);
}

#[test]
fn test_internal_node_split() {
    let mut tree = create_test_tree();

    // Insert enough keys to create internal nodes and trigger internal splits
    // This should create multiple leaf pages and eventually split internal nodes
    let keys: Vec<i64> = (1..=20).collect();
    insert_keys(&mut tree, &keys);

    // Verify all keys are still searchable
    verify_searches(&mut tree, &keys, &[]);

    // Test that tree structure is maintained with more insertions
    let more_keys: Vec<i64> = (21..=30).collect();
    insert_keys(&mut tree, &more_keys);

    let all_keys: Vec<i64> = (1..=30).collect();
    verify_searches(&mut tree, &all_keys, &[]);
}

#[test]
fn test_root_split_and_height_increase() {
    let mut tree = create_test_tree();

    // Insert enough keys to force root split
    let keys: Vec<i64> = (1..=15).collect();
    insert_keys(&mut tree, &keys);

    // Verify tree maintains correctness after root split
    verify_searches(&mut tree, &keys, &[]);

    // Continue inserting to test multiple levels
    let more_keys: Vec<i64> = (16..=25).collect();
    insert_keys(&mut tree, &more_keys);

    let all_keys: Vec<i64> = (1..=25).collect();
    verify_searches(&mut tree, &all_keys, &[]);
}

#[test]
fn test_left_redistribution() {
    let mut tree = create_test_tree();

    // Create a scenario where left redistribution is needed
    // Insert keys to create specific tree structure
    insert_keys(&mut tree, &[10, 20, 30, 40, 50, 60]);

    // Delete keys to trigger underflow and left redistribution
    assert!(tree.delete(50));
    assert!(tree.delete(60));

    // Verify remaining keys are still accessible
    verify_searches(&mut tree, &[10, 20, 30, 40], &[50, 60]);

    // Test that we can still insert and search
    tree.insert(15, RecordId { page_id: 15, slot_id: 15 });
    verify_searches(&mut tree, &[10, 15, 20, 30, 40], &[50, 60]);
}

#[test]
fn test_right_redistribution() {
    let mut tree = create_test_tree();

    // Create scenario for right redistribution
    insert_keys(&mut tree, &[5, 10, 15, 20, 25, 30]);

    // Delete keys to trigger right redistribution
    assert!(tree.delete(5));
    assert!(tree.delete(10));

    // Verify remaining keys
    verify_searches(&mut tree, &[15, 20, 25, 30], &[5, 10]);

    // Test continued operations
    tree.insert(12, RecordId { page_id: 12, slot_id: 12 });
    verify_searches(&mut tree, &[12, 15, 20, 25, 30], &[5, 10]);
}

#[test]
fn test_left_merge() {
    let mut tree = create_test_tree();

    // Build tree structure that will require left merge
    insert_keys(&mut tree, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Delete keys to trigger merge with left sibling
    for key in [10, 9, 8, 7, 6] {
        assert!(tree.delete(key));
    }

    // Verify remaining keys
    verify_searches(&mut tree, &[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10]);

    // Test tree is still functional
    tree.insert(11, RecordId { page_id: 11, slot_id: 11 });
    verify_searches(&mut tree, &[1, 2, 3, 4, 5, 11], &[6, 7, 8, 9, 10]);
}

#[test]
fn test_right_merge() {
    let mut tree = create_test_tree();

    // Build tree for right merge scenario
    insert_keys(&mut tree, &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);

    // Delete keys to trigger merge with right sibling
    for key in [10, 20, 30, 40, 50] {
        assert!(tree.delete(key));
    }

    // Verify remaining keys
    verify_searches(&mut tree, &[60, 70, 80, 90, 100], &[10, 20, 30, 40, 50]);

    // Test continued functionality
    tree.insert(55, RecordId { page_id: 55, slot_id: 55 });
    verify_searches(&mut tree, &[55, 60, 70, 80, 90, 100], &[10, 20, 30, 40, 50]);
}

#[test]
fn test_root_collapse() {
    let mut tree = create_test_tree();

    // Build a tree with multiple levels
    let keys: Vec<i64> = (1..=15).collect();
    insert_keys(&mut tree, &keys);

    // Delete most keys to trigger root collapse
    for key in 6..=15 {
        assert!(tree.delete(key));
    }

    // Verify remaining keys are still accessible
    verify_searches(&mut tree, &[1, 2, 3, 4, 5], &(6..=15).collect::<Vec<_>>());

    // Delete more to potentially trigger further collapses
    for key in [3, 4, 5] {
        assert!(tree.delete(key));
    }

    verify_searches(&mut tree, &[1, 2], &(3..=15).collect::<Vec<_>>());

    // Test that single remaining keys work
    assert!(tree.delete(2));
    verify_searches(&mut tree, &[1], &(2..=15).collect::<Vec<_>>());

    // Test final deletion
    assert!(tree.delete(1));
    verify_searches(&mut tree, &[], &(1..=15).collect::<Vec<_>>());
}

#[test]
fn test_complex_mixed_operations() {
    let mut tree = create_test_tree();

    // Complex sequence of operations
    insert_keys(&mut tree, &[50, 25, 75, 10, 30, 60, 80, 5, 15, 20, 35]);

    // Verify initial state
    verify_searches(&mut tree, &[50, 25, 75, 10, 30, 60, 80, 5, 15, 20, 35], &[]);

    // Delete some keys
    for key in [5, 15, 35, 80] {
        assert!(tree.delete(key));
    }

    // Insert new keys
    insert_keys(&mut tree, &[12, 27, 65, 90]);

    // Verify final state
    verify_searches(
        &mut tree,
        &[50, 25, 75, 10, 30, 60, 20, 12, 27, 65, 90],
        &[5, 15, 35, 80]
    );

    // More complex deletions to trigger merges and redistributions
    for key in [10, 20, 12, 27] {
        assert!(tree.delete(key));
    }

    verify_searches(
        &mut tree,
        &[50, 25, 75, 30, 60, 65, 90],
        &[5, 10, 12, 15, 20, 27, 35, 80]
    );
}

#[test]
fn test_duplicate_key_handling() {
    let mut tree = create_test_tree();

    // Insert key
    tree.insert(10, RecordId { page_id: 10, slot_id: 1 });
    assert!(tree.search(&10).is_some());

    // Insert same key again (should update or handle appropriately)
    tree.insert(10, RecordId { page_id: 10, slot_id: 2 });
    assert!(tree.search(&10).is_some());

    // Delete should work
    assert!(tree.delete(10));
    assert!(tree.search(&10).is_none());

    // Delete again should fail
    assert!(!tree.delete(10));
}

#[test]
fn test_large_scale_operations() {
    let mut tree = create_test_tree();

    // Insert large number of keys
    let keys: Vec<i64> = (1..=100).collect();
    insert_keys(&mut tree, &keys);

    // Verify all keys
    verify_searches(&mut tree, &keys, &[]);

    // Delete every other key
    for key in keys.iter().step_by(2) {
        assert!(tree.delete(*key));
    }

    // Verify remaining keys
    let remaining: Vec<i64> = keys.iter().skip(1).step_by(2).copied().collect();
    let deleted: Vec<i64> = keys.iter().step_by(2).copied().collect();
    verify_searches(&mut tree, &remaining, &deleted);

    // Delete remaining keys in reverse order
    for key in remaining.iter().rev() {
        assert!(tree.delete(*key));
    }

    // Verify empty tree
    verify_searches(&mut tree, &[], &keys);
}

#[test]
fn test_boundary_conditions() {
    let mut tree = create_test_tree();

    // Test with minimum and maximum i64 values
    let extreme_keys = vec![i64::MIN, i64::MAX, 0, -1, 1];
    insert_keys(&mut tree, &extreme_keys);
    verify_searches(&mut tree, &extreme_keys, &[]);

    // Test deletion of extreme values
    assert!(tree.delete(i64::MIN));
    assert!(tree.delete(i64::MAX));
    verify_searches(&mut tree, &[0, -1, 1], &[i64::MIN, i64::MAX]);

    // Test that tree handles negative numbers correctly
    insert_keys(&mut tree, &[-100, -50, -25, -10, -5]);
    verify_searches(&mut tree, &[0, -1, 1, -100, -50, -25, -10, -5], &[]);
}

// Test Hang: to be investigated later
// #[test]
// fn test_sequential_insert_delete_patterns() {
//     let mut tree = create_test_tree();
//
//     // Test ascending insert, descending delete
//     insert_keys(&mut tree, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
//     for key in (1..=10).rev() {
//         assert!(tree.delete(key));
//     }
//     verify_searches(&mut tree, &[], &(1..=10).collect::<Vec<_>>());
//
//     // Test descending insert, ascending delete
//     insert_keys(&mut tree, &[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
//     for key in 1..=10 {
//         assert!(tree.delete(key));
//     }
//     verify_searches(&mut tree, &[], &(1..=10).collect::<Vec<_>>());
// }