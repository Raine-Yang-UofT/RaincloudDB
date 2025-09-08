use std::sync::Arc;
use tempfile::NamedTempFile;
use raincloud_db::storage::bplus_tree::BPlusTree;
use raincloud_db::storage::bufferpool::BufferPool;
use raincloud_db::storage::disk_manager::FileDiskManager;
use raincloud_db::storage::index_page::{IndexPage, RecordId};
use raincloud_db::storage::page::Page;
use raincloud_db::storage::replacement_strategy::ReplacementStrategyType;
use raincloud_db::types::SlotId;

fn setup_buffer_pool(capacity: usize) -> (Arc<BufferPool<IndexPage>>, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let disk_manager =
        Arc::new(FileDiskManager::<IndexPage>::open(path).unwrap());
    let buffer_pool = Arc::new(BufferPool::new(
        capacity,
        ReplacementStrategyType::LRU,
        disk_manager,
    ));
    (buffer_pool, temp_file)
}

fn setup_empty_tree() -> BPlusTree {
    let (buffer_pool, _temp_file) = setup_buffer_pool(10);
    let root_page = buffer_pool.create_page().unwrap();
    let root_id = root_page.read().unwrap().page.get_id();
    BPlusTree::new(root_id, buffer_pool, 4, 4)
}

fn setup_small_leaf_tree(leaf_max: usize) -> BPlusTree {
    let (buffer_pool, _temp_file) = setup_buffer_pool(10);
    let root_page = buffer_pool.create_page().unwrap();
    let root_id = root_page.read().unwrap().page.get_id();
    BPlusTree::new(root_id, buffer_pool, 4, leaf_max)
}

fn setup_small_internal_tree(internal_max: usize, leaf_max: usize) -> BPlusTree {
    let (buffer_pool, _temp_file) = setup_buffer_pool(10);
    let root_page = buffer_pool.create_page().unwrap();
    let root_id = root_page.read().unwrap().page.get_id();
    BPlusTree::new(root_id, buffer_pool, internal_max, leaf_max)
}

#[test]
fn test_single_insert() {
    let mut tree = setup_empty_tree();
    let rid = RecordId { page_id: 1, slot_id: 0 };
    tree.insert(10, rid);
    assert_eq!(tree.search(&10), Some(rid));
}

#[test]
fn test_multiple_inserts_no_split() {
    let mut tree = setup_empty_tree();
    let rids: Vec<_> = (0..5)
        .map(|i| RecordId { page_id: 1, slot_id: i })
        .collect();

    for (i, rid) in rids.iter().enumerate() {
        tree.insert(i as i64, *rid);
    }

    for (i, rid) in rids.iter().enumerate() {
        assert_eq!(tree.search(&(i as i64)), Some(*rid));
    }
}

#[test]
fn test_leaf_split() {
    let mut tree = setup_small_leaf_tree(3); // leaf_max_keys = 3
    // add enough leaves to trigger split
    let rids: Vec<_> = (0..6)
        .map(|i| RecordId { page_id: 2, slot_id: i })
        .collect();
    // verify both sides are searchable
    for (i, rid) in rids.iter().enumerate() {
        tree.insert(i as i64, *rid);
    }
    for (i, rid) in rids.iter().enumerate() {
        assert_eq!(tree.search(&(i as i64)), Some(*rid));
    }
}

#[test]
fn test_internal_split() {
    let mut tree = setup_small_internal_tree(2, 3); // small limits
    for i in 0..20 {
        tree.insert(i, RecordId { page_id: 3, slot_id: i as SlotId });
    }

    for i in 0..20 {
        assert!(tree.search(&i).is_some());
    }
}

#[test]
fn test_delete_no_merge() {
    let mut tree = setup_empty_tree();
    let rid = RecordId { page_id: 1, slot_id: 0 };
    tree.insert(42, rid);

    assert!(tree.delete(42));
    assert_eq!(tree.search(&42), None);
}

#[test]
fn test_delete_with_merge_and_root_collapse() {
    let mut tree = setup_small_internal_tree(2, 3);
    for i in 0..10 {
        tree.insert(i, RecordId { page_id: 5, slot_id: i as SlotId });
    }

    for i in 0..10 {
        assert!(tree.delete(i));
    }

    for i in 0..10 {
        assert_eq!(tree.search(&i), None);
    }
}