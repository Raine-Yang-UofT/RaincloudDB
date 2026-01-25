use paste::paste;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::{NamedTempFile};
use raincloud_db::storage::page::page::{Page, PageError};
use raincloud_db::storage::page::data_page::{DataPage};
use raincloud_db::storage::bufferpool::BufferPool;
use raincloud_db::storage::disk_manager::FileDiskManager;
use raincloud_db::storage::replacement_strategy::ReplacementStrategyType;
use raincloud_db::{with_create_pages, with_read_pages, with_write_pages};
use raincloud_db::storage::free_list::FreeList;
use raincloud_db::storage::page::header_page::HeaderPage;
use raincloud_db::types::{FLUSH, NO_FLUSH};

fn setup_buffer_pool(capacity: usize) -> (Arc<BufferPool<DataPage>>, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(path).unwrap());
    let header_disk_manager = Arc::new(FileDiskManager::<HeaderPage>::open(path).unwrap());
    let free_list = Arc::new(Mutex::new(FreeList::new(header_disk_manager, 0)));
    let buffer_pool = Arc::new(BufferPool::new(
        capacity,
        ReplacementStrategyType::LRU,
        disk_manager,
        free_list
    ));
    (buffer_pool, temp_file)
}

#[test]
fn test_basic_fetch_create_unpin() {
    let (pool, _temp_file) = setup_buffer_pool(2);

    // Test creating a page
    let page_id;
    with_create_pages!(pool, [(page_id, page)], NO_FLUSH, {});

    // Test fetching the same page without error
    with_write_pages!(pool, [(page_id, page)], NO_FLUSH, {});
    with_read_pages!(pool, [(page_id, _page)], {});
}

#[test]
fn test_dirty_page_persistence() {
    let (pool, temp_file) = setup_buffer_pool(2);

    // Create and mark as dirty
    let page_id;
    with_create_pages!(pool, [(page_id, page)], FLUSH, {});

    // Create new pool to test persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let header_disk_manager = Arc::new(FileDiskManager::<HeaderPage>::open(temp_file.path()).unwrap());
    let free_list = Arc::new(Mutex::new(FreeList::new(header_disk_manager, 0)));
    let new_pool = BufferPool::new(2, ReplacementStrategyType::LRU, disk_manager, free_list);

    // Should be able to fetch from disk without error
    with_read_pages!(Arc::new(new_pool), [(page_id, _page)], {});
}

#[test]
fn test_flush_page() {
    let (pool, temp_file) = setup_buffer_pool(3);

    let id1;
    let id2;
    let id3;
    with_create_pages!(pool, [(id1, page1), (id2, page2), (id3, page3)], FLUSH, {});

    // Flush pages
    pool.flush_page(id1).unwrap();
    pool.flush_page(id2).unwrap();
    pool.flush_page(id3).unwrap();

    // Verify persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let header_disk_manager = Arc::new(FileDiskManager::<HeaderPage>::open(temp_file.path()).unwrap());
    let free_list = Arc::new(Mutex::new(FreeList::new(header_disk_manager, 0)));
    let new_pool = Arc::new(BufferPool::new(5, ReplacementStrategyType::LRU, disk_manager, free_list));

    // All pages should be accessible
    assert!(new_pool.fetch_page(id1).is_ok());
    assert!(new_pool.fetch_page(id2).is_ok());
    assert!(new_pool.fetch_page(id3).is_ok());
}

#[test]
fn test_flush_all() {
    let (pool, temp_file) = setup_buffer_pool(3);

    let id1;
    let id2;
    let id3;
    let pool = Arc::clone(&pool);
    with_create_pages!(pool, [(id1, page1), (id2, page2), (id3, page3)], FLUSH, {});

    // Flush all
    pool.flush_all();

    // Verify persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let header_disk_manager = Arc::new(FileDiskManager::<HeaderPage>::open(temp_file.path()).unwrap());
    let free_list = Arc::new(Mutex::new(FreeList::new(header_disk_manager, 0)));
    let new_pool = Arc::new(BufferPool::new(5, ReplacementStrategyType::LRU, disk_manager, free_list));

    // All pages should be accessible
    assert!(new_pool.fetch_page(id1).is_ok());
    assert!(new_pool.fetch_page(id2).is_ok());
    assert!(new_pool.fetch_page(id3).is_ok());
}

#[test]
fn test_error_handling() {
    let (pool, _temp_file) = setup_buffer_pool(2);

    // Test invalid page operations
    assert!(matches!(
        pool.fetch_page(999),
        Err(PageError::InvalidPage)
    ));

    assert!(matches!(
        pool.flush_page(999),
        Err(PageError::InvalidPage)
    ));
}

#[test]
fn test_capacity_constraint() {
    let capacity = 3;
    let (pool, _temp_file) = setup_buffer_pool(capacity);
    let pool = Arc::clone(&pool);

    let mut page_ids = vec![];
    let mut page_id;
    // Create exactly capacity + 1 pages
    for _ in 0..=capacity {
        with_create_pages!(pool, [(page_id, page)], NO_FLUSH, {
            page_ids.push(page_id);
        });
    }

    // All pages should be accessible
    let mut accessible_count = 0;
    for &page_id in &page_ids {
        with_read_pages!(pool, [(page_id, _page)], {
            accessible_count += 1;
        });
    }
    assert_eq!(accessible_count, capacity + 1);

    // Should have exactly capacity pages
    assert_eq!(pool.current_size(), capacity);
}

// TODO: To be investigated later
// #[test]
// fn test_concurrent_access_multiple_threads() {
//     let (pool, _temp_file) = setup_buffer_pool(10);
//     let num_threads = 4;
//     let pages_per_thread = 3;
//
//     let mut handles = vec![];
//
//     for thread_id in 0..num_threads {
//         let pool = Arc::clone(&pool);
//
//         handles.push(thread::spawn(move || {
//             let mut page_ids = vec![];
//             // Each thread creates and accesses pages
//             for i in 0..pages_per_thread {
//                 let page_id ;
//                 with_create_pages!(pool, [(page_id, page)], (thread_id + i) % 2 == 0, {
//                     page_ids.push(page_id);
//                     thread::sleep(Duration::from_millis(5));
//                 })
//             }
//
//             // Try to access own pages again
//             let pool = Arc::clone(&pool);
//             for &page_id in &page_ids {
//                 with_read_pages!(pool, [(page_id, _page)], {});
//             }
//
//             page_ids
//         }));
//     }
//
//     // Collect all page IDs created
//     let mut all_page_ids = vec![];
//     for handle in handles {
//         let page_ids = handle.join().unwrap();
//         all_page_ids.extend(page_ids);
//     }
//
//     // Verify pool is in consistent state (not over capacity)
//     let current_size = pool.current_size();
//     assert!(current_size <= 10, "Buffer pool should not exceed capacity");
//
//     // All pages should be accessible
//     let mut accessible_count = 0;
//     let pool = Arc::clone(&pool);
//     for &page_id in &all_page_ids {
//         with_read_pages!(pool, [(page_id, _page)], {
//             accessible_count += 1;
//         })
//     }
//
//     assert_eq!(accessible_count, num_threads * pages_per_thread);
// }

#[test]
fn test_pinned_page_protection() {
    let (pool, _temp_file) = setup_buffer_pool(2);

    // Create and keep pinned
    let id1;
    let id2;
    let id3;
    with_create_pages!(pool, [(id1, page1)], NO_FLUSH, {
        // Create second page and unpin
        with_create_pages!(pool, [(id2, page2)], NO_FLUSH, {});

        // Create third page - should evict page2, not page1
        with_create_pages!(pool, [(id3, page3)], NO_FLUSH, {
            // page1 and page3 should still be pinned
            assert!(pool.fetch_page(id1).is_ok());
            assert!(pool.fetch_page(id3).is_ok());
        });
    });
}

#[test]
fn test_concurrent_access_same_page() {
    let (pool, _tmp) = setup_buffer_pool(4);

    // Create 1 shared page (keep pinned by creator for reference)
    let shared_id;
    let n_threads = 8;
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(n_threads));

    with_create_pages!(pool, [(shared_id, shared)], NO_FLUSH, {});

    for _ in 0..n_threads {
        let barrier_cl = Arc::clone(&barrier);
        let pool_cl = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            // Everyone starts together
            barrier_cl.wait();

            // Fetch (pin) the same page
            with_write_pages!(pool_cl, [(shared_id, shared)], FLUSH, {
                    // Simulate work while holding the pin
                    thread::sleep(Duration::from_millis(5));
                });
            // Return whether we succeeded
            true
        }));
    }

    // Wait for everyone
    for h in handles { h.join().unwrap(); };
}

// TODO: To be investigated later
// Page ABA check stress
//
// create a race where an eviction observes a candidate's Arc,
// but before it removes the mapping, the mapping changes to a different Arc
// for the same PageId (evict -> later re-fetch).
// #[test]
// fn test_aba_stress_mapping_changes() {
//     // tiny capacity to maximize churn
//     let (pool, _tmp) = setup_buffer_pool(1);
//
//     // Start with one known page X
//     let x = pool.create_page().unwrap();
//     let xid = x.read().unwrap().page.get_id();
//
//     // Unpin X so it can be evicted
//     pool.unpin_page(xid, false).unwrap();
//
//     // Thread A: continuously creates pages to evict whatever is resident
//     let pool_a = Arc::clone(&pool);
//     let a = thread::spawn(move || {
//         for _ in 0..50 {
//             let p = pool_a.create_page().unwrap();
//             let id = p.read().unwrap().page.get_id();
//             // immediately unpin to let it be evicted by others
//             let _ = pool_a.unpin_page(id, false);
//             // short pause to interleave
//             thread::sleep(Duration::from_millis(1));
//         }
//     });
//
//     // Thread B: repeatedly fetches X, then unpins, causing mapping to flip between
//     // "some other page" and "X", likely with a new Arc for X after re-load.
//     let pool_b = Arc::clone(&pool);
//     let b = thread::spawn(move || {
//         for _ in 0..50 {
//             if let Ok(_px) = pool_b.fetch_page(xid) {
//                 // hold briefly, then unpin (dirty randomly)
//                 thread::sleep(Duration::from_millis(1));
//                 let _ = pool_b.unpin_page(xid, true);
//             }
//         }
//     });
//
//     a.join().unwrap();
//     b.join().unwrap();
//
//     // Invariants: no panic; pool size never exceeds capacity; X is still fetchable at the end.
//     assert!(pool.current_size() <= 1, "capacity must never be exceeded");
//     assert!(pool.fetch_page(xid).is_ok(), "X should be fetchable after churn");
//     pool.unpin_page(xid, false).unwrap();
// }
