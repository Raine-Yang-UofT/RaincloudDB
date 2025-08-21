use std::sync::{Arc, Barrier};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use tempfile::{NamedTempFile};
use raincloud_db::storage::page::{Page, PageError};
use raincloud_db::storage::data_page::{DataPage};
use raincloud_db::storage::bufferpool::BufferPool;
use raincloud_db::storage::disk_manager::FileDiskManager;
use raincloud_db::storage::replacement_strategy::ReplacementStrategyType;

fn setup_buffer_pool(capacity: usize) -> (Arc<BufferPool<DataPage>>, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(path).unwrap());
    let buffer_pool = Arc::new(BufferPool::new(
        capacity,
        ReplacementStrategyType::LRU,
        disk_manager,
    ));
    (buffer_pool, temp_file)
}

#[test]
fn test_basic_fetch_create_unpin() {
    let (pool, _temp_file) = setup_buffer_pool(2);

    // Test creating a page
    let page_ref = pool.create_page().unwrap();
    let page_id = page_ref.read().unwrap().page.get_id();

    // Test unpinning
    pool.unpin_page(page_id, false).unwrap();

    // Test fetching the same page
    let fetched_ref = pool.fetch_page(page_id).unwrap();
    assert_eq!(fetched_ref.read().unwrap().page.get_id(), page_id);

    // Clean up
    pool.unpin_page(page_id, false).unwrap();
}

#[test]
fn test_dirty_page_persistence() {
    let (pool, temp_file) = setup_buffer_pool(2);

    // Create and mark as dirty
    let page_ref = pool.create_page().unwrap();
    let page_id = page_ref.read().unwrap().page.get_id();
    pool.unpin_page(page_id, true).unwrap(); // Mark dirty

    // Flush explicitly
    pool.flush_page(page_id).unwrap();

    // Create new pool to test persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let new_pool = BufferPool::new(2, ReplacementStrategyType::LRU, disk_manager);

    // Should be able to fetch from disk
    let fetched = new_pool.fetch_page(page_id).unwrap();
    assert_eq!(fetched.read().unwrap().page.get_id(), page_id);

    new_pool.unpin_page(page_id, false).unwrap();
}

#[test]
fn test_flush_page() {
    let (pool, temp_file) = setup_buffer_pool(3);

    // Create multiple pages with different dirty states
    let page1 = pool.create_page().unwrap();
    let page1_id = page1.read().unwrap().page.get_id();
    pool.unpin_page(page1_id, true).unwrap(); // Dirty

    let page2 = pool.create_page().unwrap();
    let page2_id = page2.read().unwrap().page.get_id();
    pool.unpin_page(page2_id, false).unwrap(); // Clean

    let page3 = pool.create_page().unwrap();
    let page3_id = page3.read().unwrap().page.get_id();
    pool.unpin_page(page3_id, true).unwrap(); // Dirty

    // Flush pages
    pool.flush_page(page1_id).unwrap();
    pool.flush_page(page2_id).unwrap();
    pool.flush_page(page3_id).unwrap();

    // Verify persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let new_pool = BufferPool::new(5, ReplacementStrategyType::LRU, disk_manager);

    // All pages should be accessible
    assert!(new_pool.fetch_page(page1_id).is_ok());
    assert!(new_pool.fetch_page(page2_id).is_ok());
    assert!(new_pool.fetch_page(page3_id).is_ok());
}

#[test]
fn test_flush_all() {
    let (pool, temp_file) = setup_buffer_pool(3);

    // Create multiple pages with different dirty states
    let page1 = pool.create_page().unwrap();
    let page1_id = page1.read().unwrap().page.get_id();
    pool.unpin_page(page1_id, true).unwrap(); // Dirty

    let page2 = pool.create_page().unwrap();
    let page2_id = page2.read().unwrap().page.get_id();
    pool.unpin_page(page2_id, false).unwrap(); // Clean

    let page3 = pool.create_page().unwrap();
    let page3_id = page3.read().unwrap().page.get_id();
    pool.unpin_page(page3_id, true).unwrap(); // Dirty

    // Flush all
    pool.flush_all();

    // Verify persistence
    let disk_manager = Arc::new(FileDiskManager::<DataPage>::open(temp_file.path()).unwrap());
    let new_pool = BufferPool::new(5, ReplacementStrategyType::LRU, disk_manager);

    // All pages should be accessible
    assert!(new_pool.fetch_page(page1_id).is_ok());
    assert!(new_pool.fetch_page(page2_id).is_ok());
    assert!(new_pool.fetch_page(page3_id).is_ok());
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
        pool.unpin_page(999, false),
        Err(PageError::InvalidPage)
    ));

    assert!(matches!(
        pool.flush_page(999),
        Err(PageError::InvalidPage)
    ));

    // Test double unpin error
    let page = pool.create_page().unwrap();
    let page_id = page.read().unwrap().page.get_id();
    pool.unpin_page(page_id, false).unwrap();

    assert!(matches!(
        pool.unpin_page(page_id, false),
        Err(PageError::PageAlreadyUnpinned)
    ));
}

#[test]
fn test_capacity_constraint() {
    let capacity = 3;
    let (pool, _temp_file) = setup_buffer_pool(capacity);

    let mut page_ids = vec![];

    // Create exactly capacity + 1 pages
    for _ in 0..=capacity {
        let page = pool.create_page().unwrap();
        let page_id = page.read().unwrap().page.get_id();
        page_ids.push(page_id);
        pool.unpin_page(page_id, false).unwrap();
    }

    // All pages should be accessible
    let mut accessible_count = 0;
    for &page_id in &page_ids {
        if pool.fetch_page(page_id).is_ok() {
            accessible_count += 1;
            pool.unpin_page(page_id, false).unwrap();
        }
    }
    assert_eq!(accessible_count, capacity + 1);

    // Should have exactly capacity pages
    assert_eq!(pool.current_size(), capacity);
}

#[test]
fn test_concurrent_access_multiple_threads() {
    let (pool, _temp_file) = setup_buffer_pool(10);
    let num_threads = 4;
    let pages_per_thread = 3;

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let pool_clone = Arc::clone(&pool);

        handles.push(thread::spawn(move || {
            let mut page_ids = vec![];

            // Each thread creates and accesses pages
            for i in 0..pages_per_thread {
                let page = pool_clone.create_page().unwrap();
                let page_id = page.read().unwrap().page.get_id();
                page_ids.push(page_id);

                // Simulate some work
                thread::sleep(Duration::from_millis(5));

                // Mark some as dirty based on thread/page pattern
                let is_dirty = (thread_id + i) % 2 == 0;
                pool_clone.unpin_page(page_id, is_dirty).unwrap();
            }

            // Try to access own pages again
            for &page_id in &page_ids {
                let result = pool_clone.fetch_page(page_id);
                if result.is_ok() {
                    pool_clone.unpin_page(page_id, false).unwrap();
                }
            }

            page_ids
        }));
    }

    // Collect all page IDs created
    let mut all_page_ids = vec![];
    for handle in handles {
        let page_ids = handle.join().unwrap();
        all_page_ids.extend(page_ids);
    }

    // Verify pool is in consistent state (not over capacity)
    let current_size = pool.current_size();
    assert!(current_size <= 10, "Buffer pool should not exceed capacity");

    // All pages should be accessible
    let mut accessible_count = 0;
    for &page_id in &all_page_ids {
        if pool.fetch_page(page_id).is_ok() {
            accessible_count += 1;
            pool.unpin_page(page_id, false).unwrap();
        }
    }

    assert_eq!(accessible_count, num_threads * pages_per_thread);
}

#[test]
fn test_pinned_page_protection() {
    let (pool, _temp_file) = setup_buffer_pool(2);

    // Create and keep pinned
    let page1 = pool.create_page().unwrap();
    let page1_id = page1.read().unwrap().page.get_id();

    // Create second page and unpin
    let page2 = pool.create_page().unwrap();
    let page2_id = page2.read().unwrap().page.get_id();
    pool.unpin_page(page2_id, false).unwrap();

    // Create third page - should evict page2, not page1
    let page3 = pool.create_page().unwrap();
    let page3_id = page3.read().unwrap().page.get_id();

    // page1 and page3 should still be pinned
    assert!(pool.fetch_page(page1_id).is_ok());
    assert!(pool.fetch_page(page3_id).is_ok());
}

#[test]
fn test_concurrent_access_same_page() {
    let (pool, _tmp) = setup_buffer_pool(4);

    // Create 1 shared page (keep pinned by creator for reference)
    let shared = pool.create_page().unwrap();
    let shared_id = shared.read().unwrap().page.get_id();

    let n_threads = 8;
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = vec![];

    for _ in 0..n_threads {
        let pool_cl = Arc::clone(&pool);
        let barrier_cl = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            // Everyone starts together
            barrier_cl.wait();

            // Fetch (pin) the same page
            let _ = pool_cl.fetch_page(shared_id).unwrap();

            // Simulate work while holding the pin
            thread::sleep(Duration::from_millis(5));

            // Mark some as dirty, some not — mixed pattern
            let tid = thread::current().id();
            let hash = format!("{:?}", tid).bytes().fold(0u32, |a,b| a.wrapping_add(b as u32));
            let dirty = (hash % 2) == 0;

            pool_cl.unpin_page(shared_id, dirty).unwrap();

            // Return whether we succeeded
            true
        }));
    }

    // Wait for everyone
    for h in handles { h.join().unwrap(); }

    // At this point, only the creator's original pin remains
    let cnt = shared.read().unwrap().pin_count.load(Ordering::SeqCst);
    assert_eq!(cnt, 1, "All threads should have unpinned; one creator pin remains");
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
