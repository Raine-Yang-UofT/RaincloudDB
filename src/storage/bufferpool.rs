use crate::storage::page::{Page, PageError};
use crate::storage::disk_manager::{DiskManager};
use std::collections::HashMap;
use std::sync::{RwLock, Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::storage::replacement_strategy::{
    ReplacementStrategy, ReplacementStrategyType, replacement_strategy_factory
};
use crate::types::{PageId};

#[derive(Debug)]
pub struct BufferFrame<P: Page> {
    pub page: RwLock<P>,
    pub is_dirty: AtomicBool,    // if the page is modified
    pub pin_count: AtomicUsize   // number of clients using the page
}

// RAII wrapper for BufferFrame pin count update
pub struct PageGuard<P: Page> {
    pub frame: Arc<BufferFrame<P>>,
    pool: Arc<BufferPool<P>>,
    is_dirty: bool,
}

impl<P: Page> PageGuard<P> {
    pub fn new(frame: Arc<BufferFrame<P>>, pool: Arc<BufferPool<P>>) -> PageGuard<P> {
        frame.pin_count.fetch_add(1, Ordering::SeqCst);
        Self { frame, pool, is_dirty: false }
    }

    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, P> {
        self.frame.page.read().unwrap()
    }

    pub fn write(&mut self) -> std::sync::RwLockWriteGuard<'_, P> {
        self.is_dirty = true;
        self.frame.page.write().unwrap()
    }

}

impl<P: Page> Drop for PageGuard<P> {
    fn drop(&mut self) {
        // decrement pin count
        self.frame.pin_count.fetch_sub(1, Ordering::SeqCst);
        if self.is_dirty {
            self.frame.is_dirty.store(true, Ordering::SeqCst);
        }

        // notify evict condvar if pin count hits zero
        if self.frame.pin_count.load(Ordering::SeqCst) == 0 {
            let (lock, cv) = &self.pool.evict_cv;
            let _g = lock.lock().unwrap();
            cv.notify_one();
        }
    }
}

pub struct BufferPool<P: Page> {
    page_table: RwLock<HashMap<PageId, Arc<BufferFrame<P>>>>,
    capacity: usize,
    disk: Arc<dyn DiskManager<P>>,
    strategy: RwLock<Box<dyn ReplacementStrategy>>,
    evict_cv: (Mutex<()>, Condvar)  // condvar to notify an eviction is available
}

impl<P: Page + 'static> BufferPool<P> {
    pub fn new(
        capacity: usize,
        strategy_type: ReplacementStrategyType,
        disk: Arc<dyn DiskManager<P>>,
    ) -> Self {
        let strategy = replacement_strategy_factory(
            strategy_type
        );

        BufferPool {
            page_table: RwLock::new(HashMap::new()),
            capacity,
            disk,
            strategy: RwLock::new(strategy),
            evict_cv: (Mutex::new(()), Condvar::new())
        }
    }

    /// Get the current bufferpool size
    pub fn current_size(&self) -> usize {
        self.page_table.read().unwrap().len()
    }

    /// Fetch a page into memory, pinning it. Evicts a page if needed (blocking).
    pub fn fetch_page(self: &Arc<Self>, page_id: PageId) -> Result<PageGuard<P>, PageError> {
        // page is already in memory
        {
            let frames = self.page_table.read().unwrap();
            if let Some(frame) = frames.get(&page_id) {
                self.strategy.write().unwrap().update(page_id);
                return Ok(PageGuard::new(Arc::clone(frame), Arc::clone(self)));
            }
        }

        // fetch page from disk
        let page = self.disk.read_page(page_id).ok_or(PageError::InvalidPage)?;

        // evict if full
        loop {
            let need_evict = {
                let frames = self.page_table.read().unwrap();
                frames.len() >= self.capacity
            };
            if !need_evict {
                break;
            }
            self.evict_one(); // will block until space is available
        }

        let frame = Arc::new(BufferFrame {
            page: RwLock::new(page),
            is_dirty: AtomicBool::new(false),
            pin_count: AtomicUsize::new(0),
        });

        let mut frames = self.page_table.write().unwrap();

        // possible race condition: another thread may have inserted the same page meanwhile.
        if let Some(existing) = frames.get(&page_id) {
            self.strategy.write().unwrap().update(page_id);
            return Ok(PageGuard::new(Arc::clone(existing), Arc::clone(self)));
        }

        frames.insert(page_id, Arc::clone(&frame));
        self.strategy.write().unwrap().update(page_id);
        Ok(PageGuard::new(frame, Arc::clone(self)))
    }

    /// Create a new in-memory page
    pub fn create_page(self: &Arc<Self>) -> Result<PageGuard<P>, PageError> {
        let page_id = self.disk.allocate_page_id();
        let page = P::new(page_id);

        // evict if full
        loop {
            let need_evict = {
                let frames = self.page_table.read().unwrap();
                frames.len() >= self.capacity
            };
            if !need_evict {
                break;
            }
            self.evict_one();
        }

        let frame = Arc::new(BufferFrame {
            page: RwLock::new(page),
            is_dirty: AtomicBool::new(true), // new page needs to be written to disk
            pin_count: AtomicUsize::new(0),
        });

        {
            let mut frames = self.page_table.write().unwrap();
            // page id should be unique, but if some logic reuses ids, guard anyway:
            if let Some(existing) = frames.get(&page_id) {
                self.strategy.write().unwrap().update(page_id);
                return Ok(PageGuard::new(Arc::clone(existing), Arc::clone(self)));
            }
            frames.insert(page_id, Arc::clone(&frame));
        }
        self.strategy.write().unwrap().update(page_id);
        Ok(PageGuard::new(frame, Arc::clone(self)))
    }

    /// Flush a single dirty page in buffer pool
    pub fn flush_page(&self, page_id: PageId) -> Result<(), PageError> {
        let frame = self.page_table.read().unwrap()
            .get(&page_id)
            .ok_or(PageError::InvalidPage)?
            .clone();

        if frame.is_dirty.swap(false, Ordering::SeqCst) {
            let page = frame.page.read().unwrap();
            self.disk.write_page(&page);
        }

        Ok(())
    }

    /// Flush all dirty pages in buffer pool
    pub fn flush_all(&self) {
        // clone refs to avoid holding the table lock during I/O
        let frames: Vec<Arc<BufferFrame<P>>> = {
            let map = self.page_table.read().unwrap();
            map.values().cloned().collect()
        };
        for frame in frames {
            if frame.pin_count.load(Ordering::SeqCst) == 0 && frame.is_dirty.load(Ordering::SeqCst) {
                let page = frame.page.read().unwrap();
                self.disk.write_page(&page);
            }
        }
    }

    /// Remove a connection using the page, optionally marking it dirty
    /// Notify bufferpool to be able to make eviction
    #[deprecated]
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<(), PageError> {
        // let frames = self.page_table.read().unwrap();
        // if let Some(frame) = frames.get(&page_id) {
        //     let fg = frame.write().unwrap();
        //     if is_dirty {
        //         fg.is_dirty.store(true, Ordering::SeqCst);
        //     }
        //
        //     // prevent underflow: disallow unpin when already 0
        //     let current = fg.pin_count.load(Ordering::SeqCst);
        //     if current == 0 {
        //         return Err(PageError::PageAlreadyUnpinned);
        //     }
        //
        //     let prev = fg.pin_count.fetch_sub(1, Ordering::SeqCst);
        //     if prev == 1 {
        //         // page is unpinned, notify eviction waiters
        //         let (_, cv) = &self.evict_cv;
        //         cv.notify_all();
        //     }
        //     Ok(())
        // } else {
        //     Err(PageError::InvalidPage)
        // }
        Ok(())
    }

    /// Evict one unpinned page using the replacement strategy.
    /// If the bufferpool is full and no page is available for eviction,
    /// evict_one will block until a page can be evicted
    fn evict_one(&self) {
        loop {
            let mut evicted = false;

            // restrict strategy to inner scope so that we do not hold lock while waiting
            {
                let mut strategy = self.strategy.write().unwrap();
                let candidates = strategy.get_evict();

                for evicted_id in candidates {
                    let evicted_ref = {
                        let frames = self.page_table.read().unwrap();
                        if let Some(r) = frames.get(&evicted_id) {
                            Arc::clone(r)
                        } else {
                            continue;
                        }
                    };

                    // skip pinned page
                    if evicted_ref.pin_count.load(Ordering::SeqCst) != 0 {
                        continue;
                    }

                    // flush page if it's dirty
                    if evicted_ref.is_dirty.swap(false, Ordering::SeqCst) {
                        let page_read = evicted_ref.page.read().unwrap();
                        self.disk.write_page(&*page_read);
                    }

                    // remove from page table (perform ABA checking)
                    let mut frames = self.page_table.write().unwrap();
                    if frames.get(&evicted_id).map(|a| Arc::ptr_eq(a, &evicted_ref))
                        .unwrap_or(false)
                    {
                        frames.remove(&evicted_id);
                        evicted = true;
                        break;
                    }
                }
                if evicted {
                    return;
                }
            }

            // if none were evictable, wait until unpin happens
            let (lock, cv) = &self.evict_cv;
            let g = lock.lock().unwrap();
            let _g = cv.wait(g).unwrap();
        }
    }
}
