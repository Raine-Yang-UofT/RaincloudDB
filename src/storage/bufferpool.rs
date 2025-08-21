use crate::storage::page::{Page, PageError};
use crate::storage::disk_manager::{DiskManager};
use std::collections::HashMap;
use std::sync::{RwLock, Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::storage::replacement_strategy::{
    ReplacementStrategy, ReplacementStrategyType, replacement_strategy_factory
};
use crate::types::{PageId};

type PageRef<P> = Arc<RwLock<BufferFrame<P>>>;

#[derive(Debug)]
pub struct BufferFrame<P: Page> {
    pub page: P,
    pub is_dirty: bool,    // if the page is modified
    pub pin_count: AtomicUsize   // number of clients using the page
}

pub struct BufferPool<P: Page> {
    page_table: RwLock<HashMap<PageId, PageRef<P>>>,
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
    pub fn fetch_page(&self, page_id: PageId) -> Result<PageRef<P>, PageError> {
        // page is already in memory
        {
            let frames = self.page_table.read().unwrap();
            if let Some(frame) = frames.get(&page_id) {
                {
                    let fg = frame.read().unwrap();
                    fg.pin_count.fetch_add(1, Ordering::SeqCst);
                }
                self.strategy.write().unwrap().update(page_id);
                return Ok(Arc::clone(frame));
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

        let frame = Arc::new(RwLock::new(BufferFrame {
            page,
            is_dirty: false,
            pin_count: AtomicUsize::new(1),
        }));

        let mut frames = self.page_table.write().unwrap();

        // possible race condition: another thread may have inserted the same page meanwhile.
        if let Some(existing) = frames.get(&page_id) {
            // Use the existing one; bump pin and return it.
            {
                let fg = existing.read().unwrap();
                fg.pin_count.fetch_add(1, Ordering::SeqCst);
            }
            self.strategy.write().unwrap().update(page_id);
            return Ok(Arc::clone(existing));
        }

        frames.insert(page_id, Arc::clone(&frame));
        self.strategy.write().unwrap().update(page_id);
        Ok(frame)
    }

    /// Create a new in-memory page
    pub fn create_page(&self) -> Result<PageRef<P>, PageError> {
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

        let frame = Arc::new(RwLock::new(BufferFrame {
            page,
            is_dirty: true, // new page needs to be written to disk
            pin_count: AtomicUsize::new(1),
        }));

        {
            let mut frames = self.page_table.write().unwrap();
            // page id should be unique, but if some logic reuses ids, guard anyway:
            if let Some(existing) = frames.get(&page_id) {
                {
                    let mut fg = existing.write().unwrap();
                    fg.pin_count.fetch_add(1, Ordering::SeqCst);
                    fg.is_dirty = true;
                }
                self.strategy.write().unwrap().update(page_id);
                return Ok(Arc::clone(existing));
            }
            frames.insert(page_id, Arc::clone(&frame));
        }
        self.strategy.write().unwrap().update(page_id);
        Ok(frame)
    }

    /// Flush a single dirty page in buffer pool
    pub fn flush_page(&self, page_id: PageId) -> Result<(), PageError> {
        let frames = self.page_table.read().unwrap();
        if let Some(frame) = frames.get(&page_id) {
            let fg = frame.read().unwrap();

            // only flush pages not being used (pin_count == 0)
            if fg.pin_count.load(Ordering::SeqCst) == 0 {
                // write dirty page to disk
                if fg.is_dirty {
                    self.disk.write_page(&fg.page);
                }
                Ok(())
            } else {
                Err(PageError::PageLatched)
            }
        } else {
            Err(PageError::InvalidPage)
        }
    }

    /// Flush all dirty pages in buffer pool
    pub fn flush_all(&self) {
        // clone refs to avoid holding the table lock during I/O
        let frames: Vec<PageRef<P>> = {
            let map = self.page_table.read().unwrap();
            map.values().cloned().collect()
        };
        for frame in frames {
            let fg = frame.read().unwrap();
            if fg.pin_count.load(Ordering::SeqCst) == 0 && fg.is_dirty {
                self.disk.write_page(&fg.page);
            }
        }
    }

    /// Remove a connection using the page, optionally marking it dirty
    /// Notify bufferpool to be able to make eviction
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<(), PageError> {
        let frames = self.page_table.read().unwrap();
        if let Some(frame) = frames.get(&page_id) {
            let mut fg = frame.write().unwrap();
            if is_dirty {
                fg.is_dirty = true;
            }

            // prevent underflow: disallow unpin when already 0
            let current = fg.pin_count.load(Ordering::SeqCst);
            if current == 0 {
                return Err(PageError::PageAlreadyUnpinned);
            }

            let prev = fg.pin_count.fetch_sub(1, Ordering::SeqCst);
            if prev == 1 {
                // page is unpinned, notify eviction waiters
                let (_, cv) = &self.evict_cv;
                cv.notify_all();
            }
            Ok(())
        } else {
            Err(PageError::InvalidPage)
        }
    }

    /// Evict one unpinned page using the replacement strategy.
    /// If the bufferpool is full and no page is available for eviction,
    /// evict_one will block until a page can be evicted
    fn evict_one(&self) {
        loop {
            {
                // restrict strategy to inner scope so that we do not hold lock while waiting
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

                    if let Ok(mut fg) = evicted_ref.try_write() {
                        // skip pinned page
                        if fg.pin_count.load(Ordering::SeqCst) != 0 {
                            continue;
                        }

                        // flush page if it's dirty
                        if fg.is_dirty {
                            self.disk.write_page(&fg.page);
                            fg.is_dirty = false;
                        }

                        let mut frames = self.page_table.write().unwrap();
                        // ensure we're removing the same Arc (ABA check)
                        let still_same = frames
                            .get(&evicted_id)
                            .map(|a| Arc::ptr_eq(a, &evicted_ref))
                            .unwrap_or(false);
                        if still_same {
                            frames.remove(&evicted_id);
                            return;
                        }
                    };
                }
            }

            // if none were evictable, wait until unpin happens
            let (lock, cv) = &self.evict_cv;
            let g = lock.lock().unwrap();
            let _g = cv.wait(g).unwrap();
        }
    }
}
