use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::types::PageId;
use crate::storage::page::header_page::{HeaderPage, FREE_HEADER_SIZE};
use crate::storage::disk_manager::{DiskManager};
use crate::storage::page::page::Page;

/// A frame of header page in memory
struct HeaderFrame {
    header: HeaderPage,
    is_dirty: AtomicBool,
}

pub struct FreeList {
    disk: Arc<dyn DiskManager<HeaderPage>>, // disk manager used by free list
    head: Mutex<PageId>,                    // PageId of head page (0 if none)
    cache: Mutex<HashMap<PageId, Arc<Mutex<HeaderFrame>>>>,
}

impl FreeList {

    pub fn new(disk: Arc<dyn DiskManager<HeaderPage>>, head_page_id: PageId) -> Self {
        FreeList {
            disk,
            head: Mutex::new(head_page_id),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a page id from free list
    pub fn allocate(&mut self, flush: bool) -> PageId {
        let start = { *self.head.lock().unwrap() };
        if start == 0 {
            // create first header
            return self.create_and_allocate(flush);
        }

        let mut curr = start;
        loop {
            let entry_arc = self.load_header(curr);
            let mut entry = entry_arc.lock().unwrap();
            if let Some(page_id) = entry.header.allocate_header() {
                if flush {
                    self.disk.write_page(&entry.header);
                }
                entry.is_dirty.store(!flush, Ordering::SeqCst);
                return page_id;
            }

            match entry.header.get_next() {
                Some(next_page) if next_page != 0 => {
                    curr = next_page;
                    continue;
                }
                _ => break,
            }
        }

        // free list is full, create new header page
        self.create_and_allocate(flush)
    }

    /// Deallocate page_id back into free list
    pub fn deallocate(&self, page_id: PageId, flush: bool) -> Result<(), String> {
        let head = { *self.head.lock().unwrap() };
        if head == 0 {
            return Err("freelist empty, cannot deallocate".into());
        }

        let mut curr = head;
        loop {
            let entry_arc = self.load_header(curr);
            let mut entry = entry_arc.lock().unwrap();
            let offset = entry.header.get_offset();
            if page_id >= offset as PageId && page_id < (offset + FREE_HEADER_SIZE) as PageId {
                entry.header.deallocate_header(page_id as usize);
                if flush {
                    self.disk.write_page(&entry.header);
                }
                entry.is_dirty.store(!flush, Ordering::SeqCst);

                // update cache
                return Ok(());
            }

            match entry.header.get_next() {
                Some(next) if next != 0 => {
                    curr = next;
                    continue;
                }
                _ => break,
            }
        }

        Err(format!("no header found covering page id {}", page_id))
    }

    /// Create a new header page to head of list and allocate a page header
    fn create_and_allocate(&mut self, flush: bool) -> PageId {
        let mut head_guard = self.head.lock().unwrap();
        let start = *head_guard;

        if start == 0 {
            // create first header page (id = 1)
            let mut page = HeaderPage::new(1);
            let allocated = page.allocate_header().expect("New header page should have empty slot");

            // update head and cache with the mutated page
            *head_guard = page.get_id();
            self.cache.lock().unwrap().insert(page.get_id(), Arc::new(Mutex::new(HeaderFrame {
                header: page,
                is_dirty: AtomicBool::new(!flush),
            })));

            // optionally flush page
            if flush {
                self.disk.write_page(&page);
            }

            return allocated;
        }

        // append header page after existing start
        let mut page = HeaderPage::new(start + 1);
        let prev_page = self.load_header(start).lock().unwrap().header;
        page.set_next(prev_page.get_id());
        page.set_offset(prev_page.get_offset() + FREE_HEADER_SIZE);

        // allocate on the page BEFORE inserting into cache
        let allocated = page.allocate_header().expect("New header page should have empty slot");

        // update head and insert the mutated page into cache using its real id
        *head_guard = page.get_id();
        self.cache.lock().unwrap().insert(page.get_id(), Arc::new(Mutex::new(HeaderFrame {
            header: page,
            is_dirty: AtomicBool::new(true),
        })));

        allocated
    }

    /// Flush dirty page headers to disk
    pub fn flush_header(&self, page_id: PageId) {
        if let Some(entry) = self.cache.lock().unwrap().get(&page_id) {
            let page = entry.lock().unwrap();
            if page.is_dirty.load(Ordering::SeqCst) {
                self.disk.write_page(&page.header);
                page.is_dirty.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Flush entire header page cache
    pub fn flush_all(&self) {
        for entry in self.cache.lock().unwrap().values() {
            let page = entry.lock().unwrap();
            if page.is_dirty.load(Ordering::SeqCst) {
                self.disk.write_page(&page.header);
                page.is_dirty.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Load header from cache or disk. Caller gets ownership of HeaderEntry.
    fn load_header(&self, page_id: PageId) -> Arc<Mutex<HeaderFrame>> {
        // check cache
        {
            let cache = self.cache.lock().unwrap();
            if let Some(entry) = cache.get(&page_id) {
                return Arc::clone(entry);
            }
        }

        // read from disk
        match self.disk.read_page(page_id) {
            Some(p) => {
                // p is HeaderPage
                let entry = Arc::new(Mutex::new(HeaderFrame {
                    header: p,
                    is_dirty: AtomicBool::new(false),
                }));
                self.cache.lock().unwrap().insert(page_id, Arc::clone(&entry));
                entry
            }
            None => {
                panic!("could not read header page {} from disk", page_id);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;
    use crate::storage::disk_manager::FileDiskManager;
    use crate::types::{FLUSH, NO_FLUSH};
    use super::*;

    fn setup_freelist() -> FreeList {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let disk = Arc::new(FileDiskManager::<HeaderPage>::open(path).unwrap());
        FreeList::new(disk.clone(), 0)
    }

    #[test]
    fn allocate_creates_first_header() {
        let mut freelist = setup_freelist();
        freelist.allocate(FLUSH);

        // After allocation, the disk should contain header page with id 1 (per FreeList implementation)
        freelist.disk.read_page(1).expect("header page 1 should exist on disk");
    }

    #[test]
    fn deallocate_on_empty_returns_err() {
        let freelist = setup_freelist();
        let res = freelist.deallocate(10, FLUSH);
        assert!(res.is_err(), "deallocate on empty freelist should return Err");
    }

    #[test]
    fn deallocate_and_reuse_page() {
        let mut freelist = setup_freelist();
        let header = freelist.allocate(NO_FLUSH);
        freelist.flush_all();

        // deallocate header back
        freelist.deallocate(header, FLUSH).expect("deallocate should succeed");

        // allocate again, should reuse the same page id
        let header2 = freelist.allocate(FLUSH);
        assert_eq!(header, header2, "re-allocated header should be the same as deallocated one");

        // flush header to disk and read back to ensure no panics and data exists
        freelist.flush_header(1);
        freelist.disk.read_page(1).expect("header page 1 should exist");
    }

    #[test]
    fn allocate_second_header_when_full() {
        use crate::storage::page::header_page::FREE_HEADER_SIZE;
        let mut freelist = setup_freelist();

        // fill the first header completely
        let mut allocated = vec![];
        for _ in 0..=(8 * FREE_HEADER_SIZE) {
            let p = freelist.allocate(FLUSH);
            allocated.push(p);
        }

        // next allocation should force creation of a new header page
        let extra = freelist.allocate(FLUSH);
        freelist.flush_all();

        // creates new header page id = start + 1 (start == 1) -> page id 2
        let second_page = freelist.disk.read_page(2);
        assert!(second_page.is_some(), "expected a second header page on disk (id 2)");
        assert_ne!(extra, 0, "allocated page id should be non-zero");
    }

    #[test]
    fn flush_header_persist() {
        let mut freelist = setup_freelist();

        freelist.allocate(NO_FLUSH);
        // flush a specific header page
        freelist.flush_header(1);

        // flush all headers
        freelist.flush_all();

        // confirm header page exists on disk after flushes
        freelist.disk.read_page(1).expect("header page 1 should exist after flush");
        assert_ne!(*freelist.head.get_mut().unwrap(), 0);
    }
}