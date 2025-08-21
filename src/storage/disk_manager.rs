use crate::storage::page::{Page};
use crate::types::{PAGE_SIZE, PageId};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

pub trait DiskManager<P: Page>: Send + Sync {
    fn read_page(&self, id: PageId) -> Option<P>;
    fn write_page(&self, page: &P);
    fn allocate_page_id(&self) -> PageId;
}

pub struct FileDiskManager<P: Page> {
    file: Mutex<File>,
    next_page_id: AtomicU64,
    _phantom: std::marker::PhantomData<P>,
}

impl<P: Page> FileDiskManager<P> {

    // open file on disk
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // infer next page id from number of existing pages
        // TODO: consider storing next page id persistently for better crash recovery
        let num_pages = file.metadata()?.len() as usize / PAGE_SIZE;

        Ok(FileDiskManager {
            file: Mutex::new(file),
            next_page_id: AtomicU64::new(num_pages as u64),
            _phantom: std::marker::PhantomData,
        })
    }

    // get offset of page in disk
    fn get_offset(page_id: PageId) -> u64 {
        (page_id as usize * PAGE_SIZE) as u64
    }
}

impl<P: Page> DiskManager<P> for FileDiskManager<P> {

    /// Read a page from disk
    fn read_page(&self, id: PageId) -> Option<P> {
        let mut buf = [0u8; PAGE_SIZE];
        let mut file = self.file.lock().unwrap();

        if file.seek(SeekFrom::Start(Self::get_offset(id))).is_err() {
            return None;
        }

        if file.read_exact(&mut buf).is_err() {
            return None;
        }

        P::deserialize(&buf)
    }

    /// Write a page to disk
    fn write_page(&self, page: &P) {
        let buf = page.serialize();
        let mut file = self.file.lock().unwrap();

        if file.seek(SeekFrom::Start(Self::get_offset(page.get_id()))).is_err() {
            return;
        }

        let _ = file.write_all(&buf);
        let _ = file.flush();
    }

    /// Assign a unique page id
    fn allocate_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as PageId
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use crate::storage::page::Page;
    use crate::storage::data_page::DataPage;

    #[test]
    fn test_open_new_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let disk_manager = FileDiskManager::<DataPage>::open(path).unwrap();

        assert_eq!(disk_manager.next_page_id.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_open_existing_file_with_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Create a file with 2 pages of zeros
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap();
        file.write_all(&[0u8; PAGE_SIZE * 2]).unwrap();

        let disk_manager = FileDiskManager::<DataPage>::open(path).unwrap();
        assert_eq!(disk_manager.next_page_id.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn test_allocate_page_id() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let disk_manager = FileDiskManager::<DataPage>::open(path).unwrap();

        assert_eq!(disk_manager.allocate_page_id(), 0);
        assert_eq!(disk_manager.allocate_page_id(), 1);
        assert_eq!(disk_manager.allocate_page_id(), 2);
    }

    #[test]
    fn test_write_and_read_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let disk_manager = FileDiskManager::open(path).unwrap();

        let page_id = disk_manager.allocate_page_id();
        let page = DataPage::new(page_id);
        // Modify page content here if DataPage has methods to add data

        disk_manager.write_page(&page);
        let read_page = disk_manager.read_page(page_id).unwrap();

        assert_eq!(page.get_id(), read_page.get_id());
        // Add more assertions based on Page's content if modified
    }

    #[test]
    fn test_read_nonexistent_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let disk_manager = FileDiskManager::<DataPage>::open(path).unwrap();

        assert!(disk_manager.read_page(999).is_none());
    }

    #[test]
    fn test_write_page_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let page_id;
        {
            let disk_manager = FileDiskManager::open(path).unwrap();
            page_id = disk_manager.allocate_page_id();
            let page = DataPage::new(page_id);
            disk_manager.write_page(&page);
        }

        // Re-open the file and check persistence
        let disk_manager = FileDiskManager::<DataPage>::open(path).unwrap();
        let read_page = disk_manager.read_page(page_id);
        assert!(read_page.is_some());
        assert_eq!(read_page.unwrap().get_id(), page_id);
    }
}