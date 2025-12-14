pub mod bufferpool;
pub mod disk_manager;
pub mod replacement_strategy;
pub mod bplus_tree;
pub mod page;
pub mod free_list;

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use bufferpool::BufferPool;
use disk_manager::{DiskManager, FileDiskManager};
use free_list::FreeList;
use page::data_page::DataPage;
use page::header_page::HeaderPage;
use replacement_strategy::ReplacementStrategyType;
use crate::types::{DATA_FILE, HEADER_FILE};

#[derive(Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub database_dir: PathBuf,
    pub bufferpool_capacity: usize,
    pub bufferpool_replacement_strategy: ReplacementStrategyType,
}

pub struct StorageEngine {
    // primary buffer pool for DataPage
    pub buffer_pool: Arc<BufferPool<DataPage>>,

    // underlying disk manager for data pages
    pub data_disk: Arc<dyn DiskManager<DataPage>>,

    // disk manager used by the free list (header pages)
    pub header_disk: Arc<dyn DiskManager<HeaderPage>>,

    // free list (manages free pages; uses header_disk internally)
    pub free_list: Arc<Mutex<FreeList>>,
}

impl StorageEngine {
    /// Create a storage engine given a config. This will:
    /// - open a data file manager,
    /// - open a header file manager (for freelist/metadata),
    /// - construct the FreeList,
    /// - build the BufferPool<DataPage>.
    pub fn new(config: StorageConfig) -> io::Result<Self> {
        // prepare paths
        let header_path = config.database_dir.join(HEADER_FILE);
        let data_path = config.database_dir.join(DATA_FILE);

        // open disk managers
        let header_disk = Arc::new(FileDiskManager::<HeaderPage>::open(&header_path)?);
        let data_disk = Arc::new(FileDiskManager::<DataPage>::open(&data_path)?);

        // init freelist with given head page id
        let freelist =  Arc::new(Mutex::new(FreeList::new(header_disk.clone(), 0)));

        // build buffer pool for DataPage
        let buffer_pool = BufferPool::new(
            config.bufferpool_capacity,
            config.bufferpool_replacement_strategy,
            data_disk.clone(),
            Arc::clone(&freelist),
        );

        Ok(StorageEngine {
            buffer_pool: Arc::new(buffer_pool),
            data_disk,
            header_disk,
            free_list: Arc::clone(&freelist),
        })
    }
    
    /// Deactivate storage engine and flush all dirty pages
    pub fn deactivate(&mut self) {
        self.buffer_pool.flush_all();
        self.free_list.lock().unwrap().flush_all();
    }
}