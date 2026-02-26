use crate::storage::replacement_strategy::ReplacementStrategyType;

// global constants
// data page configs
pub const PAGE_SIZE: usize = 4096;
pub const MAX_SLOTS: usize = 255;

// global types
pub type PageId = u32;
pub type SlotId = u8;
pub type ColumnId = usize;

// defined constats
pub const FLUSH: bool = true;
pub const NO_FLUSH: bool = false;
pub type DbResult<T> = Result<T, DbError>;
#[derive(Debug)]
pub enum DbError {
    // scanner errors
    ScannerError(String),
    // parser errors
    ParseError(String),
    // interpreter errors
    DatabaseNotFound(String),
    DuplicateDatabase(String),
    ConnectionNotFound(String),
    ConnectionExist(String),
    TableNotFound(String),
    DuplicateTable(String),
    ColumnNotFound(String),
    DuplicateColumn(String),
    ColumnMismatch(String),
    TypeMismatch(String),
    ExpressionNotFound(String),
    InternalError(String),
}

// global file names
pub const DATA_FILE: &str = "data.rcdb";
pub const HEADER_FILE: &str = "header.rcdb";
pub const CATALOG_FILE: &str = "catalog.json";

// default storage engine configuration
pub const DEFAULT_BUFFERPOOL_SIZE: usize = 32;
pub const DEFAULT_BUFFERPOOL_REPLACEMENT: ReplacementStrategyType = ReplacementStrategyType::LRU;
