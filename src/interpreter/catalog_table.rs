use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use crate::compiler::ast::ColumnDef;
use crate::types::{DbError, PageId};

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub first_page_id: PageId,
    // derived fields are reconstructed during runtime
    #[serde(skip)]
    pub column_index: HashMap<String, usize>,
}

impl TableSchema {
    pub fn rebuild_column_index(&mut self) {
        self.column_index = self.columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.to_uppercase(), i))
            .collect();
    }
}

#[derive(Serialize, Deserialize, Default)]
struct CatalogData {
    tables: HashMap<String, TableSchema>,
}

pub struct Catalog {
    tables: HashMap<String, TableSchema>,
    path: PathBuf,
}

impl Catalog {

    pub fn new(path: PathBuf) -> Self {
        Self {
            tables: HashMap::new(),
            path,
        }
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name.to_uppercase().as_str())
    }

    pub fn add_table(&mut self, table_schema: TableSchema) -> Result<(), DbError> {
        self.tables.insert(table_schema.name.to_uppercase(), table_schema);
        self.save_catalog().map_err(|e| DbError::InternalError(e.to_string()))
    }

    pub fn remove_table(&mut self, table: &str) -> Result<(), DbError> {
        self.tables.remove(&table.to_uppercase());
        self.save_catalog().map_err(|e| DbError::InternalError(e.to_string()))
    }

    pub fn get_table_schema(&self, table: &str) -> Option<&TableSchema> {
        self.tables.get(&table.to_uppercase())
    }

    pub fn load_catalog(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(&self.path)?;
        let mut data: CatalogData = serde_json::from_str(&content)?;

        // rebuild derived fields
        for table in data.tables.values_mut() {
            table.rebuild_column_index();
        }

        self.tables = data.tables;
        Ok(())
    }

    pub fn save_catalog(&self) -> Result<(), Box<dyn std::error::Error>> {
        let data = CatalogData {
            tables: self.tables.clone(),
        };

        let json = serde_json::to_string_pretty(&data)?;
        fs::write(&self.path, json)?;

        Ok(())
    }
}
