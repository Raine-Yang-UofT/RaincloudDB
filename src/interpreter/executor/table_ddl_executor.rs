use crate::storage::page::page::Page;
use paste::paste;
use crate::compiler::ast::ColumnDef;
use crate::interpreter::executor::Executor;
use crate::interpreter::catalog::TableSchema;
use crate::types::NO_FLUSH;
use crate::{with_create_pages, with_read_pages, with_write_pages};

impl Executor {

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();

        // check for duplicate table name in current database
        let database = ctx.current_db.clone().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_some() {
            return Err(format!("The table '{}' already exists", name));
        }

        // create first table page
        let page_id;
        let storage_engine = ctx.storage_engines.get(&database).unwrap();
        with_create_pages!(storage_engine.buffer_pool, [(page_id, page)], NO_FLUSH, {});

        // insert table information to catalog
        let schema = TableSchema {
            name: String::from(name),
            columns,
            first_page_id: page_id,
        };

        match ctx.catalog.add_table(&database, schema) {
            Ok(_) => Ok(format!("Table '{}' created successfully", name)),
            Err(e) => Err(e),
        }
    }

    pub fn drop_table(&mut self, name: &str) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();

        // check if the table exists in database
        let database = ctx.current_db.clone().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_none() {
            return Err(format!("The table '{}' does not exists", name));
        }

        // mark all pages of table as freed
        let mut page_id = ctx.catalog.get_table_schema(&database, name).unwrap().first_page_id;
        let mut next_id;
        let storage_engine = ctx.storage_engines.get(&database).unwrap();
        while page_id != 0 {
            with_read_pages!(storage_engine.buffer_pool, [(page_id, page)], {
                next_id = page.get_next_id();
                storage_engine.buffer_pool.free_page(page_id, NO_FLUSH);
                page_id = next_id
            });
        }

        // remove table information from catalog
        match ctx.catalog.remove_table(&database, name) {
            Ok(_) => Ok(format!("Table '{}' dropped successfully", name)),
            Err(e) => Err(e),
        }
    }
}