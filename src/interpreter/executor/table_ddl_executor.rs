use crate::storage::page::page::Page;
use paste::paste;
use crate::compiler::ast::{ColumnDef, RowDef};
use crate::interpreter::executor::Executor;
use crate::interpreter::catalog::TableSchema;
use crate::types::NO_FLUSH;
use crate::{with_create_pages, with_read_pages, with_write_pages};

impl Executor {

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        let database = ctx.current_db.clone().unwrap();

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
        let database = ctx.current_db.clone().unwrap();

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

    pub fn insert_table(&mut self, table: &str, rows: Vec<RowDef>) -> Result<String, String> {
        let ctx = self.context.read().unwrap();
        let database = ctx.current_db.clone().unwrap();
        let num_rows = rows.len();

        // write records to pages
        let mut page_id = ctx.catalog.get_table_schema(&database, table).unwrap().first_page_id;
        let storage_engine = ctx.storage_engines.get(&database).unwrap();
        for record in rows {
            let record_bytes = record.serialize().expect("Error serializing record");
            loop {
                // attempt to insert to current page
                with_write_pages!(storage_engine.buffer_pool, [(page_id, page)], NO_FLUSH, {
                    if page.insert_record(&record_bytes).is_none() {
                        // there is no sufficient space in current page
                        if page.get_next_id() == 0 {
                            // reach the end of heap file, append new page
                            let new_page_id;
                            with_create_pages!(storage_engine.buffer_pool, [(new_page_id, new_page)], NO_FLUSH, {
                                page.set_next_id(new_page_id);
                                new_page.insert_record(&record_bytes).expect("Error inserting record to new page");
                            });
                            break;
                        }
                        // try insert to next page
                        page_id = page.get_next_id();
                    } else {
                        break;
                    }
                });
            }
            
        }

        Ok(format!("Insert {} records to table '{}'", num_rows, table))
    }
}