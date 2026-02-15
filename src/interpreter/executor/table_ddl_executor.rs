use std::collections::HashMap;
use crate::storage::page::page::{Page, PageError};
use paste::paste;
use crate::compiler::ast::{ColumnDef, Literal, RowDef};
use crate::interpreter::executor::{Executor, ExprContext};
use crate::interpreter::catalog::TableSchema;
use crate::types::NO_FLUSH;
use crate::{with_create_pages, with_read_pages, with_write_pages};
use crate::compiler::bounded_ast::{BoundAssignment, BoundExpr};

impl Executor {

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        let database = ctx.current_db.clone().unwrap();

        // create first table page
        let page_id;
        let storage_engine = ctx.storage_engines.get(&database).unwrap();
        with_create_pages!(storage_engine.buffer_pool, [(page_id, page)], NO_FLUSH, {});
        
        // create column index
        let mut column_index = HashMap::new();
        for i in 0..columns.len() {
            column_index.insert(columns[i].name.clone(), i);
        }

        // insert table information to catalog
        let schema = TableSchema {
            name: String::from(name),
            columns,
            column_index,
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

    pub fn insert_table(&mut self, table: &str, rows: &Vec<RowDef>) -> Result<String, String> {
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

    pub fn update_table(
        &mut self, 
        table: &str, 
        assignments: &Vec<BoundAssignment>,
        selection: &Option<BoundExpr>
    ) -> Result<String, String> {

        let ctx = self.context.read().unwrap();
        let database = ctx.current_db.clone().unwrap();

        let schema = ctx.catalog.get_table_schema(&database, table).unwrap();
        let storage_engine = ctx.storage_engines.get(&database).unwrap();

        let mut page_id = schema.first_page_id;
        let mut next_id;
        let mut updated_count = 0;

        while page_id != 0 {
            with_write_pages!(storage_engine.buffer_pool, [(page_id, page)], NO_FLUSH, {
                // track the records that need re-insertion
                let mut updates = Vec::new();
                next_id = page.get_next_id();

                for (slot_id, record_bytes) in page.iter_record() {
                    let mut row = RowDef::deserialize(record_bytes, &schema.columns)
                        .expect("Error deserializing record");

                    // skip the row only if the condition evaluates to false
                    // no condition means updating every row
                    if let Some(condition) = selection {
                        if let Literal::Bool(false) = self.execute_expression(
                            condition,
                            &ExprContext { row: &row }
                        )? {
                            continue;
                        }
                    }

                    // apply update and serialize result
                    for assign in assignments {
                        row.record[assign.column_id] = assign.value.clone();
                    }
                    let result_bytes = row.serialize().expect("Error serializing result");
                    updates.push((slot_id, result_bytes));
                }

                // apply update to page
                for (slot_id, result_bytes) in updates {
                    match page.update_record(slot_id, &result_bytes) {
                        Ok(_) => { updated_count += 1; },
                        Err(PageError::RecordSizeChanged) => {
                            // the record size has changed, delete the record
                            // and insert a new record in table
                            page.delete_record(slot_id).expect("Error deleting record");

                            let mut insert_page_id = schema.first_page_id;
                            loop {
                                // iterate through table to find space for insertion
                                with_write_pages!( storage_engine.buffer_pool, [(insert_page_id, insert_page)], NO_FLUSH, {
                                    // successfully insert new record
                                    if insert_page.insert_record(&result_bytes).is_some() {
                                        break;
                                    }

                                    // append new pages
                                    if insert_page.get_next_id() == 0 {
                                        let new_page_id;
                                        with_create_pages!(storage_engine.buffer_pool, [(new_page_id, new_page)], NO_FLUSH, {
                                            insert_page.set_next_id(new_page_id);
                                            new_page.insert_record(&result_bytes).expect("Error inserting record to new page");
                                        });
                                        break;
                                    }

                                    insert_page_id = insert_page.get_next_id();
                                });
                            }
                            updated_count += 1;
                        },
                        Err(e) => panic!("Unexpected update error: {:?}", e),
                    }
                }
            });
            page_id = next_id;
        }

        Ok(format!("Updated {} rows in table '{}'", updated_count, table))
    }
}