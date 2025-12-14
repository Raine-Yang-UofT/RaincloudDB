use crate::storage::page::page::Page;
use paste::paste;
use crate::compiler::ast::ColumnDef;
use crate::interpreter::executor::Executor;
use crate::interpreter::catalog::TableSchema;
use crate::types::NO_FLUSH;
use crate::with_create_pages;

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
}