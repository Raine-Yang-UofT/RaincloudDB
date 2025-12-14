pub mod execution_context;
pub mod catalog;
pub mod executor;

use std::path::Path;
use std::sync::{Arc, RwLock};
use execution_context::ExecutionContext;
use catalog::Catalog;
use crate::compiler::ast::Statement;
use crate::interpreter::executor::Executor;

pub struct Interpreter {
    context: Arc<RwLock<ExecutionContext>>,
}

impl Interpreter {
    pub fn new(dbms_root_dir: impl AsRef<Path>) -> Self {
        let dbms_root_dir = dbms_root_dir.as_ref().to_path_buf();

        let catalog = Catalog::new(&dbms_root_dir);
        let databases = catalog.list_databases();

        let context = Arc::new(RwLock::new(
            ExecutionContext::new(dbms_root_dir.clone(), catalog)
        ));


        // initialize one storage engine for every database in catalog
        {
            let mut ctx = context.write().unwrap();
            for db_name in databases {
                ctx.initialize_storage_engine(db_name)
                    .expect("Failed to register storage engine");
            }
        }

        Self { context }
    }

    /// Entry point for SQL interpreter
    pub fn execute(&mut self, stmt: Statement) -> Result<String, String> {
        let mut executor = Executor::new(Arc::clone(&self.context));
        executor.execute(stmt)
    }
}