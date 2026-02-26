pub mod execution_context;
pub mod catalog;
pub mod executor;
pub mod analyzer;

use std::path::Path;
use std::sync::{Arc, RwLock};
use execution_context::ExecutionContext;
use catalog::Catalog;
use crate::compiler::ast::Statement;
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::executor::Executor;
use crate::types::DbError::DatabaseNotFound;
use crate::types::DbResult;

#[derive(Debug)]
pub enum ExecResult {
    Success(String),
    AffectedRows(usize, String),
    QueryResult(Vec<Vec<String>>),
}

pub struct Interpreter {
    pub context: Arc<RwLock<ExecutionContext>>,
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

    /// Entry point for interactive SQL interpreter
    pub fn execute(&mut self, stmt: Statement) -> DbResult<ExecResult> {
        // only database-level statements are permitted if a database connection does not exist
        if self.context.read().unwrap().current_db.is_none() {
            if !matches!(stmt,
                Statement::CreateDatabase { name: _ } |
                Statement::DropDatabase { name: _ } |
                Statement::ConnectDatabase { name: _ } |
                Statement::DisconnectDatabase {}
            ) {
                return Err(DatabaseNotFound("A database connection does not exist".to_string()));
            }
        }

        let mut analyzer = Analyzer::new(Arc::clone(&self.context));
        let mut executor = Executor::new(Arc::clone(&self.context));

        match analyzer.analyze(stmt) { 
            Ok(stmt) => executor.execute(stmt),
            Err(msg) => Err(msg),
        }
    }
}