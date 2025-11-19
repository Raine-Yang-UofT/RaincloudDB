use crate::interpreter::catalog::Catalog;

pub struct ExecutionContext {
    pub config_dir: String,
    pub current_db: Option<String>,
    pub catalog: Catalog,
}

impl ExecutionContext {
    pub fn new(config_dir: &str, catalog: Catalog) -> ExecutionContext {
        Self {
            config_dir: config_dir.to_string(),
            current_db: None,
            catalog,
        }
    }

    pub fn set_current_db(&mut self, db: String) {
        self.current_db = Some(db);
    }

    pub fn get_current_db(&self) -> Option<&str> {
        self.current_db.as_deref()
    }
}