use tempfile::TempDir;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::{ExecResult, Interpreter};
use raincloud_db::types::DbResult;

pub fn setup_interpreter() -> Interpreter {
    let tmpdir = TempDir::new().expect("create temp dir");
    let dbms_root = tmpdir.path().to_path_buf();
    Interpreter::new(dbms_root)
}

pub fn test_sql(sql: &str, interpreter: &mut Interpreter) -> Vec<DbResult<ExecResult>> {
    let mut scanner = Scanner::new(sql);
    let mut parser = Parser::new(&mut scanner);

    let mut results = Vec::new();
    for stmt in parser.parse().unwrap() {
        results.push(interpreter.execute(stmt));
    }
    results
}

pub fn assert_sql_success(sql: &str, interpreter: &mut Interpreter) {
    for res in test_sql(sql, interpreter) {
        if let Err(err) = res {
            panic!("SQL statement {} failed with error: {:?}", sql, err);
        }
    }
}

pub fn assert_sql_failure(sql: &str, interpreter: &mut Interpreter) {
    for res in test_sql(sql, interpreter) {
        if let Ok(msg) = res {
            panic!("Expect SQL statement {} to fail, got: {:?}", sql, msg);
        }
    }
}
