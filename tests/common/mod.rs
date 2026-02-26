use tempfile::TempDir;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::Interpreter;
use raincloud_db::types::DbError;

pub fn setup_interpreter() -> Interpreter {
    let tmpdir = TempDir::new().expect("create temp dir");
    let dbms_root = tmpdir.path().to_path_buf();
    Interpreter::new(dbms_root)
}

pub fn test_sql(sql: &str, interpreter: &mut Interpreter) -> Result<(), DbError> {
    let mut scanner = Scanner::new(sql);
    let mut parser = Parser::new(&mut scanner);

    for stmt in parser.parse()? {
        interpreter.execute(stmt)?;
    }
    
    Ok(())
}
