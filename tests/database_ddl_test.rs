mod common;

use raincloud_db::interpreter::ExecResult;
use raincloud_db::types::{DATA_FILE, HEADER_FILE};
use crate::common::{setup_interpreter, test_sql, assert_sql_success, assert_sql_failure};

#[test]
fn test_create_database() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1;", &mut interpreter);

    let ctx = interpreter.context.read().unwrap();
    let db_path = ctx.dbms_root_dir.join("db1");

    // check database files are created and database exists in catalog
    assert!(db_path.exists());
    assert!(db_path.join(DATA_FILE).exists());
    assert!(db_path.join(HEADER_FILE).exists());
    assert!(ctx.catalog.list_databases().contains(&"db1".to_string()));
    assert!(ctx.storage_engines.contains_key("db1"));
}

#[test]
fn test_connect_disconnect_database() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    
    {
        let ctx = interpreter.context.read().unwrap();
        assert_eq!(ctx.current_db.as_deref(), Some("db1"));
    }

    assert_sql_success("DISCONNECT;", &mut interpreter);
    {
        let ctx = interpreter.context.read().unwrap();
        assert!(ctx.current_db.is_none());
    }
}

#[test]
fn test_drop_database() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    {
        let ctx = interpreter.context.read().unwrap();
        assert!(ctx.dbms_root_dir.join("db1").exists());
        assert!(ctx.storage_engines.contains_key("db1"));
    }

    // there is active connection to database, cannot drop
    assert_sql_failure("DROP DATABASE db1;", &mut interpreter);

    // check database directory is removed and database is removed from catalog
    assert_sql_success("DISCONNECT; DROP DATABASE db1;", &mut interpreter);
    let ctx = interpreter.context.read().unwrap();
    assert!(!ctx.dbms_root_dir.join("db1").exists());
    assert!(!ctx.catalog.list_databases().contains(&"db1".to_string()));
    assert!(!ctx.storage_engines.contains_key("db1"));
}

#[test]
fn test_drop_nonexistent_database() {
    let mut interpreter = setup_interpreter();
   assert_sql_failure("DROP DATABASE undefined;", &mut interpreter);
}