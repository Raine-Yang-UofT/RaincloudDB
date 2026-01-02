mod common;

use raincloud_db::types::{DATA_FILE, HEADER_FILE};
use crate::common::{setup_interpreter, test_sql};

#[test]
fn test_create_database() {
    let mut interpreter = setup_interpreter();
    assert!(test_sql("CREATE DATABASE db1;", &mut interpreter).is_ok());

    let ctx = interpreter.context.read().unwrap();
    let db_path = ctx.dbms_root_dir.join("db1");

    assert!(db_path.exists());
    assert!(db_path.join(DATA_FILE).exists());
    assert!(db_path.join(HEADER_FILE).exists());
    assert!(ctx.catalog.list_databases().contains(&"db1".to_string()));
    assert!(ctx.storage_engines.contains_key("db1"));
}

#[test]
fn test_connect_disconnect_database() {
    let mut interpreter = setup_interpreter();
    assert!(test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).is_ok());
    
    {
        let ctx = interpreter.context.read().unwrap();
        assert_eq!(ctx.current_db.as_deref(), Some("db1"));
    }

    assert!(test_sql("DISCONNECT;", &mut interpreter).is_ok());
    {
        let ctx = interpreter.context.read().unwrap();
        assert!(ctx.current_db.is_none());
    }
}

#[test]
fn test_drop_database() {
    let mut interpreter = setup_interpreter();
    assert!(test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).is_ok());
    {
        let ctx = interpreter.context.read().unwrap();
        assert!(ctx.dbms_root_dir.join("db1").exists());
        assert!(ctx.storage_engines.contains_key("db1"));
    }

    // there is active connection to database, cannot drop
    assert!(test_sql("DROP DATABASE db1;", &mut interpreter).is_err());

    assert!(test_sql("DISCONNECT; DROP DATABASE db1;", &mut interpreter).is_ok());
    let ctx = interpreter.context.read().unwrap();
    assert!(!ctx.dbms_root_dir.join("db1").exists());
    assert!(!ctx.catalog.list_databases().contains(&"db1".to_string()));
    assert!(!ctx.storage_engines.contains_key("db1"));
}

#[test]
fn test_drop_nonexistent_database() {
    let mut interpreter = setup_interpreter();
   assert!(test_sql("DROP DATABASE undefined;", &mut interpreter).is_err());
}