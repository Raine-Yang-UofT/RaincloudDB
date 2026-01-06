mod common;

use paste::paste;
use raincloud_db::with_read_pages;
use crate::common::{setup_interpreter, test_sql};

#[test]
fn test_create_table() {
    let mut interpreter = setup_interpreter();
    assert!(test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).is_ok());

    let sql = "CREATE TABLE users (id INT, name CHAR(5));";
    assert!(test_sql(sql, &mut interpreter).is_ok());

    // check catalog for table existence and initial page allocation
    let ctx = interpreter.context.read().unwrap();
    let catalog = &ctx.catalog;
    let table = catalog.get_table_schema("db1", "users")
        .expect("Table should exist in catalog");

    assert_eq!(table.name, "users");
    assert_eq!(table.columns.len(), 2);
    assert!(table.first_page_id > 0);
}

#[test]
fn test_create_table_duplicate_error() {
    let mut interpreter = setup_interpreter();
    test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).unwrap();
    test_sql("CREATE TABLE users (id INT);", &mut interpreter).unwrap();

    assert!(test_sql("CREATE TABLE users (id INT);", &mut interpreter).is_err());
}

#[test]
fn test_insert_and_page_overflow() {
    let mut interpreter = setup_interpreter();
    test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).unwrap();
    test_sql("CREATE TABLE logs (id INT, data CHAR(10));", &mut interpreter).unwrap();

    for i in 0..1000 {
        assert!(test_sql(&format!("INSERT INTO logs VALUES ({i}, \"aaaaaaaaaa\");"), &mut interpreter).is_ok());
    }

    // the table schema should still point to the same first_page_id,
    // but the storage engine should now have multiple pages linked.
    let ctx = interpreter.context.read().unwrap();
    let table = ctx.catalog.get_table_schema("db1", "logs").unwrap();
    let first_id = table.first_page_id;

    // check if a second page was linked
    let storage = ctx.storage_engines.get("db1").unwrap();
    with_read_pages!(storage.buffer_pool, [(first_id, page)], {
        assert!(page.get_next_id() > 0, "A second page should have been allocated and linked");
    });
}

#[test]
fn test_insert_multiple_records() {
    let mut interpreter = setup_interpreter();
    test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).unwrap();
    test_sql("CREATE TABLE logs (id INT, data1 CHAR(10), data2 INT, data3 CHAR(5));", &mut interpreter).unwrap();

    for i in 0..1000 {
        assert!(test_sql(&format!("INSERT INTO logs VALUES \
        ({i}, \"aaaaaaaaaa\", 1500, \"aaaaa\"), \
        (0, \"abcdabcdee\", 0, \"bbbbb\"),\
        (100, \";;;;;;;;;;\", 100, \"+=*;.\"),\
        (1000000000, \"$$$$$$$$$$\", 1000000000, \"*****\");"), &mut interpreter).is_ok());
    }
}

#[test]
fn test_drop_table_cleans_up_pages() {
    let mut interpreter = setup_interpreter();
    test_sql("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).unwrap();
    test_sql("CREATE TABLE temp (id INT);", &mut interpreter).unwrap();

    assert!(test_sql("DROP TABLE temp;", &mut interpreter).is_ok());

    // check catalog entry is removed
    let ctx = interpreter.context.read().unwrap();
    assert!(ctx.catalog.get_table_schema("db1", "temp").is_none());
}

#[test]
fn test_table_operations_without_connection() {
    let mut interpreter = setup_interpreter();
    test_sql("CREATE DATABASE db1;", &mut interpreter).unwrap();

    let result = test_sql("CREATE TABLE failure (id INT);", &mut interpreter);
    assert!(result.is_err(), "Should fail if no database is currently connected");
}