mod common;

use paste::paste;
use raincloud_db::interpreter::ExecResult;
use raincloud_db::with_read_pages;
use crate::common::{test_sql, setup_interpreter, assert_sql_success, assert_sql_failure};

#[test]
fn test_create_table() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);

    let sql = "CREATE TABLE users (id INT, name CHAR(5));";
    assert_sql_success(sql, &mut interpreter);

    // check catalog for table existence and initial page allocation
    let ctx = interpreter.context.read().unwrap();
    let catalog = &ctx.catalog;
    let table = catalog.get_table_schema("db1", "users")
        .expect("Table should exist in catalog");

    assert_eq!(table.name, "USERS");
    assert_eq!(table.columns.len(), 2);
    assert!(table.first_page_id > 0);
}

#[test]
fn test_create_table_duplicate_error() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE users (id INT);", &mut interpreter);
    assert_sql_failure("CREATE TABLE users (id INT);", &mut interpreter);
}

#[test]
fn test_create_table_duplicate_column_error() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_failure("CREATE TABLE users (id INT, id CHAR(10));", &mut interpreter);
}

#[test]
fn test_insert_and_page_overflow() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE logs (id INT, data CHAR(10));", &mut interpreter);

    for i in 0..1000 {
        assert_sql_success(&format!("INSERT INTO logs VALUES ({i}, \"aaaaaaaaaa\");"), &mut interpreter);
    }

    // check all records are present
    let result =
        test_sql("SELECT ID, DATA FROM LOGS WHERE data=\"aaaaaaaaaa\";", &mut interpreter);
    if let ExecResult::QueryResult(res) = result[0].as_ref().unwrap() {
        for (i, record) in res.iter().enumerate() {
            assert_eq!(*record, vec![i.to_string().as_str(), "'aaaaaaaaaa'"]);
        }
    } else {
        panic!("Expected QueryResult return type");
    }

    // the table schema should still point to the same first_page_id,
    // but the storage engine should now have multiple pages linked.
    let ctx = interpreter.context.read().unwrap();
    let table = ctx.catalog.get_table_schema("db1", "logs").unwrap();
    let first_id = table.first_page_id;

    // check if a second page was linked
    let storage = ctx.storage_engines.get("DB1").unwrap();
    with_read_pages!(storage.buffer_pool, [(first_id, page)], {
        assert!(page.get_next_id() > 0, "A second page should have been allocated and linked");
    });
}

#[test]
fn test_insert_multiple_records() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE logs (id INT, data1 CHAR(10), data2 INT, data3 CHAR(5));", &mut interpreter);

    for i in 0..1000 {
        assert_sql_success(&format!("INSERT INTO logs VALUES \
        ({i}, \"aaaaaaaaaa\", 1500, \"aaaaa\"), \
        (0, \"abcdabcdee\", 0, \"bbbbb\"),\
        (100, \";;;;;;;;;;\", 100, \"+=*;.\"),\
        (1000000000, \"$$$$$$$$$$\", 1000000000, \"*****\");"), &mut interpreter);
    }

    // validate total row count
    let result = test_sql("SELECT ID, DATA1, DATA2, DATA3 FROM LOGS;", &mut interpreter);
    let rows = match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res,
        _ => panic!("Expected QueryResult"),
    };
    assert_eq!(rows.len(), 4000);
}

#[test]
fn test_drop_table_cleans_up_pages() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT);", &mut interpreter);

    assert_sql_success("DROP TABLE temp;", &mut interpreter);

    // check catalog entry is removed
    let ctx = interpreter.context.read().unwrap();
    assert!(ctx.catalog.get_table_schema("db1", "temp").is_none());
}

#[test]
fn test_update_table_single_row() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
    assert_sql_success("UPDATE temp SET name = \"bar  \" WHERE id = 0;", &mut interpreter);

    // check the row is updated
    let result = test_sql("SELECT ID, NAME FROM TEMP;", &mut interpreter);
    let rows = match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res,
        _ => panic!("Expected QueryResult"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["0", "'bar  '"]);
}

#[test]
fn test_update_no_matching_rows() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
    assert_sql_success("UPDATE temp SET name = \"bar  \" WHERE id = 999;", &mut interpreter);

    // check no update happens
    let result = test_sql("SELECT ID, NAME FROM TEMP;", &mut interpreter);
    let rows = match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res,
        _ => panic!("Expected QueryResult"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["0", "'foo  '"]);
}

#[test]
fn test_update_all_rows() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (1, \"baz  \");", &mut interpreter);
    assert_sql_success("UPDATE temp SET name = \"bar  \";", &mut interpreter);

    // check the rows are updated
    let result = test_sql("SELECT ID, NAME FROM TEMP;", &mut interpreter);
    let rows = match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res,
        _ => panic!("Expected QueryResult"),
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["0", "'bar  '"]);
    assert_eq!(rows[1], vec!["1", "'bar  '"]);
}

#[test]
fn test_update_multiple_assignments() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
    assert_sql_success("UPDATE temp SET id = 10, name = \"bar  \" WHERE id = 0;", &mut interpreter);

    // check the rows are updated
    let result = test_sql("SELECT ID, NAME FROM TEMP;", &mut interpreter);
    let rows = match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res,
        _ => panic!("Expected QueryResult"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["10", "'bar  '"]);
}

// TODO: add support for literal = literal in parser
// #[test]
// fn test_update_with_constant_expression() {
//     let mut interpreter = setup_interpreter();
//
//     assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter).unwrap();
//     assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter).unwrap();
//     assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter).unwrap();
//
//     assert!(assert_sql_success(
//         "UPDATE temp SET name = \"bar  \" WHERE 1 = 1;\
//         UPDATE tem SET id = 6 WHERE 1 = 0;",
//         &mut interpreter
//     ).is_ok());
// }

#[test]
fn test_update_type_mismatch() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
    assert_sql_failure("UPDATE temp SET id = \"hello\" WHERE id = 0;", &mut interpreter);
}

#[test]
fn test_update_invalid_column() {
    let mut interpreter = setup_interpreter();

    assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
    assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
    assert_sql_failure("UPDATE temp SET age = 10;", &mut interpreter);
}

// TODO: require parser refactoring for full expression support
// #[test]
// fn test_update_invalid_predicate_type() {
//     let mut interpreter = setup_interpreter();
//
//     assert_sql_success("CREATE DATABASE db1; CONNECT TO db1;", &mut interpreter);
//     assert_sql_success("CREATE TABLE temp (id INT, name CHAR(5));", &mut interpreter);
//     assert_sql_success("INSERT INTO temp VALUES (0, \"foo  \");", &mut interpreter);
//     assert_sql_failure("UPDATE temp SET name = \"bar  \" WHERE name;", &mut interpreter);
// }

#[test]
fn test_table_operations_without_connection() {
    let mut interpreter = setup_interpreter();
    assert_sql_success("CREATE DATABASE db1;", &mut interpreter);
    assert_sql_failure("CREATE TABLE failure (id INT);", &mut interpreter);
}
