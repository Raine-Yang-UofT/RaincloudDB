mod common;

use raincloud_db::interpreter::ExecResult;
use raincloud_db::types::DbResult;
use crate::common::{setup_interpreter, assert_sql_success, test_sql};

fn get_rows(result: Vec<DbResult<ExecResult>>) -> Vec<Vec<String>> {
    match result[0].as_ref().unwrap() {
        ExecResult::QueryResult(res) => res.clone(),
        _ => panic!("Expected QueryResult"),
    }
}

#[test]
fn test_select_no_condition() {
    let mut interpreter = setup_interpreter();
    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
             CREATE TABLE users (id INT, name CHAR(5));",
        &mut interpreter,
    );

    assert_sql_success(
        "INSERT INTO users VALUES
        (0, 'aaaaa'),
        (1, 'bbbbb'),
        (2, 'ccccc');",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT id, name FROM users;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["0", "'aaaaa'"]);
    assert_eq!(rows[1], vec!["1", "'bbbbb'"]);
    assert_eq!(rows[2], vec!["2", "'ccccc'"]);
}

#[test]
fn test_select_simple_where() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (id INT, val INT);
         INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT id, val FROM t WHERE id = 2;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["2", "20"]);
}

#[test]
fn test_select_where_expression() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (a INT, b INT);
         INSERT INTO t VALUES (1,2), (2,3), (3,2);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT a, b FROM t WHERE a + b = 5;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["2", "3"]);
    assert_eq!(rows[1], vec!["3", "2"]);
}

#[test]
fn test_select_complex_where() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (x INT, y INT);
         INSERT INTO t VALUES
         (1,1), (2,2), (3,3), (4,4);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT x, y FROM t
         WHERE (x > 1 AND y < 4) OR x = 1;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "1"]);
    assert_eq!(rows[1], vec!["2", "2"]);
    assert_eq!(rows[2], vec!["3", "3"]);
}

#[test]
fn test_select_projection_expression() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (a INT, b INT);
         INSERT INTO t VALUES (1,2), (3,4);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT a + b, a * b FROM t;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["3", "2"]);
    assert_eq!(rows[1], vec!["7", "12"]);
}

#[test]
fn test_select_projection_and_where_expr() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (a INT, b INT);
         INSERT INTO t VALUES (1,2), (2,3), (3,4);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT a * 2, b + 1 FROM t
         WHERE a + b >= 5;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["4", "4"]);
    assert_eq!(rows[1], vec!["6", "5"]);
}

#[test]
fn test_select_no_rows() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (id INT);
         INSERT INTO t VALUES (1), (2), (3);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT id FROM t WHERE id > 10;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 0);
}

#[test]
fn test_select_after_update() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (id INT, v INT);
         INSERT INTO t VALUES (1,10), (2,20);",
        &mut interpreter,
    );

    assert_sql_success(
        "UPDATE t SET v = v + 5 WHERE id = 2;",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT id, v FROM t WHERE id = 2;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["2", "25"]);
}

#[test]
fn test_select_constant_expression() {
    let mut interpreter = setup_interpreter();

    assert_sql_success(
        "CREATE DATABASE db1; CONNECT TO db1;
         CREATE TABLE t (id INT);
         INSERT INTO t VALUES (1), (2);",
        &mut interpreter,
    );

    let rows = get_rows(test_sql(
        "SELECT id, 1 + 2, id * 0 FROM t;",
        &mut interpreter,
    ));

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "3", "0"]);
    assert_eq!(rows[1], vec!["2", "3", "0"]);
}
