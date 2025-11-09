use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::ast::*;

fn parse_sql(sql: &str) -> Vec<Statement> {
    let mut scanner = Scanner::new(sql);
    let mut parser = Parser::new(&mut scanner);
    parser.parse().unwrap()
}

#[test]
fn test_create_database() {
    let sql = "CREATE DATABASE testdb;";
    let stmts = parse_sql(sql);

    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateDatabase { name } => assert_eq!(name, "testdb"),
        _ => panic!("Expected CreateDatabase statement"),
    }
}

#[test]
fn test_drop_database() {
    let sql = "DROP DATABASE testdb;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::DropDatabase { name } => assert_eq!(name, "testdb"),
        _ => panic!("Expected DropDatabase statement"),
    }
}

#[test]
fn test_use_database() {
    let sql = "USE testdb;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::UseDatabase { name } => assert_eq!(name, "testdb"),
        _ => panic!("Expected UseDatabase statement"),
    }
}

#[test]
fn test_create_table() {
    let sql = "CREATE TABLE users (id INT, name CHAR(10));";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::CreateTable { name, columns } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "id");
            assert!(matches!(columns[0].data_type, DataType::Int));
            assert_eq!(columns[1].name, "name");
            assert!(matches!(columns[1].data_type, DataType::Char(10)));
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn test_insert() {
    let sql = "INSERT INTO users VALUES (1, 'Alice');";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values.len(), 2);
            assert!(matches!(values[0], Literal::Int(1)));
            if let Literal::String(s) = &values[1] {
                assert_eq!(s, "Alice");
            } else {
                panic!("Expected Literal::String");
            }
        }
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_update_with_where() {
    let sql = "UPDATE users SET name = 'Bob' WHERE id = 1;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Update { table, assignments, selection } => {
            assert_eq!(table, "users");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].column, "name");
            assert!(matches!(assignments[0].value, Literal::String(ref s) if s == "Bob"));
            match selection {
                Some(Expression::Equals(l, r)) => {
                    assert!(matches!(**l, Expression::Identifier(ref name) if name == "id"));
                    assert!(matches!(**r, Expression::Literal(Literal::Int(1))));
                }
                _ => panic!("Expected equality expression"),
            }
        }
        _ => panic!("Expected Update statement"),
    }
}

#[test]
fn test_select_with_where() {
    let sql = "SELECT name, age FROM users WHERE id = 1;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Select { columns, table, selection } => {
            assert_eq!(columns, &vec!["name".to_string(), "age".to_string()]);
            assert_eq!(table, "users");
            match selection {
                Some(Expression::Equals(l, r)) => {
                    assert!(matches!(**l, Expression::Identifier(ref name) if name == "id"));
                    assert!(matches!(**r, Expression::Literal(Literal::Int(1))));
                }
                _ => panic!("Expected WHERE id = 1"),
            }
        }
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_drop_table() {
    let sql = "DROP TABLE users;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::DropTable { name } => assert_eq!(name, "users"),
        _ => panic!("Expected DropTable"),
    }
}

#[test]
fn test_multiple_statements() {
    let sql = "
            CREATE DATABASE db1;
            CREATE TABLE t1 (id INT);
            DROP DATABASE db1;
        ";
    let stmts = parse_sql(sql);
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0], Statement::CreateDatabase { .. }));
    assert!(matches!(stmts[1], Statement::CreateTable { .. }));
    assert!(matches!(stmts[2], Statement::DropDatabase { .. }));
}

#[test]
fn test_parse_error_missing_semicolon() {
    let sql = "CREATE DATABASE testdb";
    let mut scanner = Scanner::new(sql);
    let mut parser = Parser::new(&mut scanner);
    let err = parser.parse().unwrap_err();
    assert!(
        err.contains("Expected Semicolon") || err.contains("Expected TokenType::Semicolon"),
        "Error should mention missing semicolon, got: {}",
        err
    );
}