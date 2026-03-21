use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::ast::*;

fn parse_sql(sql: &str) -> Vec<Statement> {
    let mut scanner = Scanner::new(sql);
    let mut parser = Parser::new(&mut scanner).unwrap();
    parser.parse().unwrap()
}

#[test]
fn test_create_database() {
    let sql = "CREATE DATABASE testdb;";
    let stmts = parse_sql(sql);

    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateDatabase { name } => assert_eq!(name, "TESTDB"),
        _ => panic!("Expected CreateDatabase statement"),
    }
}

#[test]
fn test_drop_database() {
    let sql = "DROP DATABASE testdb;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::DropDatabase { name } => assert_eq!(name, "TESTDB"),
        _ => panic!("Expected DropDatabase statement"),
    }
}

#[test]
fn test_connect_database() {
    let sql = "CONNECT TO tesTdb;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::ConnectDatabase { name } => assert_eq!(name, "TESTDB"),
        _ => panic!("Expected ConnectDatabase statement"),
    }
}

#[test]
fn test_disconnect_database() {
    let sql = "DISCONNECT;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::DisconnectDatabase { } => assert!(true),
        _ => panic!("Expected DisconnectDatabase statement"),
    }
}

#[test]
fn test_create_table() {
    let sql = "CREATE TABLE users (id INT, name CHAR(10));";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::CreateTable { name, columns } => {
            assert_eq!(name, "USERS");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "ID");
            assert!(matches!(columns[0].data_type, DataType::Int));
            assert_eq!(columns[1].name, "NAME");
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
        Statement::Insert { table, rows } => {
            assert_eq!(table, "USERS");
            assert_eq!(rows.len(), 1);

            if let Expression::Literal(Literal::Int(v)) = &rows[0][0] {
                assert_eq!(*v, 1);
            } else {
                panic!("Expected Literal::Int");
            }

            if let Expression::Literal(Literal::String(s)) = &rows[0][1] {
                assert_eq!(s, "Alice");
            } else {
                panic!("Expected Literal::String");
            }
        }
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_insert_multiple() {
    let sql = "INSERT INTO Users VALUES (1, 'Alice'), (2, 'Bob');";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Insert { table, rows } => {
            assert_eq!(table, "USERS");
            assert_eq!(rows.len(), 2);

            if let Expression::Literal(Literal::Int(v)) = &rows[0][0] {
                assert_eq!(*v, 1);
            } else {
                panic!("Expected Literal::Int");
            }

            if let Expression::Literal(Literal::String(s)) = &rows[0][1] {
                assert_eq!(s, "Alice");
            } else {
                panic!("Expected Literal::String");
            }

            if let Expression::Literal(Literal::Int(v)) = &rows[1][0] {
                assert_eq!(*v, 2);
            } else {
                panic!("Expected Literal::Int");
            }

            if let Expression::Literal(Literal::String(s)) = &rows[1][1] {
                assert_eq!(s, "Bob");
            } else {
                panic!("Expected Literal::String");
            }
        }
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_insert_multiple_expression_rows() {
    let sql = "INSERT INTO users VALUES (1+2, 3*4), (10-5, 8/2);";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Insert { rows, .. } => {

            match &rows[0][0] {
                Expression::Binary { op, .. } => {
                    assert_eq!(*op, BinaryOp::Add);
                }
                _ => panic!(),
            }

            match &rows[0][1] {
                Expression::Binary { op, .. } => {
                    assert_eq!(*op, BinaryOp::Mul);
                }
                _ => panic!(),
            }

            match &rows[1][0] {
                Expression::Binary { op, .. } => {
                    assert_eq!(*op, BinaryOp::Sub);
                }
                _ => panic!(),
            }

            match &rows[1][1] {
                Expression::Binary { op, .. } => {
                    assert_eq!(*op, BinaryOp::Div);
                }
                _ => panic!(),
            }
        }
        _ => panic!("Expected Insert"),
    }
}

#[test]
fn test_insert_nested_expression() {
    let sql = "INSERT INTO users VALUES ((1 + 2) * 3);";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Insert { rows, .. } => {

            match &rows[0][0] {
                Expression::Binary { lhs, op, rhs } => {
                    assert_eq!(*op, BinaryOp::Mul);

                    match &**lhs {
                        Expression::Binary { op, .. } => {
                            assert_eq!(*op, BinaryOp::Add);
                        }
                        _ => panic!("Expected Add inside Mul"),
                    }

                    assert_eq!(
                        **rhs,
                        Expression::Literal(Literal::Int(3))
                    );
                }
                _ => panic!("Expected Mul"),
            }

        }
        _ => panic!("Expected Insert"),
    }
}

#[test]
fn test_insert_with_add_expression() {
    let sql = "INSERT INTO users VALUES (1 + 2, 'Alice');";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Insert { table, rows } => {
            assert_eq!(table, "USERS");
            assert_eq!(rows.len(), 1);

            match &rows[0][0] {
                Expression::Binary { lhs, op, rhs } => {
                    assert_eq!(*op, BinaryOp::Add);

                    assert_eq!(
                        **lhs,
                        Expression::Literal(Literal::Int(1))
                    );

                    assert_eq!(
                        **rhs,
                        Expression::Literal(Literal::Int(2))
                    );
                }
                _ => panic!("Expected Add expression"),
            }

            match &rows[0][1] {
                Expression::Literal(Literal::String(s)) => {
                    assert_eq!(s, "Alice");
                }
                _ => panic!("Expected string"),
            }
        }
        _ => panic!("Expected Insert"),
    }
}

#[test]
fn test_update() {
    let sql = "UPDATE USERS SET Name = 'Bob';";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Update { table, assignments, selection } => {
            assert_eq!(table, "USERS");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].column, "NAME");
            assert!(matches!(assignments[0].value, Expression::Literal(Literal::String(ref s)) if s == "Bob"));
            assert!(selection.is_none(), "Expected no WHERE clause");
        }
        _ => panic!("Expected Update statement"),
    }
}

#[test]
fn test_update_with_where() {
    let sql = "UPDATE users SET name = 'Bob' WHERE id = 1;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Update { table, assignments, selection } => {
            assert_eq!(table, "USERS");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].column, "NAME");
            assert!(matches!(assignments[0].value, Expression::Literal(Literal::String(ref s)) if s == "Bob"));
            match selection {
                Some(Expression::Binary { lhs, op, rhs}) => {
                    assert_eq!(*op, BinaryOp::Eq);
                    assert!(matches!(**lhs, Expression::Identifier(ref name) if name == "ID"));
                    assert!(matches!(**rhs, Expression::Literal(Literal::Int(1))));
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

            assert_eq!(table, "USERS");
            assert_eq!(columns.len(), 2);

            assert!(matches!(columns[0],Expression::Identifier(ref name) if name == "NAME"));
            assert!(matches!(columns[1],Expression::Identifier(ref name) if name == "AGE"));

            match selection {
                Some(Expression::Binary { lhs, op, rhs }) => {
                    assert_eq!(*op, BinaryOp::Eq);
                    assert!(matches!(**lhs,Expression::Identifier(ref name) if name == "ID"));
                    assert!(matches!(**rhs,Expression::Literal(Literal::Int(1))));
                }
                _ => panic!("Expected WHERE id = 1"),
            }
        }
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_select_arithmetic_projection() {
    let sql = "SELECT age + 1 FROM users;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Select { columns, table, selection } => {

            assert_eq!(table, "USERS");
            assert!(selection.is_none());

            match &columns[0] {
                Expression::Binary { lhs, op, rhs } => {
                    assert_eq!(*op, BinaryOp::Add);
                    assert!(matches!(**lhs,Expression::Identifier(ref name) if name == "AGE"));
                    assert!(matches!(**rhs,Expression::Literal(Literal::Int(1))));
                }
                _ => panic!("Expected AGE + 1"),
            }
        }
        _ => panic!("Expected select"),
    }
}

#[test]
fn test_select_multiple_expressions() {
    let sql = "SELECT age + 1, age * 2, name FROM users;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Select { columns, .. } => {
            assert_eq!(columns.len(), 3);
            assert!(matches!(columns[2],Expression::Identifier(ref n) if n == "NAME"));
        }
        _ => panic!(),
    }
}

#[test]
fn test_select_complex_expression() {
    let sql = "
        SELECT age * 2 + 1, -age
        FROM users
        WHERE (age + 5) > 10 OR NOT active;
    ";

    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::Select { columns, selection, .. } => {
            assert_eq!(columns.len(), 2);
            assert!(matches!(columns[0], Expression::Binary { .. }));
            assert!(matches!(columns[1], Expression::Unary { .. }));

            match selection {
                Some(Expression::Binary { op, .. }) => {
                    assert_eq!(*op, BinaryOp::Or);
                }
                _ => panic!("Expected OR"),
            }
        }
        _ => panic!(),
    }
}

#[test]
fn test_update_multiple_assignments() {
    let sql = "UPDATE users SET name = 'Bob', age = 30, email = 'bob@example.com' WHERE id = 1;";
    let stmts = parse_sql(sql);

    match &stmts[0] {
        Statement::Update { table, assignments, selection } => {
            assert_eq!(table, "USERS");
            assert_eq!(assignments.len(), 3);
            
            assert_eq!(assignments[0].column, "NAME");
            assert!(matches!(assignments[0].value, Expression::Literal(Literal::String(ref s)) if s == "Bob"));

            assert_eq!(assignments[1].column, "AGE");
            assert!(matches!(assignments[1].value, Expression::Literal(Literal::Int(30))));

            assert_eq!(assignments[2].column, "EMAIL");
            assert!(matches!(assignments[2].value, Expression::Literal(Literal::String(ref s)) if s == "bob@example.com"));

            match selection {
                Some(Expression::Binary { lhs, op, rhs}) => {
                    assert_eq!(*op, BinaryOp::Eq);
                    assert!(matches!(**lhs, Expression::Identifier(ref name) if name == "ID"));
                    assert!(matches!(**rhs, Expression::Literal(Literal::Int(1))));
                }
                _ => panic!("Expected equality expression"),
            }
        }
        _ => panic!("Expected Update statement"),
    }
}

#[test]
fn test_drop_table() {
    let sql = "DROP TABLE users;";
    let stmts = parse_sql(sql);
    match &stmts[0] {
        Statement::DropTable { name } => assert_eq!(name, "USERS"),
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
    let mut parser = Parser::new(&mut scanner).unwrap();
    parser.parse().unwrap_err();
}