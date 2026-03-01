use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::compiler::token::TokenType;
use std::panic;
use raincloud_db::types::DbError;

fn collect_tokens(sql: &str) -> Vec<TokenType> {
    let mut scanner = Scanner::new(sql);
    let mut tokens = vec![];

    loop {
        let tok = scanner.next_token().unwrap();
        tokens.push(tok.token_type.clone());
        if tok.token_type == TokenType::Eof {
            break;
        }
    }
    tokens
}

#[test]
fn test_create_database() {
    let tokens = collect_tokens("CREATE DATABASE test_db;");

    assert_eq!(
        tokens,
        vec![
            TokenType::Create,
            TokenType::Database,
            TokenType::Identifier("TEST_DB".to_string()),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_connect_database() {
    let tokens = collect_tokens("CONNECT TO test_db; DISCONNECT;");

    assert_eq!(
        tokens,
        vec![
            TokenType::Connect,
            TokenType::To,
            TokenType::Identifier("TEST_DB".to_string()),
            TokenType::Semicolon,
            TokenType::Disconnect,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_create_table() {
    let sql = r#"
            Create Table users (
                id INT,
                name CHAR(10)
            );
        "#;

    let tokens = collect_tokens(sql);

    assert_eq!(
        tokens,
        vec![
            TokenType::Create,
            TokenType::Table,
            TokenType::Identifier("USERS".into()),
            TokenType::LParen,
            TokenType::Identifier("ID".into()),
            TokenType::Int,
            TokenType::Comma,
            TokenType::Identifier("NAME".into()),
            TokenType::Char,
            TokenType::LParen,
            TokenType::IntLiteral(10),
            TokenType::RParen,
            TokenType::RParen,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_insert() {
    let tokens = collect_tokens(
        "insert into Users VALUES (1, 'Alice');"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Insert,
            TokenType::Into,
            TokenType::Identifier("USERS".into()),
            TokenType::Values,
            TokenType::LParen,
            TokenType::IntLiteral(1),
            TokenType::Comma,
            TokenType::StringLiteral("Alice".into()),
            TokenType::RParen,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_select_where() {
    let tokens = collect_tokens(
        "SELECT name FROM users WHERE id = 1;"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Identifier("NAME".into()),
            TokenType::From,
            TokenType::Identifier("USERS".into()),
            TokenType::Where,
            TokenType::Identifier("ID".into()),
            TokenType::Equal,
            TokenType::IntLiteral(1),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_comparison_operators() {
    let tokens = collect_tokens(
        "SELECT * FROM users WHERE age >= 18 AND age <= 65 OR age > 100 OR age < 0 OR age != 21;"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("USERS".into()),
            TokenType::Where,
            TokenType::Identifier("AGE".into()),
            TokenType::GEqual,        // >=
            TokenType::IntLiteral(18),
            TokenType::And,
            TokenType::Identifier("AGE".into()),
            TokenType::LEqual,         // <=
            TokenType::IntLiteral(65),
            TokenType::Or,
            TokenType::Identifier("AGE".into()),
            TokenType::Greater,        // >
            TokenType::IntLiteral(100),
            TokenType::Or,
            TokenType::Identifier("AGE".into()),
            TokenType::Less,           // <
            TokenType::IntLiteral(0),
            TokenType::Or,
            TokenType::Identifier("AGE".into()),
            TokenType::NotEqual,           // !=
            TokenType::IntLiteral(21),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_arithmetic_operators() {
    let tokens = collect_tokens(
        "SELECT price * quantity + tax - discount / 2 FROM orders;"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Identifier("PRICE".into()),
            TokenType::Star,              // *
            TokenType::Identifier("QUANTITY".into()),
            TokenType::Plus,               // +
            TokenType::Identifier("TAX".into()),
            TokenType::Minus,               // -
            TokenType::Identifier("DISCOUNT".into()),
            TokenType::Slash,               // /
            TokenType::IntLiteral(2),
            TokenType::From,
            TokenType::Identifier("ORDERS".into()),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_logical_operators() {
    let tokens = collect_tokens(
        "SELECT * FROM products WHERE NOT in_stock OR (category = 'Electronics' AND price < 1000);"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("PRODUCTS".into()),
            TokenType::Where,
            TokenType::Not,                  // NOT
            TokenType::Identifier("IN_STOCK".into()),
            TokenType::Or,                    // OR
            TokenType::LParen,
            TokenType::Identifier("CATEGORY".into()),
            TokenType::Equal,
            TokenType::StringLiteral("Electronics".into()),
            TokenType::And,                   // AND
            TokenType::Identifier("PRICE".into()),
            TokenType::Less,
            TokenType::IntLiteral(1000),
            TokenType::RParen,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_complex_boolean_expression() {
    let tokens = collect_tokens(
        "SELECT * FROM employees WHERE (salary >= 50000 AND department != 'IT') OR (salary < 30000 AND NOT manager);"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("EMPLOYEES".into()),
            TokenType::Where,
            TokenType::LParen,
            TokenType::Identifier("SALARY".into()),
            TokenType::GEqual,
            TokenType::IntLiteral(50000),
            TokenType::And,
            TokenType::Identifier("DEPARTMENT".into()),
            TokenType::NotEqual,
            TokenType::StringLiteral("IT".into()),
            TokenType::RParen,
            TokenType::Or,
            TokenType::LParen,
            TokenType::Identifier("SALARY".into()),
            TokenType::Less,
            TokenType::IntLiteral(30000),
            TokenType::And,
            TokenType::Not,
            TokenType::Identifier("MANAGER".into()),
            TokenType::RParen,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_mixed_arithmetic_and_comparison() {
    let tokens = collect_tokens(
        "SELECT * FROM items WHERE price * quantity >= total + tax - discount;"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("ITEMS".into()),
            TokenType::Where,
            TokenType::Identifier("PRICE".into()),
            TokenType::Star,                    // *
            TokenType::Identifier("QUANTITY".into()),
            TokenType::GEqual,                   // >=
            TokenType::Identifier("TOTAL".into()),
            TokenType::Plus,                      // +
            TokenType::Identifier("TAX".into()),
            TokenType::Minus,                      // -
            TokenType::Identifier("DISCOUNT".into()),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_chained_comparisons() {
    let tokens = collect_tokens(
        "SELECT * FROM products WHERE 10 < price AND price <= 100;"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("PRODUCTS".into()),
            TokenType::Where,
            TokenType::IntLiteral(10),
            TokenType::Less,                       // <
            TokenType::Identifier("PRICE".into()),
            TokenType::And,
            TokenType::Identifier("PRICE".into()),
            TokenType::LEqual,                      // <=
            TokenType::IntLiteral(100),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_negation_and_arithmetic() {
    let tokens = collect_tokens(
        "SELECT * FROM transactions WHERE NOT (amount * -1 > 0);"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Identifier("TRANSACTIONS".into()),
            TokenType::Where,
            TokenType::Not,
            TokenType::LParen,
            TokenType::Identifier("AMOUNT".into()),
            TokenType::Star,                         // *
            TokenType::Minus,                          // - (unary minus becomes number -1 in scanner)
            TokenType::IntLiteral(1),
            TokenType::Greater,                         // >
            TokenType::IntLiteral(0),
            TokenType::RParen,
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_keywords_case_insensitivity() {
    let tokens = collect_tokens(
        "SELECT * FROM users WHERE age >= 18 AND name = 'John' OR NOT active;"
    );

    // Test lowercase version
    let tokens_lower = collect_tokens(
        "select * from users where age >= 18 and name = 'John' or not active;"
    );

    assert_eq!(tokens, tokens_lower);
}

#[test]
fn test_comments_and_whitespace() {
    let tokens = collect_tokens(
        "-- comment\nsELecT id frOM users; -- trailing"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Select,
            TokenType::Identifier("ID".into()),
            TokenType::From,
            TokenType::Identifier("USERS".into()),
            TokenType::Semicolon,
            TokenType::Eof,
        ]
    );
}

#[test]
fn test_unterminated_string_error_reporting() {
    let sql = r#"
        INSERT INTO users VALUES ('Alice;
        "#; // missing closing quote

    let mut scanner = Scanner::new(sql);

    loop {
        let result = scanner.next_token();

        if matches!(result, Err(DbError::ScannerError(_))) {
            return;
        }

        if let Ok(token) = result {
            if token.token_type == TokenType::Eof {
                panic!("Expected ScannerError, got EOF");
            }
        }
    }
}

#[test]
fn test_unexpected_character_error_correct_line() {
    let sql = r#"
        SELECT @ FROM users;
        "#; // '@' unsupported character
    let mut scanner = Scanner::new(sql);

    loop {
        let result = scanner.next_token();

        if matches!(result, Err(DbError::ScannerError(_))) {
            return;
        }

        if let Ok(token) = result {
            if token.token_type == TokenType::Eof {
                panic!("Expected ScannerError, got EOF");
            }
        }
    }
}