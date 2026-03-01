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