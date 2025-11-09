use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::compiler::token::TokenType;
use std::panic;

fn collect_tokens(sql: &str) -> Vec<TokenType> {
    let mut scanner = Scanner::new(sql);
    let mut tokens = vec![];

    loop {
        let tok = scanner.next_token();
        tokens.push(tok.token_type.clone());
        if tok.token_type == TokenType::Eof {
            break;
        }
    }
    tokens
}

fn run_and_catch<F: FnOnce() -> () + panic::UnwindSafe>(f: F) -> String {
    match panic::catch_unwind(f) {
        Ok(_) => panic!("Expected panic, but scanner did not panic"),
        Err(e) => {
            if let Some(msg) = e.downcast_ref::<String>() {
                msg.clone()
            } else if let Some(msg) = e.downcast_ref::<&str>() {
                msg.to_string()
            } else {
                panic!("Panic with non-string message");
            }
        }
    }
}

#[test]
fn test_create_database() {
    let tokens = collect_tokens("CREATE DATABASE test_db; USE test_db;");

    assert_eq!(
        tokens,
        vec![
            TokenType::Create,
            TokenType::Database,
            TokenType::Identifier("test_db".to_string()),
            TokenType::Semicolon,
            TokenType::Use,
            TokenType::Identifier("test_db".to_string()),
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
            TokenType::Identifier("users".into()),
            TokenType::LParen,
            TokenType::Identifier("id".into()),
            TokenType::Int,
            TokenType::Comma,
            TokenType::Identifier("name".into()),
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
        "insert into users VALUES (1, 'Alice');"
    );

    assert_eq!(
        tokens,
        vec![
            TokenType::Insert,
            TokenType::Into,
            TokenType::Identifier("users".into()),
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
            TokenType::Identifier("name".into()),
            TokenType::From,
            TokenType::Identifier("users".into()),
            TokenType::Where,
            TokenType::Identifier("id".into()),
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
            TokenType::Identifier("id".into()),
            TokenType::From,
            TokenType::Identifier("users".into()),
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

    let msg = run_and_catch(|| {
        let mut scanner = Scanner::new(sql);
        loop { scanner.next_token(); }
    });

    assert!(msg.contains("unterminated string literal"));
    assert!(msg.contains("line 3")); // correct line number for error
}

#[test]
fn test_unexpected_character_error_correct_line() {
    let sql = r#"
        SELECT @ FROM users;
        "#; // '@' unsupported character

    let msg = run_and_catch(|| {
        let mut scanner = Scanner::new(sql);
        loop { scanner.next_token(); }
    });

    assert!(msg.contains("Unexpected character"));
    assert!(msg.contains("line 2"));
}