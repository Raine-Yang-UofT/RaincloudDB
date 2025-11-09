use crate::compiler::ast::*;
use crate::compiler::scanner::Scanner;
use crate::compiler::token::{Token, TokenType};

type ParseResult<T> = Result<T, String>;

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {

    /// Creating parser from scanner result
    pub fn new(scanner: &mut Scanner) -> Self {
        let mut tokens = vec![];
        loop {
            let token = scanner.next_token();
            tokens.push(token.clone());
            if token.token_type == TokenType::Eof {
                break;
            }
        }

        Parser { tokens, current: 0 }
    }

    /// Main parser method
    pub fn parse(&mut self) -> ParseResult<Vec<Statement>> {
        let mut statements = vec![];
        while !self.is_at_end() {
            statements.push(self.parse_statement()?);
        }
        Ok(statements)
    }

    /**
    statement := create_database_stmt
    | drop_database_stmt
    | create_table_stmt
    | drop_table_stmt
    | insert_stmt
    | update_stmt
    | select_stmt;
     */
    fn parse_statement(&mut self) -> ParseResult<Statement> {
        match self.peek().token_type {
            TokenType::Create => self.parse_create(),
            TokenType::Drop   => self.parse_drop(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Select => self.parse_select(),
            _ => Err(format!("Unexpected token {:?} at line {}", self.peek(), self.peek().line)),
        }
    }

    /**
    create_stmt := CREATE DATABASE identifier;
    create_stmt := CREATE TABLE identifier ( column_def_stmt (, column_def_stmt)* );
     */
    fn parse_create(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Create)?;
        let token = self.peek();
        match token.token_type {
            TokenType::Database => {
                self.consume(TokenType::Database)?;
                let name = self.consume_identifier()?;
                self.consume(TokenType::Semicolon)?;
                Ok(Statement::CreateDatabase { name })
            },
            TokenType::Table => {
                self.consume(TokenType::Table)?;
                let name = self.consume_identifier()?;
                self.consume(TokenType::LParen)?;

                let mut columns = vec![self.parse_column_def()?];
                while self.match_token(TokenType::Comma) {
                    columns.push(self.parse_column_def()?);
                }

                self.consume(TokenType::RParen)?;
                self.consume(TokenType::Semicolon)?;

                Ok(Statement::CreateTable { name, columns })
            },
            _ => Err(format!("Expected DATABASE or TABLE at line {}", token.line).into()),
        }
    }

    /**
    drop_stmt := DROP DATABASE identifier;
    drop_stmt := DROP TABLE identifier;
    */
    fn parse_drop(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Drop)?;
        let token = self.peek();
        match token.token_type {
            TokenType::Database => {
                self.consume(TokenType::Database)?;
                let name = self.consume_identifier()?;
                self.consume(TokenType::Semicolon)?;
                Ok(Statement::DropDatabase { name })
            },
            TokenType::Table => {
                self.consume(TokenType::Table)?;
                let name = self.consume_identifier()?;
                self.consume(TokenType::Semicolon)?;
                Ok(Statement::DropTable { name })
            },
            _ => Err(format!("Expected DATABASE or TABLE at line {}", token.line).into()),
        }
    }

    /**
    column_def_stmt := identifier: data_type_stmt
    */
    fn parse_column_def(&mut self) -> ParseResult<ColumnDef> {
        let name = self.consume_identifier()?;
        let data_type = self.parse_data_type()?;
        Ok(ColumnDef { name, data_type })
    }

    /**
    data_type_stmt := INT | CHAR ( int_literal )
    */
    fn parse_data_type(&mut self) -> ParseResult<DataType> {
        let token = self.advance();
        match token.token_type {
            TokenType::Int => Ok(DataType::Int),
            TokenType::Char => {
                self.consume(TokenType::LParen)?;
                let len = self.consume_int_literal()? as u32;
                self.consume(TokenType::RParen)?;
                Ok(DataType::Char(len))
            }
            _ => Err(format!("Expected valid data type on line {:?}", token.line))
        }
    }

    /**
    insert_stmt := INSERT INTO identifier VALUES ( literal (,literal)* ) ;
    */
    fn parse_insert(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Insert)?;
        self.consume(TokenType::Into)?;

        let table = self.consume_identifier()?;
        self.consume(TokenType::Values)?;
        self.consume(TokenType::LParen)?;

        let mut values = vec![self.parse_literal()?];
        while self.match_token(TokenType::Comma) {
            values.push(self.parse_literal()?);
        }

        self.consume(TokenType::RParen)?;
        self.consume(TokenType::Semicolon)?;

        Ok(Statement::Insert {table, values})
    }

    /**
    update_stmt := UPDATE identifier SET ( assignment (,assignment)* ) (WHERE expression)?;
    */
    fn parse_update(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Update)?;
        let table = self.consume_identifier()?;

        self.consume(TokenType::Set)?;
        let mut assignments = vec![self.parse_assignment()?];
        while self.match_token(TokenType::Comma) {
            assignments.push(self.parse_assignment()?);
        }

        let selection = if self.match_token(TokenType::Where) {
            Some(self.parse_expression()?)
        } else { None };

        self.consume(TokenType::Semicolon)?;

        Ok(Statement::Update {table, assignments, selection})
    }

    /**
    select_stmt := SELECT identifier (,identifier)* FROM identifier (WHERE expression)?;
    */
    fn parse_select(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Select)?;

        let mut columns = vec![self.consume_identifier()?];
        while self.match_token(TokenType::Comma) {
            columns.push(self.consume_identifier()?);
        }

        self.consume(TokenType::From)?;
        let table = self.consume_identifier()?;

        let selection = if self.match_token(TokenType::Where) {
            Some(self.parse_expression()?)
        } else { None };

        self.consume(TokenType::Semicolon)?;

        Ok(Statement::Select {table, columns, selection})
    }

    /**
    assignment := identifier = literal
    */
    fn parse_assignment(&mut self) -> ParseResult<Assignment> {
        let column = self.consume_identifier()?;
        self.consume(TokenType::Equal)?;
        let value = self.parse_literal()?;
        Ok(Assignment { column, value })
    }

    /**
    expression := identifier = literal

    currently only support equality expression
     */
    fn parse_expression(&mut self) -> ParseResult<Expression> {
        let left = Expression::Identifier(self.consume_identifier()?);
        self.consume(TokenType::Equal)?;
        let right = Expression::Literal(self.parse_literal()?);
        Ok(Expression::Equals(Box::new(left), Box::new(right)))
    }

    /// literal
    fn parse_literal(&mut self) -> ParseResult<Literal> {
        let token = self.advance();
        match token.token_type {
            TokenType::IntLiteral(v) => Ok(Literal::Int(v)),
            TokenType::StringLiteral(s) => Ok(Literal::String(s)),
            t => Err(format!("Expected literal, got {:?} at line {:?}", t, token.line)),
        }
    }

    // helper functions
    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn advance(&mut self) -> Token {
        if !self.is_at_end() { self.current += 1; }
        self.tokens[self.current - 1].clone()
    }

    fn is_at_end(&self) -> bool {
        self.peek().token_type == TokenType::Eof
    }

    fn match_token(&mut self, token: TokenType) -> bool {
        if self.peek().token_type == token {
            self.advance();
            true
        } else {
            false
        }
    }

    fn consume(&mut self, token: TokenType) -> ParseResult<()> {
        if self.peek().token_type == token {
            self.advance();
            Ok(())
        } else {
            Err(format!("Expected {:?}, got {:?}", token, self.peek()))
        }
    }

    /// identifier
    fn consume_identifier(&mut self) -> ParseResult<String> {
        if let TokenType::Identifier(name) = &self.peek().token_type {
            let name = name.clone();
            self.advance();
            Ok(name)
        } else {
            Err(format!("Expected identifier, got {:?}", self.peek()))
        }
    }

    /// int literal
    fn consume_int_literal(&mut self) -> ParseResult<i32> {
        if let TokenType::IntLiteral(v) = self.peek().token_type {
            self.advance();
            Ok(v)
        } else {
            Err(format!("Expected integer literal, got {:?}", self.peek()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::scanner::Scanner;

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
}
