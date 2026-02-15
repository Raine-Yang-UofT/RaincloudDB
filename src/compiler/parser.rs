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
    | drop_stmt
    | connect_database_stmt
    | disconnect_database_stmt
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
            TokenType::Connect => self.parse_connect(),
            TokenType::Disconnect => self.parse_disconnect(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Select => self.parse_select(),
            _ => Err(format!("Unexpected token {:?} at line {}", self.peek(), self.peek().line)),
        }
    }

    /**
    create_database_stmt := CREATE DATABASE identifier;
    create_table_stmt := CREATE TABLE identifier ( column_def_stmt (, column_def_stmt)* );
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
    drop_database_stmt := DROP DATABASE identifier;
    drop_table_stmt := DROP TABLE identifier;
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
    connect_database_stmt := CONNECT TO identifier ;
    */
    fn parse_connect(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Connect)?;
        self.consume(TokenType::To)?;
        let name = self.consume_identifier()?;
        self.consume(TokenType::Semicolon)?;
        Ok(Statement::ConnectDatabase { name })
    }

    /**
    disconnect_database_stmt := DISCONNECT;
    */
    fn parse_disconnect(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Disconnect)?;
        self.consume(TokenType::Semicolon)?;
        Ok(Statement::DisconnectDatabase { })
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
    insert_stmt := INSERT INTO identifier VALUES row ( , row )* ;
    */
    fn parse_insert(&mut self) -> ParseResult<Statement> {
        self.consume(TokenType::Insert)?;
        self.consume(TokenType::Into)?;

        let table = self.consume_identifier()?;
        self.consume(TokenType::Values)?;

        let mut rows = Vec::new();
        rows.push(self.parse_value_row()?);
        while self.match_token(TokenType::Comma) {
            rows.push(self.parse_value_row()?);
        }

        self.consume(TokenType::Semicolon)?;

        Ok(Statement::Insert {table, rows})
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
    expression :=
    literal |
    identifier |
    (literal | identifier ) = (literal | identifier)
     */
    fn parse_expression(&mut self) -> ParseResult<Expression> {
        let left = Expression::Identifier(self.consume_identifier()?);
        self.consume(TokenType::Equal)?;
        let right = Expression::Literal(self.parse_literal()?);
        Ok(Expression::Equals(Box::new(left), Box::new(right)))
    }

    /**
    row := ( literal ( , literal )* )
    */
    fn parse_value_row(&mut self) -> ParseResult<RowDef> {
        self.consume(TokenType::LParen)?;

        let mut row = vec![self.parse_literal()?];
        while self.match_token(TokenType::Comma) {
            row.push(self.parse_literal()?);
        }

        self.consume(TokenType::RParen)?;
        Ok(RowDef { record: row })
    }

    /// literal
    fn parse_literal(&mut self) -> ParseResult<Literal> {
        let token = self.advance();
        match token.token_type {
            TokenType::IntLiteral(v) => Ok(Literal::Int(v)),
            TokenType::StringLiteral(s) => Ok(Literal::String(s)),
            TokenType::BoolLiteral(b) => Ok(Literal::Bool(b)),
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
