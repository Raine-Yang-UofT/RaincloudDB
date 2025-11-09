use crate::compiler::token::{Token, TokenType};

pub struct Scanner {
    source: Vec<char>,
    start: usize,
    current: usize,
    line: usize,
}

impl Scanner {

    pub fn new(source: &str) -> Self {
        Self {
            source: source.chars().collect(),
            start: 0,
            current: 0,
            line: 1,
        }
    }

    /// Checking reaching end of file
    fn is_at_end(&self) -> bool {
        self.current >= self.source.len()
    }

    /// Return and consume current character
    fn advance(&mut self) -> char {
        let c = self.source[self.current];
        self.current += 1;
        c
    }

    /// Return current character, not consume
    fn peek(&self) -> char {
        if self.is_at_end() { '\0' } else { self.source[self.current] }
    }

    /// Return next character, not consume
    fn peek_next(&self) -> char {
        if self.current + 1 >= self.source.len() { '\0' } else { self.source[self.current + 1] }
    }

    /// Create token of given type
    fn add_token(&self, token_type: TokenType) -> Token {
        let lexeme: String = self.source[self.start..self.current].iter().collect();

        Token {
            token_type,
            lexeme,
            line: self.line,
        }
    }

    /// Skip whitespace and comment
    fn skip_whitespace(&mut self) {
        while !self.is_at_end() {
            match self.peek() {
                ' ' | '\r' | '\t' => { self.advance(); },
                '\n' => {
                    // enter new line
                    self.line += 1;
                    self.advance();
                }
                '-' if self.peek_next() == '-' => {
                    // SQL comment: -- until end of line
                    while self.peek() != '\n' && !self.is_at_end() {
                        self.advance();
                    }
                }
                _ => break,
            }
        }
    }

    /// Scan keyword and identifier
    fn identifier(&mut self) -> Token {
        while self.peek().is_ascii_alphanumeric() || self.peek() == '_' {
            self.advance();
        }

        let text: String = self.source[self.start..self.current].iter().collect();
        let upper = text.to_uppercase();

        let token_type = match upper.as_str() {
            "CREATE" => TokenType::Create,
            "DATABASE" => TokenType::Database,
            "DROP" => TokenType::Drop,
            "TABLE" => TokenType::Table,
            "INSERT" => TokenType::Insert,
            "INTO" => TokenType::Into,
            "VALUES" => TokenType::Values,
            "UPDATE" => TokenType::Update,
            "SET" => TokenType::Set,
            "WHERE" => TokenType::Where,
            "SELECT" => TokenType::Select,
            "FROM" => TokenType::From,
            "INT" => TokenType::Int,
            "CHAR" => TokenType::Char,
            "USE" => TokenType::Use,
            _ => TokenType::Identifier(text),
        };

        self.add_token(token_type)
    }

    /// Scan integer
    fn number(&mut self) -> Token {
        while self.peek().is_ascii_digit() {
            self.advance();
        }

        let value: String = self.source[self.start..self.current].iter().collect();
        let int_value = value.parse::<i32>().unwrap();
        self.add_token(TokenType::IntLiteral(int_value))
    }

    /// Scan string
    fn string(&mut self, quote: char) -> Token {
        while self.peek() != quote && !self.is_at_end() {
            if self.peek() == '\n' {
                self.line += 1;
            }
            self.advance();
        }

        if self.is_at_end() {
            panic!("unterminated string literal at line {}", self.line);
        }

        // closing quote
        self.advance();

        let value: String = self.source[self.start + 1..self.current - 1].iter().collect();
        self.add_token(TokenType::StringLiteral(value))
    }

    /// Scan tokens from text
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();
        self.start = self.current;

        if self.is_at_end() {
            return self.add_token(TokenType::Eof);
        }

        let c = self.advance();
        match c {
            '(' => self.add_token(TokenType::LParen),
            ')' => self.add_token(TokenType::RParen),
            ',' => self.add_token(TokenType::Comma),
            ';' => self.add_token(TokenType::Semicolon),
            '=' => self.add_token(TokenType::Equal),
            '"' | '\'' => self.string(c),
            '0'..='9' => self.number(),
            'A'..='Z' | 'a'..='z' | '_' =>self.identifier(),
            _ => {
                panic!("Unexpected character '{}' at line {}", c, self.line);
            }
        }
    }
}


