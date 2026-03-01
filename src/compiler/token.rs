#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenType {
    // symbols
    LParen,     // (
    RParen,     // )
    Comma,      // ,
    Semicolon,  // ;
    Equal,      // =
    Greater,    // >
    GEqual,     // >=
    Less,       // <
    LEqual,     // <=
    Star,       // *
    Slash,      // /
    Plus,       // +
    Minus,      // -

    // data types
    Int,
    Char,

    // literals
    Identifier(String),
    IntLiteral(i32),
    StringLiteral(String),
    BoolLiteral(bool),

    // keywords
    Create, Database, Drop, Table, Insert, Into,
    Values, Update, Set, Where, Select, From,
    Connect, To, Disconnect, And, Or, Not,

    // special
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub token_type: TokenType,
    pub lexeme: String,
    pub line: usize, // line number for error handling
}