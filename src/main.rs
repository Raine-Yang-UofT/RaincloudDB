use std::path::PathBuf;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::Interpreter;

fn main() {
    let input = "
     DROP DATABASE test;
     --CREATE DATABASE test;
     --CONNECT TO test;
     --CREATE TABLE test_table ( a INT, b CHAR(200) );
     --DROP TABLE test_table;
      ";
    let mut scanner = Scanner::new(input);
    let mut parser = Parser::new(&mut scanner);

    let dbms_root = PathBuf::from("C:\\Home\\Project\\RaincloudDB\\db");
    let mut interpreter = Interpreter::new(dbms_root);

    for statement in parser.parse().unwrap() {
        println!("{:?}", interpreter.execute(statement));
    }
}
