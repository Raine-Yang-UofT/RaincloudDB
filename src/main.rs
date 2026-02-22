use std::path::PathBuf;
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::Interpreter;

fn main() {
    let input = "
     DROP DATABASE test;
     CREATE DATABASE test;
     CONNECT TO test;
     CREATE TABLE test_table ( a INT, b CHAR(3) );
     INSERT INTO test_table VALUES (10, \"abc\"), (10, \"cde\"), (30, \"xyz\");
     SELECT a, b FROM test_table WHERE a=10;
     UPDATE test_table SET a = 10 WHERE b = \"xyz\";
     SELECT a, b FROM test_table WHERE a=10;
      ";
    let mut scanner = Scanner::new(input);
    let mut parser = Parser::new(&mut scanner);

    let dbms_root = PathBuf::from("C:\\Home\\Project\\RaincloudDB\\db");
    let mut interpreter = Interpreter::new(dbms_root);

    for statement in parser.parse().unwrap() {
        println!("{:?}", interpreter.execute(statement));
    }
}
