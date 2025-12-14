use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::catalog::Catalog;
use raincloud_db::interpreter::execution_context::ExecutionContext;
use raincloud_db::interpreter::executor::Executor;

fn main() {
    let input = "
     DROP DATABASE test;
     CREATE DATABASE test;
     CONNECT TO test;
     CREATE TABLE test_table ( a INT, b CHAR(200) ); ";
    let mut scanner = Scanner::new(input);
    let mut parser = Parser::new(&mut scanner);

    let dbms_root = PathBuf::from("C:\\Home\\Project\\RaincloudDB\\db");
    let catalog = Catalog::new(&dbms_root);
    let execution_context = ExecutionContext::new(dbms_root, catalog);
    let mut executor = Executor::new(Arc::new(RwLock::new(execution_context)));

    for statement in parser.parse().unwrap() {
        println!("{:?}", executor.execute(statement));
    }
}
