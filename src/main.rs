use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::catalog::Catalog;
use raincloud_db::interpreter::execution_context::ExecutionContext;
use raincloud_db::interpreter::executor::Executor;

fn main() {
    let input = "DROP DATABASE test;";
    let mut scanner = Scanner::new(input);
    let mut parser = Parser::new(&mut scanner);

    let config_dir = "C:\\Home\\Project\\RaincloudDB\\db";
    let catalog = Catalog::new(config_dir);
    let mut execution_context = ExecutionContext::new(config_dir, catalog);
    let mut executor = Executor::new(&mut execution_context);

    for statement in parser.parse().unwrap() {
        println!("{:?}", executor.execute(statement));
    }
}
