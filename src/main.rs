use std::io::{self, Write};
use std::path::PathBuf;

use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;
use raincloud_db::interpreter::{Interpreter, ExecResult};
use raincloud_db::types::DbError;

fn execute_input(input: &str, interpreter: &mut Interpreter) {
    let mut scanner = Scanner::new(input);
    let mut parser = Parser::new(&mut scanner);

    match parser.parse() {
        Ok(statements) => {
            for stmt in statements {
                match interpreter.execute(stmt) {
                    Ok(result) => print_exec_result(result),
                    Err(err) => print_db_error(err),
                }
            }
        }
        Err(err) => print_db_error(err),
    }
}

fn print_exec_result(result: ExecResult) {
    match result {
        ExecResult::Success(msg) => {
            println!("{msg}");
        }

        ExecResult::AffectedRows(count, msg) => {
            println!("{msg} ({count} rows affected)");
        }

        ExecResult::QueryResult(rows) => {
            print_table(rows);
        }
    }
}

fn print_db_error(error: DbError) {
    println!("ERROR: {:?}", error);
}

fn print_table(rows: Vec<Vec<String>>) {
    if rows.is_empty() {
        println!("(no rows)");
        return;
    }

    // Compute column widths
    let col_count = rows[0].len();
    let mut widths = vec![0; col_count];

    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    // Print rows
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            print!("{:width$} ", cell, width = widths[i]);
        }
        println!();
    }
}

fn main() {
    let dbms_root = PathBuf::from("C:\\Home\\Project\\RaincloudDB\\db");
    let mut interpreter = Interpreter::new(dbms_root);

    println!("RaincloudDB Shell");
    println!("Type SQL statements ending with ';'");
    println!("Type 'exit;' to quit\n");

    let mut buffer = String::new();
    loop {
        print!("db > ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        buffer.push_str(&input);

        // Only execute once we see a semicolon
        if !buffer.trim_end().ends_with(';') {
            continue;
        }

        if buffer.trim().eq_ignore_ascii_case("exit;") {
            break;
        }

        execute_input(&buffer, &mut interpreter);
        buffer.clear();
    }
}
