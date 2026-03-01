use paste::paste;
use crate::compiler::ast::{Literal, RowDef};
use crate::compiler::bounded_ast::BoundExpr;
use crate::interpreter::ExecResult;
use crate::interpreter::executor::{Executor, ExprContext};
use crate::types::{ColumnId, DbResult};
use crate::with_read_pages;

impl Executor {

    pub fn select(
        &self,
        table: &str,
        columns: &Vec<ColumnId>,
        selection: &Option<BoundExpr>
    ) -> DbResult<ExecResult> {

        let ctx = self.context.read().unwrap();
        let database = ctx.current_db.clone().unwrap();

        let schema = ctx.catalog.get_table_schema(&database, table).unwrap();
        let storage_engine = ctx.storage_engines.get(&database).unwrap();

        let mut result = Vec::new();
        let mut page_id = schema.first_page_id;
        let mut next_id;

        while page_id != 0 {
            with_read_pages!(storage_engine.buffer_pool, [(page_id, page)], {
                next_id = page.get_next_id();
                for (_, record_bytes) in page.iter_record() {
                    let row = RowDef::deserialize(record_bytes, &schema.columns)
                        .expect("Error deserializing record");

                    // skip the row only if the condition evaluates to false
                    // no condition means updating every row
                    if let Some(condition) = selection {
                        if let Literal::Bool(false) = self.execute_expression(
                            condition,
                            &ExprContext { row: &row }
                        )? {
                            continue;
                        }
                    }

                    let mut projected = Vec::new();
                    for col in columns {
                        projected.push(row.record[*col].clone().to_string());
                    }

                    result.push(projected);
                }
                page_id = next_id;
            });
        }

        Ok(ExecResult::QueryResult(result))
    }
}