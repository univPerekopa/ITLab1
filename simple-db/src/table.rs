use crate::types::{DbError, DbType, DbValue, Row};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    name: String,
    rows: Vec<Row>,
    schema: Vec<DbType>,
}

impl Table {
    pub fn new(name: String, schema: Vec<DbType>) -> Self {
        Self {
            name,
            rows: Vec::new(),
            schema,
        }
    }

    pub fn insert_row(&mut self, row: Row) -> Result<(), DbError> {
        let row_schema = row.schema();
        if row_schema == self.schema {
            self.rows.push(row);
            Ok(())
        } else {
            Err(DbError::IncorrectRow)
        }
    }

    pub fn update_row(&mut self, idx: usize, row: Row) -> Result<(), DbError> {
        let row_schema = row.schema();
        if row_schema == self.schema {
            self.rows[idx] = row;
            Ok(())
        } else {
            Err(DbError::IncorrectRow)
        }
    }

    pub fn remove_row(&mut self, idx: usize) {
        if self.rows.len() > idx {
            self.rows.remove(idx);
        }
    }

    pub fn get_rows_sorted(&self, mut sort_by: Option<usize>) -> impl Iterator<Item = &Row> + '_ {
        if sort_by.unwrap_or(0) >= self.schema().len() {
            sort_by = None;
        }

        self.rows
            .iter()
            .enumerate()
            .sorted_by(|a, b| {
                let a = if let Some(sort_by) = sort_by {
                    a.1.get(sort_by).clone()
                } else {
                    DbValue::Int(a.0 as i64)
                };

                let b = if let Some(sort_by) = sort_by {
                    b.1.get(sort_by).clone()
                } else {
                    DbValue::Int(b.0 as i64)
                };

                a.partial_cmp(&b).unwrap()
            })
            .map(|(_, row)| row)
    }

    pub fn validate_rows(&self) -> Result<(), DbError> {
        for row in &self.rows {
            if row.schema() != self.schema {
                return Err(DbError::InvalidTableState(self.name.clone()));
            }
        }
        Ok(())
    }

    pub fn schema(&self) -> &[DbType] {
        &self.schema
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }
}
