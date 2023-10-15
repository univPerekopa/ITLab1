use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum DbType {
    Int,
    Real,
    Char,
    String,
    ComplexReal,
    ComplexInt,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum DbValue {
    Int(i64),
    Real(f64),
    Char(char),
    String(String),
    ComplexReal((f64, f64)),
    ComplexInt((i64, i64)),
}

pub type ComplexReal = (f64, f64);
pub type ComplexInt = (i64, i64);

impl DbValue {
    pub fn get_type(&self) -> DbType {
        match self {
            Self::Int(_) => DbType::Int,
            Self::Real(_) => DbType::Real,
            Self::Char(_) => DbType::Char,
            Self::String(_) => DbType::String,
            Self::ComplexReal(_) => DbType::ComplexReal,
            Self::ComplexInt(_) => DbType::ComplexInt,
        }
    }
}

impl Display for DbValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DbValue::Int(x) => f.write_str(&x.to_string())?,
            DbValue::Real(x) => f.write_str(&x.to_string())?,
            DbValue::String(x) => f.write_str(&x.to_string())?,
            DbValue::Char(x) => f.write_str(&x.to_string())?,
            DbValue::ComplexReal(x) => {
                if x.1 >= 0. {
                    f.write_str(&format!("{} + {}i", x.0, x.1))?
                } else {
                    f.write_str(&format!("{} - {}i", x.0, -x.1))?
                }
            }
            DbValue::ComplexInt(x) => {
                if x.1 >= 0 {
                    f.write_str(&format!("{} + {}i", x.0, x.1))?
                } else {
                    f.write_str(&format!("{} - {}i", x.0, -x.1))?
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row(pub Vec<DbValue>);

impl Row {
    pub fn get(&self, idx: usize) -> DbValue {
        self.0[idx].clone()
    }

    pub fn schema(&self) -> Vec<DbType> {
        self.0.iter().map(|v| v.get_type()).collect()
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for value in &self.0 {
            f.write_str(&value.to_string())?;
            f.write_str(" ")?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("De(serialization error): {0}")]
    Serde(#[from] bincode::Error),
    #[error("Row does not fit table's schema")]
    IncorrectRow,
    #[error("Table {0} is already present")]
    TableIsAlreadyPresent(String),
    #[error("Table {0} is missing")]
    TableIsMissing(String),
    #[error("Invalid state for table {0}")]
    InvalidTableState(String),
}
