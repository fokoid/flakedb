use std::io;
use std::num::ParseIntError;
use thiserror::Error;

mod row;
mod table;
pub use table::Table;
mod statement;
pub use statement::Statement;
pub use crate::tokens::{Token, Tokens};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("syntax error: {0}")]
    SyntaxError(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("parser error {0}")]
    ParserError(#[from] ParseIntError),
    #[error("table full (max rows {0})")]
    TableFullError(usize),
    #[error("IO error")]
    IoError(#[from] io::Error),
}