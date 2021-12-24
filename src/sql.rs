use std::io;
use thiserror::Error;

mod db;
mod pager;
mod row;
mod statement;
mod table;

pub use crate::tokens::{Token, Tokens};
pub use db::Database;
pub use statement::Statement;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("syntax error: {0}")]
    SyntaxError(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("parser error: {0}")]
    ParserError(String),
    #[error("table full (max rows {0})")]
    TableFullError(usize),
    #[error("IO error")]
    IoError(#[from] io::Error),
}
