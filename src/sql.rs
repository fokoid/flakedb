use std::io;
use thiserror::Error;

mod db;
pub mod pager;
pub mod row;
mod statement;
mod cursor;

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
    #[error("corrupt page file: {0}")]
    PageFileCorrupt(&'static str),
    #[error("corrupt page")]
    PageCorrupt(String),
    #[error("IO error")]
    IoError(#[from] io::Error),
}
