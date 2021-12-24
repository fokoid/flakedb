use crate::cli::Error::SqlError;
use crate::tokens::{Token, Tokens};
use crate::{sql, Database};
use const_format::formatcp;
use std::io::{self, Write};
use std::path::PathBuf;
use thiserror::Error;

pub const NAME: &str = env!("CARGO_PKG_NAME");
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const SPLASH: &str = "Enter '.help' for assistance";
const PROMPT: &str = formatcp!("{}>", NAME);

pub fn print_splash() -> Result<()> {
    println!("{} v{}", NAME, VERSION);
    println!("{}", SPLASH);
    io::stdout().flush()?;
    Ok(())
}

pub fn print_prompt() -> Result<()> {
    print!("{} ", PROMPT);
    io::stdout().flush()?;
    Ok(())
}

pub fn read_input() -> Result<Command> {
    let mut buffer = String::new();
    let num_bytes = io::stdin().read_line(&mut buffer)?;
    if num_bytes == 0 {
        // user entered EOF (^D)
        Ok(Command::Meta(MetaCommand::Exit))
    } else {
        Command::parse(Tokens::from(buffer.trim()))
    }
}

pub fn open_database(path: Option<&PathBuf>) -> Result<Database> {
    match Database::open(path) {
        Ok(table) => Ok(table),
        Err(error) => Err(SqlError(error)),
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    None,
    Meta(MetaCommand),
    Statement(sql::Statement),
}

impl Command {
    pub fn parse(mut tokens: Tokens) -> Result<Self> {
        match tokens.peek() {
            None | Some(Token::None) => Ok(Self::None),
            Some(Token::Meta(_)) => Ok(Self::Meta(MetaCommand::parse(tokens)?)),
            Some(Token::Other(_)) => match sql::Statement::parse(tokens) {
                Ok(statement) => Ok(Self::Statement(statement)),
                Err(error) => Err(Error::SqlError(error)),
            },
        }
    }

    pub fn execute(&self, db: &mut Database) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Meta(meta) => Ok(meta.execute()?),
            Self::Statement(sql) => match sql.execute(db) {
                Ok(_) => Ok(()),
                Err(error) => Err(Error::SqlError(error)),
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum MetaCommand {
    None,
    Exit,
}

impl MetaCommand {
    pub fn parse(mut tokens: Tokens) -> Result<Self> {
        match tokens.next() {
            None | Some(Token::None) => Ok(Self::None),
            Some(Token::Meta(".exit")) => Ok(Self::Exit),
            Some(Token::Meta(s)) => Err(Error::MetaSyntaxError(format!(
                "invalid meta command '{}'",
                s
            ))),
            Some(Token::Other(s)) => Err(Error::MetaSyntaxError(format!(
                "expected meta command, but found '{}'",
                s
            ))),
        }
    }

    pub fn execute(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Exit => Err(Error::Exit(0)),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IoError(#[from] io::Error),
    #[error("SQL error")]
    SqlError(sql::Error),
    #[error("meta command syntax error")]
    MetaSyntaxError(String),
    #[error("normal program exit")]
    Exit(i32),
}

type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
