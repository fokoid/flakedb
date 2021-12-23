use crate::sql::{self, Table};
use crate::tokens::{Token, Tokens};
use std::io::{self, Write};
use thiserror::Error;

pub fn print_prompt() -> Result<()> {
    let prompt = "> ";
    print!("{}", prompt);
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

    pub fn execute(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Meta(meta) => Ok(meta.execute()?),
            Self::Statement(sql) => match sql.execute(table) {
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
                "invalid meta command '{}'", s
            ))),
            Some(Token::Other(s)) => Err(Error::MetaSyntaxError(format!(
                "expected meta command, but found '{}'", s
            )))
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
