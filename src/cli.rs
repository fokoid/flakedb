use std::io::{self, Write};
use thiserror::Error;
use crate::sql;
use crate::tokens::{Token, Tokens};

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
            Some(Token::Other(_)) => Ok(Self::Statement(sql::Statement::parse(tokens)?)),
        }
    }

    pub fn execute(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Meta(meta) => Ok(meta.execute()?),
            Self::Statement(sql) => Ok(sql.execute()?),
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
            None => Ok(Self::None),
            Some(Token::Meta(".exit")) => Ok(Self::Exit),
            _ => Err(
                Error::SyntaxError(
                    format!("invalid meta command '{}'", String::from(tokens))
                )
            ),
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
    SqlError(#[from] sql::Error),
    #[error("meta command error")]
    MetaError(String),
    #[error("syntax error")]
    SyntaxError(String),
    #[error("normal program exit")]
    Exit(i32),
}

type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;