use std::io;
use std::io::Write;
use thiserror::Error;
use crate::sql;

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
        Ok(Command::Meta(MetaCommand::Exit))
    } else {
        Command::parse(buffer.trim())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    None,
    Meta(MetaCommand),
    Statement(sql::Statement),
}

impl Command {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        match raw.chars().nth(0) {
            None => Ok(Self::None),
            Some('.') => Ok(Self::Meta(MetaCommand::parse(&raw[1..])?)),
            _ => Ok(Self::Statement(sql::Statement::parse(raw)?)),
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
    pub fn parse(raw: &str) -> Result<Self> {
        let mut tokens = raw.split_whitespace();
        match tokens.next() {
            None => Ok(Self::None),
            Some("exit") => Ok(Self::Exit),
            _ => Err(
                Error::MetaError(
                    format!("invalid meta command '{}'", raw)
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
    #[error("normal program exit")]
    Exit(i32),
}

type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;