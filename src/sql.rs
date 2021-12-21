use thiserror::Error;
use crate::tokens::{Token, Tokens};

#[derive(Debug, Eq, PartialEq)]
pub enum Statement {
    Insert,
    Select,
    None,
}

impl Statement {
    pub fn parse(mut tokens: Tokens) -> Result<Self> {
        match tokens.next() {
            None | Some(Token::None) => Ok(Self::None),
            Some(Token::Meta(meta)) => Err(Error::SyntaxError(format!(
                "encountered meta token '{}' when SQL token was expected", meta
            ))),
            Some(Token::Other(s)) => match s {
                "insert" => Ok(Self::Insert),
                "select" => Ok(Self::Select),
                keyword => Err(Error::SyntaxError(
                    format!("unknown keyword '{}'", keyword)
                )),
            },
        }
    }

    pub fn execute(&self) -> Result<()> {
        println!("Executing...");
        match self {
            Self::Insert => {
                println!("do insert");
                Ok(())
            },
            Self::Select => {
                println!("do select");
                Ok(())
            },
            Self::None => Ok(()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("syntax error")]
    SyntaxError(String),
}

#[cfg(test)]
mod tests;