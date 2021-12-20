use thiserror::Error;

#[derive(Debug, Eq, PartialEq)]
pub enum Statement {
    Insert,
    Select,
    None,
}

impl Statement {
    pub fn parse(raw: &str) -> Result<Self> {
        let mut tokens = raw.split_whitespace();
        match tokens.next() {
            None => Ok(Self::None),
            Some("insert") => Ok(Self::Insert),
            Some("select") => Ok(Self::Select),
            _ => Err(Error::UnknownKeyword(String::from(raw))),
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
    #[error("Unknown keyword")]
    UnknownKeyword(String),
}

#[cfg(test)]
mod tests;