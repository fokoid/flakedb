use super::{Error, Result, Table, Token, Tokens};
use super::row::InputRow;

#[derive(Debug, Eq, PartialEq)]
pub enum Statement {
    Insert(InputRow),
    Select,
    None,
}

impl Statement {
    pub fn parse(mut tokens: Tokens) -> Result<Self> {
        match tokens.next() {
            None | Some(Token::None) => Ok(Self::None),
            Some(Token::Meta(meta)) => Err(Error::SyntaxError(format!(
                "encountered meta token '{}' when SQL token was expected",
                meta
            ))),
            Some(Token::Other(s)) => match s {
                "insert" => Ok(Self::Insert(InputRow::parse(&mut tokens)?)),
                "select" => Ok(Self::Select),
                keyword => Err(Error::SyntaxError(format!("unknown keyword '{}'", keyword))),
            },
        }
    }

    pub fn execute(&self, table: &mut Table) -> Result<()> {
        println!("Executing...");
        match self {
            Self::Insert(row) => {
                table.insert(&row.validate()?)?;
                Ok(())
            }
            Self::Select => {
                for row in table.select()? {
                    let row = InputRow::from(&row);
                    println!("{}", row);
                }
                Ok(())
            }
            Self::None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // do we really want to test this?
    #[test]
    fn parse_none() {
        let tokens = Tokens::from("");
        assert_eq!(Statement::parse(tokens).unwrap(), Statement::None);
    }

    // #[test_case("select" => Statement::Select ; "select")]
    // #[test_case("insert" => Statement::Insert ; "insert")]
    // fn parse_meta_valid(raw: &str) -> Statement {
    //     let tokens = Tokens::from(raw);
    //     Statement::parse(tokens).unwrap()
    // }

    #[test_case("fake")]
    #[test_case("placeholder")]
    fn parse_meta_invalid(raw: &str) {
        let tokens = Tokens::from(raw);
        assert!(matches!(
            Statement::parse(tokens).unwrap_err(),
            super::Error::SyntaxError(_)
        ))
    }
}