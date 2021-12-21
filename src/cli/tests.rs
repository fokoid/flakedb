use test_case::test_case;
use super::*;

#[test_case("" ; "empty string")]
#[test_case("     " ; "spaces")]
#[test_case("\t\t" ; "tabs")]
#[test_case("\t  \t\t    " ; "mixed whitespace")]
fn parse_empty(raw: &str) {
    let tokens = Tokens::from(raw);
    assert_eq!(Command::parse(tokens).unwrap(), Command::None);
}

#[test_case(".exit" => Command::Meta(MetaCommand::Exit) ; "meta command no args")]
fn parse_meta_valid(raw: &str) -> Command {
    let tokens = Tokens::from(raw);
    Command::parse(tokens).unwrap()
}

#[test_case(".fake")]
#[test_case(".placeholder")]
fn parse_meta_invalid(raw: &str) {
    let tokens = Tokens::from(raw);
    assert!(
        matches!(
            Command::parse(tokens).unwrap_err(),
            super::Error::SyntaxError(_)
        )
    )
}

#[test_case("select * from dual")]
fn parse_sql(raw: &str) {
    let tokens = Tokens::from(raw);
    assert!(
        matches!(
            Command::parse(tokens).unwrap(),
            Command::Statement(_)
        )
    )
}