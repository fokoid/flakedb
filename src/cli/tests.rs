use test_case::test_case;
use super::*;

#[test_case("" ; "empty string")]
#[test_case("     " ; "spaces")]
#[test_case("\t\t" ; "tabs")]
#[test_case("\t  \t\t    " ; "mixed whitespace")]
fn parse_empty(raw: &str) {
    assert_eq!(Command::parse(raw).unwrap(), Command::None);
}

// do we really want to test this?
#[test]
fn parse_meta_none() {
    assert_eq!(Command::parse(".").unwrap(), Command::Meta(MetaCommand::None));
}

#[test_case(".exit" => Command::Meta(MetaCommand::Exit) ; "meta command no args")]
fn parse_meta_valid(raw: &str) -> Command {
    Command::parse(raw).unwrap()
}

#[test_case(".fake")]
#[test_case(".placeholder")]
fn parse_meta_invalid(raw: &str) {
    assert!(
        matches!(
            Command::parse(raw).unwrap_err(),
            super::Error::MetaError(_)
        )
    )
}

#[test_case("select * from dual")]
fn parse_sql(raw: &str) {
    assert!(
        matches!(
            Command::parse(raw).unwrap(),
            Command::Statement(_)
        )
    )
}