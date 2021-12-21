use test_case::test_case;
use super::*;

// do we really want to test this?
#[test]
fn parse_none() {
    let tokens = Tokens::from("");
    assert_eq!(Statement::parse(tokens).unwrap(), Statement::None);
}

#[test_case("select" => Statement::Select ; "select")]
#[test_case("insert" => Statement::Insert ; "insert")]
fn parse_meta_valid(raw: &str) -> Statement {
    let tokens = Tokens::from(raw);
    Statement::parse(tokens).unwrap()
}

#[test_case("fake")]
#[test_case("placeholder")]
fn parse_meta_invalid(raw: &str) {
    let tokens = Tokens::from(raw);
    assert!(
        matches!(
            Statement::parse(tokens).unwrap_err(),
            super::Error::SyntaxError(_)
        )
    )
}