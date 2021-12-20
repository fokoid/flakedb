use test_case::test_case;
use super::*;

// do we really want to test this?
#[test]
fn parse_none() {
    assert_eq!(Statement::parse("").unwrap(), Statement::None);
}

#[test_case("select" => Statement::Select ; "select")]
#[test_case("insert" => Statement::Insert ; "insert")]
fn parse_meta_valid(raw: &str) -> Statement {
    Statement::parse(raw).unwrap()
}

#[test_case("fake")]
#[test_case("placeholder")]
fn parse_meta_invalid(raw: &str) {
    assert!(
        matches!(
            Statement::parse(raw).unwrap_err(),
            super::Error::UnknownKeyword(_)
        )
    )
}