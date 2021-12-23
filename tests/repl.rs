use rexpect::session::PtySession;
use rexpect::errors::Result;
use rexpect::process::wait::WaitStatus;
use test_case::test_case;

const BINARY: &str = "target/debug/flakedb";

struct Repl {
    session: PtySession,
}

impl Repl {
    fn spawn() -> Result<Self> {
        Ok(Repl { session: rexpect::spawn(BINARY, Some(1000))? })
    }

    fn execute(&mut self, line: &str) -> Result<()> {
        self.session.exp_regex(r#"\nflakedb> "#)?;
        self.session.send_line(line)?;
        Ok(())
    }

    fn expect_error(&mut self, pattern :&str) {
        self.session.exp_regex(pattern).expect("did not find expected error message");
    }

    fn expect_no_error(&mut self, pattern :&str) {
        self.session.exp_regex(pattern).expect_err("found unexpected error message");
    }
}

#[test]
fn launch_and_exit() -> Result<()> {
    let mut repl = Repl::spawn()?;
    repl.session.exp_regex("^flakedb v0.1.0").expect("wrong splash text");
    repl.session.exp_regex(r#"\nflakedb> "#).expect("wrong prompt");
    repl.session.send_line(".exit")?;
    let status = repl.session.process.wait().unwrap();
    assert!(matches!(status, WaitStatus::Exited(_, 0)));
    Ok(())
}

#[test]
fn insert_and_select() -> Result<()> {
    let mut repl = Repl::spawn()?;
    repl.execute("insert 1 karl karl.havok@hotmail.com")?;
    repl.execute("insert 2 dangerous dangerous.nights@yahoo.com")?;
    repl.execute("insert 3 fri day.nights@gmail.com")?;
    repl.execute("select")?;
    repl.session.exp_regex(r#"
1,karl,karl.havok@hotmail\.com\r?
2,dangerous,dangerous\.nights@yahoo\.com\r?
3,fri,day\.nights@gmail\.com\r?
"#).unwrap();
    Ok(())
}

#[test]
fn table_full() -> Result<()> {
    let mut repl = Repl::spawn()?;
    for _ in 0..1500 {
        repl.execute("insert 1 karl karl.havok@hotmail.com")?;
    }
    repl.expect_error("table full");
    Ok(())
}

#[test]
fn table_not_full() -> Result<()> {
    let mut repl = Repl::spawn()?;
    for _ in 0..1000 {
        repl.execute("insert 1 karl karl.havok@hotmail.com")?;
    }
    repl.expect_no_error("table full");
    Ok(())
}

#[test_case("one" ; "non numeric ID")]
#[test_case("1.43" ; "decimal ID")]
#[test_case("-14" ; "negative integer ID")]
fn invalid_id(id_string: &str) -> Result<()> {
    let mut repl = Repl::spawn()?;
    repl.execute(&format!("insert {} karl karl.havok@hotmail.com", id_string))?;
    repl.expect_error("failed while parsing id");
    Ok(())
}

#[test]
fn valid_username() -> Result<()> {
    let mut repl = Repl::spawn()?;
    let long_username: String = (0..20).map(|_| "a").collect();
    repl.execute(&format!("insert 1 {} a@b.c", long_username))?;
    repl.expect_no_error("username too long");
    Ok(())
}

#[test]
fn invalid_username() -> Result<()> {
    let mut repl = Repl::spawn()?;
    let long_username: String = (0..100).map(|_| "a").collect();
    repl.execute(&format!("insert 1 {} a@b.c", long_username))?;
    repl.expect_error("username too long");
    Ok(())
}

#[test]
fn valid_email() -> Result<()> {
    let mut repl = Repl::spawn()?;
    let long_email: String = (0..100).map(|_| "a").collect();
    repl.execute(&format!("insert 1 karl {}", long_email))?;
    repl.expect_no_error("email too long");
    Ok(())
}

#[test]
fn invalid_email() -> Result<()> {
    let mut repl = Repl::spawn()?;
    let long_email: String = (0..500).map(|_| "a").collect();
    repl.execute(&format!("insert 1 karl {}", long_email))?;
    repl.expect_error("email too long");
    Ok(())
}
