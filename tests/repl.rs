use std::process::Command;
use assert_fs::NamedTempFile;
use rexpect::session::{self, PtySession};
use rexpect::errors::Result;
use rexpect::process::wait::WaitStatus;
use test_case::test_case;

const BINARY: &str = env!("CARGO_BIN_EXE_flakedb");

struct Repl {
    session: PtySession,
}

impl Repl {
    fn spawn() -> Result<Self> {
        Self::spawn_with_args(vec![])
    }

    fn spawn_with_args(args: Vec<&str>) -> Result<Self> {
        let mut command = Command::new(BINARY);
        command.args(args);
        let session = session::spawn_command(command, Some(1000))?;
        Ok( Self { session })
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
fn persist_single_page() -> Result<()> {
    let db_file = NamedTempFile::new("persist_single_page.flake").unwrap();
    let db_path = db_file.path().to_string_lossy().into_owned();
    {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        repl.execute("insert 1 karl karl.havok@hotmail.com")?;
        repl.execute("insert 2 dangerous dangerous.nights@yahoo.com")?;
        repl.execute("insert 3 fri day.nights@gmail.com")?;
        repl.execute(".exit")?;
        repl.session.process.wait()?;
    }
    {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        repl.execute("select")?;
        let pattern = r#"
1,karl,karl.havok@hotmail\.com\r?
2,dangerous,dangerous\.nights@yahoo\.com\r?
3,fri,day\.nights@gmail\.com\r?
"#;
        let result = repl.session.exp_regex(pattern);
        if let Err(x) = &result {
            eprintln!("{}", x);
        }
        result.unwrap();
    }
    Ok(())
}

#[test] #[ignore] // broken by logging output #1
fn persist_multi_page() -> Result<()> {
    let db_file = NamedTempFile::new("persist_multi_page.flake").unwrap();
    let db_path = db_file.path().to_string_lossy().into_owned();
    let mut inserts = Vec::new();
    let mut expected_lines = Vec::new();
    for id in 0..20 {
        inserts.push(format!("insert {} karl{} karl.havok.{}@hotmail.com", id, id, id));
        expected_lines.push(format!(r#"{},karl{},karl\.havok\.{}@hotmail\.com"#, id, id, id));
    }
    {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        for insert in &inserts {
            repl.execute(insert)?;
        }
        repl.execute(".exit")?;
        repl.session.process.wait()?;
    }
    {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        repl.execute("select")?;
        let pattern = expected_lines.join(r#"\r\n"#);
        let result = repl.session.exp_regex(&pattern);
        if let Err(x) = &result {
            eprintln!("{}", x);
        }
        result.unwrap();
    }
    Ok(())
}

#[test] #[ignore] // broken by logging output #1 and partial page roundtrip issue #2
fn persist_repeated() -> Result<()> {
    let db_file = NamedTempFile::new("persist_repeated.flake").unwrap();
    let db_path = db_file.path().to_string_lossy().into_owned();
    let mut inserts = Vec::new();
    let mut expected_lines = Vec::new();

    let num_inserts = 20;
    let num_stages = 4;
    let inserts_per_stage = num_inserts / num_stages;
    let ranges = (0..num_stages).map(|i| {
        let start = i * inserts_per_stage;
        let end = start + inserts_per_stage;
        start..end
    });

    for id in 0..num_inserts {
        inserts.push(format!("insert {} karl{} karl.havok.{}@hotmail.com", id, id, id));
        expected_lines.push(format!(r#"{},karl{},karl\.havok\.{}@hotmail\.com"#, id, id, id));
    }
    for range in ranges {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        for insert in &inserts[range] {
            repl.execute(insert)?;
        }
        repl.execute(".exit")?;
        repl.session.process.wait()?;
    }
    {
        let mut repl = Repl::spawn_with_args(vec![&db_path])?;
        repl.execute("select")?;
        let pattern = expected_lines.join(r#"\r\n"#);
        let result = repl.session.exp_regex(&pattern);
        if let Err(x) = &result {
            eprintln!("{}", x);
        }
        result.unwrap();
    }
    Ok(())
}