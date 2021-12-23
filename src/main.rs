use flakedb::cli;
use flakedb::Table;
use std::process;

fn main() -> Result<(), cli::Error> {
    let mut table = Table::new();

    loop {
        cli::print_prompt()?;
        let command = match cli::read_input() {
            Err(cli::Error::MetaSyntaxError(s)) => {
                println!("Syntax error: {}.", s);
                cli::Command::None
            },
            Err(cli::Error::SqlError(error)) => {
                println!("SQL error: {}.", error);
                cli::Command::None
            }
            // any unhandled errors should propagate up and cause a panic
            x => x?,
        };
        match command.execute(&mut table) {
            Err(cli::Error::Exit(code)) => process::exit(code),
            Err(cli::Error::SqlError(error)) => {
                println!("SQL error: {}.", error);
                Ok(())
            },
            x => x,
        }?;
    }
}
