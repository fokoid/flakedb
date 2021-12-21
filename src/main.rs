use flakedb::cli;
use std::process;

fn main() -> Result<(), cli::Error> {
    loop {
        cli::print_prompt()?;
        let command = match cli::read_input() {
            Err(cli::Error::MetaError(raw)) => {
                println!("Invalid meta command: '{}'.", raw);
                cli::Command::None
            }
            Err(cli::Error::SqlError(err)) => {
                println!("Invalid SQL: '{:?}'.", err);
                cli::Command::None
            }
            // any unhandled errors should propagate up and cause a panic
            x => x?,
        };
        match command.execute() {
            Err(cli::Error::Exit(code)) => process::exit(code),
            x => x,
        }?;
    }
}
