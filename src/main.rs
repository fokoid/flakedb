use flakedb::cli;
use std::path::PathBuf;
use std::process;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = cli::NAME)]
struct Args {
    #[structopt(name = "DB_FILE", parse(from_os_str))]
    db_path: Option<PathBuf>,
}

fn main() -> Result<(), cli::Error> {
    let args = Args::from_args();

    cli::print_splash()?;
    // main loop lives in a block to ensure database is Dropped before we call exit()
    let exit_code = {
        let mut db = cli::open_database(args.db_path.as_ref())?;

        loop {
            cli::print_prompt()?;
            let command = match cli::read_input() {
                Err(cli::Error::MetaSyntaxError(s)) => {
                    eprintln!("Syntax error: {}.", s);
                    cli::Command::None
                }
                Err(cli::Error::SqlError(error)) => {
                    eprintln!("SQL error: {}.", error);
                    cli::Command::None
                }
                // any unhandled errors should propagate up and cause a panic
                x => x?,
            };
            match command.execute(&mut db) {
                Err(cli::Error::Exit(code)) => break code,
                Err(cli::Error::SqlError(error)) => {
                    eprintln!("SQL error: {}.", error);
                    Ok(())
                }
                x => x,
            }?;
        }
    };
    process::exit(exit_code);
}
