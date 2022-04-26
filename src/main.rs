use clap::{Parser, Subcommand};
use std::panic;
mod process;
use std::process::exit;

/// Pre-process data for simplified ptranse
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Parse files
    Parse {
        /// Files to process
        files: Vec<String>,
    },
}

fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        exit(1);
    }));
    let args = Args::parse();
    match &args.command {
        Commands::Parse { files } => {
            for file in files {
                match process::process(file) {
                    Ok(_) => println!("Processed {}", file),
                    Err(e) => eprintln!("Error processing {}: {}", file, e),
                };
            }
        }
    }
}
