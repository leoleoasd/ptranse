use clap::{Parser, Subcommand};
mod process;

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
