mod logic;
mod tests;

use std::error::Error;

use clap::Parser;
use tokio::{fs::File, io::{BufReader, BufWriter, stdout}};

use crate::logic::process_file;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    input_file: String,

    #[arg(short = 'o', long = "output")]
    output: Option<String>,

    #[arg(short = 'n', long = "n-workers", default_value_t=32)]
    number_of_workers: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error parsing arguments: {}", e);
            println!("Usage: linkchecker <input_file> [-o <output_file>]");
            println!(
                "If no output file is specified, the program will print the results to stdout."
            );
            std::process::exit(1);
        }
    };

    let input_file = BufReader::new(File::open(&args.input_file).await?);
    let number_of_workers = args.number_of_workers;
    match args.output {
        Some(out) => match File::create(&out).await {
            Ok(file) => process_file(input_file, BufWriter::new(file), number_of_workers).await,
            Err(err) => {
                eprintln!("Error creating output file '{}': {}", out, err);
                std::process::exit(1);
            }
        },
        None => process_file(input_file, BufWriter::new(stdout()), number_of_workers).await,
    }
}