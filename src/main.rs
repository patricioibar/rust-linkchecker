mod logic;
mod tests;

use clap::Parser;
use tokio::{
    fs::File,
    io::{BufReader, BufWriter, stdout},
};

use crate::logic::process_file;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    input_file: String,

    #[arg(short = 'o', long = "output")]
    output: Option<String>,

    #[arg(short = 'n', long = "n-workers", default_value_t = 32)]
    number_of_workers: usize,
}

#[tokio::main]
async fn main() -> Result<(), i32> {
    let Ok(args) = Args::try_parse().inspect_err(|e| eprintln!("Error parsing arguments: {}", e))
    else {
        println!("Usage: linkchecker <input_file> [-o <output_file>]");
        println!("If no output file is specified, the program will print the results to stdout.");
        return Err(1);
    };
    let Ok(input_file) = File::open(&args.input_file)
        .await
        .inspect_err(|e| eprintln!("Error opening input file '{}': {}", args.input_file, e))
    else {
        return Err(1);
    };
    let input = BufReader::new(input_file);
    let number_of_workers = args.number_of_workers;
    match args.output {
        Some(out) => {
            let Ok(file) = File::create(&out)
                .await
                .inspect_err(|e| eprintln!("Error creating output file '{}': {}", out, e))
            else {
                return Err(1);
            };
            process_file(input, BufWriter::new(file), number_of_workers).await
        }
        None => process_file(input, BufWriter::new(stdout()), number_of_workers).await,
    }
    Ok(())
}
