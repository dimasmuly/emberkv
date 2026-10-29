//! `ekv`: command-line client for emberkv.

#![allow(clippy::print_stdout)]

mod bench;
mod token;

use anyhow::Context;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use emberkv_client::Client;

#[derive(Debug, Parser)]
#[command(name = "ekv", version, about = "Command-line client for emberkv")]
struct Cli {
    /// Server address (binary protocol).
    #[arg(long, short, env = "EMBERKV_ADDR", default_value = "127.0.0.1:7878", global = true)]
    addr: String,

    /// Admin API token, if the server has authentication enabled.
    #[arg(long, env = "EMBERKV_TOKEN", global = true, hide_env_values = true)]
    token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check that the server is reachable.
    Ping,
    /// Print the value of a key.
    Get { key: String },
    /// Set a key.
    Put { key: String, value: String },
    /// Delete a key.
    Del { key: String },
    /// List keys with a prefix.
    Scan {
        #[arg(default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Show engine statistics.
    Stats,
    /// Run a load test and report throughput and latency percentiles.
    Bench(bench::BenchArgs),
    /// Generate a new API token and its auth-file entry.
    Token(token::TokenArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut client = match cli.command {
        Command::Token(args) => return token::generate(&args),
        Command::Bench(args) => return bench::run(&cli.addr, cli.token.as_deref(), args).await,
        _ => connect(&cli.addr, cli.token.as_deref()).await?,
    };

    match cli.command {
        Command::Ping => {
            let started = std::time::Instant::now();
            client.ping().await?;
            println!("PONG ({:?})", started.elapsed());
        }
        Command::Get { key } => match client.get(key).await? {
            Some(value) => println!("{}", show(&value)),
            None => {
                eprintln!("(not found)");
                std::process::exit(1);
            }
        },
        Command::Put { key, value } => {
            client.put(key, value).await?;
            println!("OK");
        }
        Command::Del { key } => {
            println!("{}", if client.delete(key).await? { "deleted" } else { "(not found)" });
        }
        Command::Scan { prefix, limit } => {
            for (key, value) in client.scan_prefix(prefix, limit).await? {
                println!("{}\t{}", show(&key), show(&value));
            }
        }
        Command::Stats => {
            let stats = client.stats().await?;
            let dead = stats.total_bytes.saturating_sub(stats.live_bytes);
            println!("keys:          {}", stats.keys);
            println!("segments:      {}", stats.segments);
            println!("disk bytes:    {}", stats.total_bytes);
            println!("live bytes:    {}", stats.live_bytes);
            println!("garbage bytes: {dead}");
        }
        Command::Bench(_) | Command::Token(_) => unreachable!("handled above"),
    }
    Ok(())
}

/// Connects and, if a token is given, authenticates.
pub(crate) async fn connect(addr: &str, token: Option<&str>) -> anyhow::Result<Client> {
    let mut client = Client::connect(addr).await.with_context(|| format!("connecting to {addr}"))?;
    if let Some(token) = token {
        client.authenticate(token.to_owned()).await.context("authenticating")?;
    }
    Ok(client)
}

fn show(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
