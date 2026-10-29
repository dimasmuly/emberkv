//! `ekv token`: mint an API token and print the matching auth-file entry.
//!
//! Only the SHA-256 of the token goes into the server's auth file; the token
//! itself is shown once and must be stored by whoever will use it (e.g. in
//! the application's secret manager).

use anyhow::Context;
use sha2::{Digest, Sha256};

#[derive(Debug, clap::Args)]
pub(crate) struct TokenArgs {
    /// Name for the key, shown in logs (e.g. "sicare-backend").
    #[arg(long)]
    name: String,
    /// Access level.
    #[arg(long, value_parser = ["read", "read_write", "admin"], default_value = "read_write")]
    access: String,
    /// Comma-separated namespaces the key may use, or "*" for all.
    #[arg(long, value_delimiter = ',', default_value = "*")]
    namespaces: Vec<String>,
}

pub(crate) fn generate(args: &TokenArgs) -> anyhow::Result<()> {
    // 256 bits from the OS CSPRNG: infeasible to guess.
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).context("reading OS randomness")?;
    let token = format!("ekv_{}", hex(&secret));
    let hash = hex(&Sha256::digest(token.as_bytes()));
    let namespaces = args.namespaces.iter().map(|ns| format!("{ns:?}")).collect::<Vec<_>>().join(", ");

    println!("Token (shown once; store it in your secret manager):\n");
    println!("  {token}\n");
    println!("Add this to the server's auth file:\n");
    println!("[[key]]");
    println!("name = {:?}", args.name);
    println!("token_sha256 = \"{hash}\"");
    println!("access = \"{}\"", args.access);
    println!("namespaces = [{namespaces}]");
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
