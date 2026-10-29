//! API-key authentication and namespace-scoped authorization.
//!
//! Tokens are never stored: the auth file holds their SHA-256 hashes, so a
//! leaked config file does not leak credentials. Lookup is by hash, which
//! also avoids the timing side channel of comparing secrets byte by byte.
//!
//! ```toml
//! [[key]]
//! name = "sicare-backend"
//! token_sha256 = "<64 hex chars, from `ekv token`>"
//! access = "read_write"          # read | read_write | admin
//! namespaces = ["sessions", "otp"]   # or ["*"]
//! ```

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::keyspace;

/// What a key may do. Ordered: each level includes the ones before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// Read keys in the key's namespaces.
    Read,
    /// Read and write keys in the key's namespaces.
    ReadWrite,
    /// Everything, in every namespace, plus admin endpoints and the binary protocol.
    Admin,
}

/// An authenticated caller.
#[derive(Debug, Clone)]
pub struct Principal {
    name: String,
    access: Access,
    namespaces: Scope,
}

#[derive(Debug, Clone)]
enum Scope {
    All,
    Only(HashSet<String>),
}

impl Principal {
    /// The key's name from the auth file (safe to log).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether this caller may perform an operation needing `needed` in `namespace`.
    pub fn can(&self, namespace: &str, needed: Access) -> bool {
        if self.access == Access::Admin {
            return true;
        }
        self.access >= needed
            && match &self.namespaces {
                Scope::All => true,
                Scope::Only(allowed) => allowed.contains(namespace),
            }
    }

    /// Whether this caller has admin access.
    pub fn is_admin(&self) -> bool {
        self.access == Access::Admin
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthFile {
    #[serde(rename = "key", default)]
    keys: Vec<KeyEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEntry {
    name: String,
    token_sha256: String,
    access: Access,
    #[serde(default)]
    namespaces: Vec<String>,
}

/// Resolves bearer tokens to [`Principal`]s.
pub struct Authenticator {
    by_hash: HashMap<[u8; 32], Arc<Principal>>,
}

impl fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print hashes, even in debug output.
        f.debug_struct("Authenticator").field("keys", &self.by_hash.len()).finish()
    }
}

impl Authenticator {
    /// Loads and validates an auth file.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading auth file {}", path.display()))?;
        Self::from_toml(&text).with_context(|| format!("invalid auth file {}", path.display()))
    }

    /// Parses and validates auth configuration.
    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let file: AuthFile = toml::from_str(text)?;
        ensure!(!file.keys.is_empty(), "no [[key]] entries defined");
        let mut by_hash = HashMap::new();
        let mut names = HashSet::new();
        for entry in file.keys {
            let name = entry.name;
            ensure!(names.insert(name.clone()), "duplicate key name {name:?}");
            let hash = parse_hash(&entry.token_sha256).with_context(|| format!("key {name:?}"))?;
            let namespaces = if entry.namespaces.iter().any(|ns| ns == "*") {
                Scope::All
            } else {
                for ns in &entry.namespaces {
                    keyspace::validate_namespace(ns).map_err(|err| anyhow!("key {name:?}: {err}"))?;
                }
                ensure!(
                    entry.access == Access::Admin || !entry.namespaces.is_empty(),
                    "key {name:?} has no namespaces; use [\"*\"] to allow all"
                );
                Scope::Only(entry.namespaces.into_iter().collect())
            };
            let principal = Arc::new(Principal { name, access: entry.access, namespaces });
            ensure!(by_hash.insert(hash, principal).is_none(), "two keys share the same token");
        }
        Ok(Self { by_hash })
    }

    /// Returns the principal owning `token`, if any.
    pub fn authenticate(&self, token: &[u8]) -> Option<Arc<Principal>> {
        self.by_hash.get(&sha256(token)).cloned()
    }
}

/// Hex-encoded SHA-256 of a token, as stored in the auth file.
pub fn hash_token(token: &str) -> String {
    sha256(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn parse_hash(hex: &str) -> anyhow::Result<[u8; 32]> {
    ensure!(hex.len() == 64, "token_sha256 must be 64 hex characters");
    let mut hash = [0u8; 32];
    for (i, byte) in hash.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).context("token_sha256 is not valid hex")?;
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(entries: &str) -> anyhow::Result<Authenticator> {
        Authenticator::from_toml(entries)
    }

    fn entry(name: &str, token: &str, access: &str, namespaces: &str) -> String {
        format!(
            "[[key]]\nname = \"{name}\"\ntoken_sha256 = \"{}\"\naccess = \"{access}\"\nnamespaces = {namespaces}\n",
            hash_token(token)
        )
    }

    #[test]
    fn scopes_and_access_levels() {
        let auth = config(&format!(
            "{}{}{}",
            entry("reader", "t-read", "read", "[\"flags\"]"),
            entry("writer", "t-write", "read_write", "[\"sessions\", \"otp\"]"),
            entry("ops", "t-admin", "admin", "[]"),
        ))
        .unwrap();

        let reader = auth.authenticate(b"t-read").unwrap();
        assert!(reader.can("flags", Access::Read));
        assert!(!reader.can("flags", Access::ReadWrite));
        assert!(!reader.can("sessions", Access::Read));

        let writer = auth.authenticate(b"t-write").unwrap();
        assert!(writer.can("otp", Access::ReadWrite));
        assert!(!writer.can("flags", Access::Read));
        assert!(!writer.is_admin());

        let admin = auth.authenticate(b"t-admin").unwrap();
        assert!(admin.can("anything", Access::Admin));

        assert!(auth.authenticate(b"wrong").is_none());
    }

    #[test]
    fn rejects_invalid_files() {
        assert!(config("").is_err(), "no keys");
        assert!(config(&entry("a", "t", "read", "[]")).is_err(), "non-admin without namespaces");
        assert!(config(&entry("a", "t", "read", "[\"Bad/Name\"]")).is_err(), "invalid namespace");
        assert!(
            config(&format!(
                "{}{}",
                entry("a", "t1", "read", "[\"x\"]"),
                entry("a", "t2", "read", "[\"x\"]")
            ))
            .is_err()
        );
        assert!(
            config(&format!("{}{}", entry("a", "t", "read", "[\"x\"]"), entry("b", "t", "read", "[\"x\"]")))
                .is_err()
        );
        assert!(
            config("[[key]]\nname=\"a\"\ntoken_sha256=\"zz\"\naccess=\"read\"\nnamespaces=[\"*\"]").is_err()
        );
        assert!(
            config(&format!("{}typo = 1\n", entry("a", "t", "read", "[\"x\"]"))).is_err(),
            "unknown field"
        );
    }

    #[test]
    fn debug_output_hides_hashes() {
        let auth = config(&entry("a", "secret", "read", "[\"*\"]")).unwrap();
        assert!(!format!("{auth:?}").contains(&hash_token("secret")));
    }
}
