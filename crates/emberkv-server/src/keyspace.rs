//! Mapping between the HTTP API's `(namespace, key)` pairs and engine keys.
//!
//! Engine key = `{namespace}/{key}`. Namespace names cannot contain `/`, so
//! the mapping is unambiguous, and each namespace occupies one contiguous key
//! range, which makes listing a namespace a cheap range scan.

use std::time::Duration;

const SEPARATOR: char = '/';
const MAX_NAMESPACE_LEN: usize = 64;

/// Namespaces are 1–64 characters of `[a-z0-9_-]`, starting with a letter or digit.
pub(crate) fn validate_namespace(namespace: &str) -> Result<(), String> {
    let valid_chars =
        namespace.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    let valid_start = namespace.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric());
    if namespace.len() > MAX_NAMESPACE_LEN || !valid_chars || !valid_start {
        return Err(format!(
            "invalid namespace {namespace:?}: use 1-{MAX_NAMESPACE_LEN} characters of a-z, 0-9, '_' or '-'"
        ));
    }
    Ok(())
}

/// Engine key for `key` inside `namespace` (validated by the caller).
pub(crate) fn storage_key(namespace: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(namespace.len() + 1 + key.len());
    out.extend_from_slice(namespace.as_bytes());
    out.push(SEPARATOR as u8);
    out.extend_from_slice(key.as_bytes());
    out
}

/// The user-facing key for an engine key inside `namespace`.
pub(crate) fn user_key(namespace: &str, storage_key: &[u8]) -> String {
    let key = storage_key.get(namespace.len() + 1..).unwrap_or_default();
    String::from_utf8_lossy(key).into_owned()
}

/// Parses durations like `500ms`, `30s`, `15m`, `2h`, `7d`. A bare number is seconds.
pub(crate) fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let invalid = || format!("invalid duration {text:?}; use e.g. 500ms, 30s, 15m, 2h or 7d");
    let amount: u64 = number.parse().map_err(|_| invalid())?;
    let unit_ms: u64 = match unit {
        "ms" => 1,
        "" | "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(invalid()),
    };
    match amount.checked_mul(unit_ms) {
        Some(0) => Err(format!("duration {text:?} must be positive")),
        Some(ms) => Ok(Duration::from_millis(ms)),
        None => Err(format!("duration {text:?} is too large")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_rules() {
        for ok in ["sessions", "otp", "feature-flags", "rate_limits", "v2", "a"] {
            assert!(validate_namespace(ok).is_ok(), "{ok}");
        }
        for bad in ["", "Sessions", "with/slash", "-leading", "_x", "spa ce", &"x".repeat(65)] {
            assert!(validate_namespace(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn keys_roundtrip_and_namespaces_do_not_overlap() {
        let key = storage_key("otp", "user:42/login");
        assert_eq!(key, b"otp/user:42/login");
        assert_eq!(user_key("otp", &key), "user:42/login");
        // "ot" must not be a prefix-match for "otp" keys.
        assert!(!storage_key("otp", "x").starts_with(&storage_key("ot", "")));
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("15m"), Ok(Duration::from_secs(900)));
        assert_eq!(parse_duration(" 2h "), Ok(Duration::from_secs(7_200)));
        assert_eq!(parse_duration("7d"), Ok(Duration::from_secs(604_800)));
        for bad in ["", "0s", "1w", "-5s", "1.5h", "s", "99999999999999999999d"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }
}
