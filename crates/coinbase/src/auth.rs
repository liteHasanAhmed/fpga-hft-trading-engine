//! WebSocket subscription signing (only needed if the `full` channel requires auth).
//!
//! Signature = base64(HMAC-SHA256(base64decode(secret), timestamp + "GET" + "/users/self/verify")).

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Clone)]
pub struct Credentials {
    pub key: String,
    pub secret_b64: String,
    pub passphrase: String,
}

impl Credentials {
    /// Read `COINBASE_API_KEY`, `COINBASE_API_SECRET`, `COINBASE_API_PASSPHRASE`.
    pub fn from_env() -> Option<Credentials> {
        Some(Credentials {
            key: std::env::var("COINBASE_API_KEY").ok()?,
            secret_b64: std::env::var("COINBASE_API_SECRET").ok()?,
            passphrase: std::env::var("COINBASE_API_PASSPHRASE").ok()?,
        })
    }

    pub fn sign(&self, timestamp: &str) -> anyhow::Result<String> {
        let key = STANDARD.decode(&self.secret_b64)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(timestamp.as_bytes());
        mac.update(b"GET/users/self/verify");
        Ok(STANDARD.encode(mac.finalize().into_bytes()))
    }
}

/// Build the subscribe message for `full` + `heartbeat` on one product.
pub fn subscribe_msg(product: &str, creds: Option<&Credentials>) -> anyhow::Result<String> {
    let mut v = serde_json::json!({
        "type": "subscribe",
        "product_ids": [product],
        "channels": ["full", "heartbeat"],
    });
    if let Some(c) = creds {
        let ts = format!("{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs());
        v["signature"] = c.sign(&ts)?.into();
        v["key"] = c.key.clone().into();
        v["passphrase"] = c.passphrase.clone().into();
        v["timestamp"] = ts.into();
    }
    Ok(v.to_string())
}
