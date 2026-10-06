//! The invite token: `faber1.` followed by base64url (no padding) of the JSON
//! [`InviteToken`].
//!
//! The token carries the new node's permanent WireGuard private key, so
//! `Debug` redacts it and the token itself must never be logged.

use std::fmt;
use std::net::Ipv4Addr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};

/// Every token starts with this. install.sh checks the shape
/// `^faber1\.[A-Za-z0-9_-]+$`.
pub const TOKEN_PREFIX: &str = "faber1.";
/// Value of [`InviteToken::v`] this build writes and accepts.
pub const TOKEN_VERSION: u32 = 1;
/// Longest token [`InviteToken::decode`] accepts, in bytes.
pub const MAX_TOKEN_LEN: usize = 64 * 1024;

/// Everything a new node needs to bring up faber0 and join.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteToken {
    /// Format version, [`TOKEN_VERSION`].
    pub v: u32,
    /// 128-bit random, lowercase hex.
    pub invite_id: String,
    /// Unix seconds, by the leader's clock.
    pub expires_at: u64,
    pub node_id: u64,
    pub mesh_ip: Ipv4Addr,
    pub mesh_cidr: Ipv4Net,
    /// The new node's own public WireGuard endpoint, as given to
    /// `mesh invite --wg-endpoint`. The joiner must register with exactly this.
    pub endpoint: String,
    /// The new node's WireGuard private key, standard base64.
    pub private_key: String,
    /// Existing nodes to add as wg peers before joining.
    pub peers: Vec<TokenPeer>,
    /// Peer listener addresses (`mesh_ip:port`) to send the join to, leader
    /// first.
    pub join_addrs: Vec<String>,
}

/// An existing node as listed in a token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenPeer {
    pub node_id: u64,
    pub public_key: String,
    /// `None` when the node has no endpoint; it is still a peer (it can
    /// reach the new node), just not one the new node can dial.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub mesh_ip: Ipv4Addr,
}

/// Why a token string could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("token must start with {TOKEN_PREFIX}")]
    BadPrefix,
    #[error("token is longer than {MAX_TOKEN_LEN} bytes")]
    TooLong,
    #[error("token is not valid base64url: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("token payload is not valid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported token version {0}")]
    UnsupportedVersion(u64),
}

impl InviteToken {
    /// `faber1.` + base64url(JSON), no padding.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("InviteToken serialises");
        format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
    }

    /// Parses a token. Surrounding whitespace (a trailing newline from a file
    /// or stdin) is ignored. The version is checked before the other fields,
    /// so a newer format reports [`TokenError::UnsupportedVersion`].
    pub fn decode(token: &str) -> Result<Self, TokenError> {
        let token = token.trim();
        if token.len() > MAX_TOKEN_LEN {
            return Err(TokenError::TooLong);
        }
        let body = token
            .strip_prefix(TOKEN_PREFIX)
            .ok_or(TokenError::BadPrefix)?;
        let bytes = URL_SAFE_NO_PAD.decode(body)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        match value.get("v").and_then(serde_json::Value::as_u64) {
            Some(v) if v == u64::from(TOKEN_VERSION) => {}
            Some(v) => return Err(TokenError::UnsupportedVersion(v)),
            None => return Err(TokenError::UnsupportedVersion(0)),
        }
        Ok(serde_json::from_value(value)?)
    }
}

impl fmt::Debug for InviteToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InviteToken")
            .field("v", &self.v)
            .field("invite_id", &self.invite_id)
            .field("expires_at", &self.expires_at)
            .field("node_id", &self.node_id)
            .field("mesh_ip", &self.mesh_ip)
            .field("mesh_cidr", &self.mesh_cidr)
            .field("endpoint", &self.endpoint)
            .field("private_key", &"<redacted>")
            .field("peers", &self.peers)
            .field("join_addrs", &self.join_addrs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const KEY: &str = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";

    fn sample() -> InviteToken {
        InviteToken {
            v: TOKEN_VERSION,
            invite_id: "0123456789abcdef0123456789abcdef".into(),
            expires_at: 1_700_003_600,
            node_id: 2,
            mesh_ip: "10.99.0.2".parse().unwrap(),
            mesh_cidr: "10.99.0.0/16".parse().unwrap(),
            endpoint: "198.51.100.2:51820".into(),
            private_key: KEY.into(),
            peers: vec![TokenPeer {
                node_id: 1,
                public_key: "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=".into(),
                endpoint: Some("203.0.113.1:51820".into()),
                mesh_ip: "10.99.0.1".parse().unwrap(),
            }],
            join_addrs: vec!["10.99.0.1:7401".into()],
        }
    }

    fn encode_value(v: serde_json::Value) -> String {
        format!(
            "{TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&v).unwrap())
        )
    }

    #[test]
    fn prefix_and_version() {
        assert_eq!(TOKEN_PREFIX, "faber1.");
        assert_eq!(TOKEN_VERSION, 1);
    }

    #[test]
    fn round_trips() {
        let t = sample();
        let s = t.encode();
        assert!(s.starts_with("faber1."));
        let body = &s[TOKEN_PREFIX.len()..];
        assert!(
            body.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "not base64url without padding: {body}"
        );
        assert_eq!(InviteToken::decode(&s).unwrap(), t);
    }

    #[test]
    fn decode_ignores_surrounding_whitespace() {
        let s = format!("  {}\n", sample().encode());
        assert_eq!(InviteToken::decode(&s).unwrap(), sample());
    }

    #[test]
    fn json_shape() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(
            v,
            json!({
                "v": 1,
                "invite_id": "0123456789abcdef0123456789abcdef",
                "expires_at": 1_700_003_600u64,
                "node_id": 2,
                "mesh_ip": "10.99.0.2",
                "mesh_cidr": "10.99.0.0/16",
                "endpoint": "198.51.100.2:51820",
                "private_key": KEY,
                "peers": [{
                    "node_id": 1,
                    "public_key": "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=",
                    "endpoint": "203.0.113.1:51820",
                    "mesh_ip": "10.99.0.1"
                }],
                "join_addrs": ["10.99.0.1:7401"]
            })
        );
    }

    #[test]
    fn rejects_bad_prefix() {
        let s = sample().encode().replacen("faber1.", "faber2.", 1);
        assert!(matches!(
            InviteToken::decode(&s),
            Err(TokenError::BadPrefix)
        ));
        assert!(matches!(
            InviteToken::decode(""),
            Err(TokenError::BadPrefix)
        ));
    }

    #[test]
    fn rejects_bad_base64() {
        assert!(matches!(
            InviteToken::decode("faber1.not*base64"),
            Err(TokenError::Base64(_))
        ));
    }

    #[test]
    fn rejects_bad_json() {
        let s = format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(b"{not json"));
        assert!(matches!(InviteToken::decode(&s), Err(TokenError::Json(_))));
    }

    #[test]
    fn rejects_other_versions() {
        let mut v = serde_json::to_value(sample()).unwrap();
        v["v"] = json!(2);
        assert!(matches!(
            InviteToken::decode(&encode_value(v.clone())),
            Err(TokenError::UnsupportedVersion(2))
        ));
        v.as_object_mut().unwrap().remove("v");
        assert!(matches!(
            InviteToken::decode(&encode_value(v)),
            Err(TokenError::UnsupportedVersion(0))
        ));
    }

    #[test]
    fn rejects_missing_fields() {
        let mut v = serde_json::to_value(sample()).unwrap();
        v.as_object_mut().unwrap().remove("private_key");
        assert!(matches!(
            InviteToken::decode(&encode_value(v)),
            Err(TokenError::Json(_))
        ));
    }

    #[test]
    fn rejects_oversized_token() {
        let s = format!("{TOKEN_PREFIX}{}", "A".repeat(MAX_TOKEN_LEN));
        assert!(matches!(InviteToken::decode(&s), Err(TokenError::TooLong)));
    }

    #[test]
    fn debug_redacts_private_key() {
        let dbg = format!("{:?}", sample());
        assert!(!dbg.contains(KEY), "{dbg}");
        assert!(dbg.contains("<redacted>"), "{dbg}");
        assert!(dbg.contains("10.99.0.2"), "{dbg}");
    }

    #[test]
    fn peer_endpoint_defaults_to_none() {
        let p: TokenPeer = serde_json::from_value(json!({
            "node_id": 1, "public_key": "pk", "mesh_ip": "10.99.0.1"
        }))
        .unwrap();
        assert_eq!(p.endpoint, None);
    }
}
