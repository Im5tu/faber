//! WireGuard (X25519) keys in the standard base64 form `wg` uses.
//!
//! [`generate_keypair`] clamps the private key the way `wg genkey` does, so
//! its output is interchangeable with the `wg` tool's.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use x25519_dalek::{PublicKey, StaticSecret};

/// Length of a raw X25519 key.
pub const KEY_LEN: usize = 32;

/// A base64 WireGuard private key. `Debug` and `Display` never print it;
/// call [`PrivateKey::expose`] where the key really is needed (wg syncconf
/// stdin, identity.json, the invite token).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrivateKey(String);

impl PrivateKey {
    /// Wraps a base64 private key without checking it. Use
    /// [`PrivateKey::parse`] for untrusted input.
    pub fn new(b64: impl Into<String>) -> Self {
        Self(b64.into())
    }

    /// Wraps a base64 private key after checking it decodes to 32 bytes.
    pub fn parse(b64: &str) -> Result<Self, KeyError> {
        decode_key(b64)?;
        Ok(Self(b64.to_string()))
    }

    /// The base64 key.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The matching base64 public key.
    pub fn public_key(&self) -> Result<String, KeyError> {
        public_key_of(&self.0)
    }
}

impl fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrivateKey(<redacted>)")
    }
}

impl fmt::Display for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Why a key could not be generated or decoded. Never contains key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("key is not valid standard base64")]
    Base64,
    #[error("key must be {KEY_LEN} bytes, got {0}")]
    Length(usize),
    #[error("system random number generator failed: {0}")]
    Rng(String),
}

/// A new clamped private key and its public key, both standard base64, as
/// `wg genkey | tee priv | wg pubkey` would produce.
pub fn generate_keypair() -> Result<(PrivateKey, String), KeyError> {
    let mut bytes = [0u8; KEY_LEN];
    getrandom::fill(&mut bytes).map_err(|e| KeyError::Rng(e.to_string()))?;
    clamp(&mut bytes);
    let secret = StaticSecret::from(bytes);
    let public = PublicKey::from(&secret);
    let private = PrivateKey(STANDARD.encode(secret.to_bytes()));
    Ok((private, STANDARD.encode(public.as_bytes())))
}

/// The base64 public key for a base64 private key, like `wg pubkey`.
pub fn public_key_of(private_b64: &str) -> Result<String, KeyError> {
    let secret = StaticSecret::from(decode_key(private_b64)?);
    Ok(STANDARD.encode(PublicKey::from(&secret).as_bytes()))
}

/// Checks that `key` is standard base64 (with padding) of 32 bytes.
pub fn validate_public_key(key: &str) -> Result<(), KeyError> {
    decode_key(key).map(|_| ())
}

fn decode_key(b64: &str) -> Result<[u8; KEY_LEN], KeyError> {
    let bytes = STANDARD.decode(b64).map_err(|_| KeyError::Base64)?;
    <[u8; KEY_LEN]>::try_from(bytes.as_slice()).map_err(|_| KeyError::Length(bytes.len()))
}

/// Curve25519 clamping, as `wg genkey` applies before printing.
fn clamp(k: &mut [u8; KEY_LEN]) {
    k[0] &= 248;
    k[31] &= 127;
    k[31] |= 64;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> [u8; KEY_LEN] {
        let v: Vec<u8> = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    fn b64_of_hex(s: &str) -> String {
        STANDARD.encode(hex(s))
    }

    #[test]
    fn rfc7748_alice_and_bob() {
        // RFC 7748 section 6.1.
        let alice = b64_of_hex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let bob = b64_of_hex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        assert_eq!(
            public_key_of(&alice).unwrap(),
            b64_of_hex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
        );
        assert_eq!(
            public_key_of(&bob).unwrap(),
            b64_of_hex("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
        );
    }

    #[test]
    fn matches_wg_pubkey() {
        // `echo yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk= | wg pubkey`
        assert_eq!(
            public_key_of("yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=").unwrap(),
            "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw="
        );
    }

    #[test]
    fn generated_keys_are_clamped_and_match() {
        let (private, public) = generate_keypair().unwrap();
        let raw = decode_key(private.expose()).unwrap();
        assert_eq!(raw[0] & 7, 0);
        assert_eq!(raw[31] & 0x80, 0);
        assert_eq!(raw[31] & 0x40, 0x40);
        assert_eq!(public_key_of(private.expose()).unwrap(), public);
        assert_eq!(private.public_key().unwrap(), public);
        validate_public_key(&public).unwrap();
        assert_eq!(private.expose().len(), 44);
        assert!(private.expose().ends_with('='));
    }

    #[test]
    fn generated_keys_differ() {
        let (a, _) = generate_keypair().unwrap();
        let (b, _) = generate_keypair().unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn validate_public_key_rejects_bad_input() {
        validate_public_key("HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=").unwrap();
        assert_eq!(validate_public_key("not base64!"), Err(KeyError::Base64));
        // Missing padding.
        assert_eq!(
            validate_public_key("HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw"),
            Err(KeyError::Base64)
        );
        // URL-safe alphabet is not accepted.
        assert_eq!(
            validate_public_key("yAnz5TF-lXXJte14tji3zlMNq-hd2rYUIgJBgB3fBmk="),
            Err(KeyError::Base64)
        );
        assert_eq!(validate_public_key("AAAA"), Err(KeyError::Length(3)));
        assert_eq!(validate_public_key(""), Err(KeyError::Length(0)));
        assert!(public_key_of("AAAA").is_err());
        assert!(PrivateKey::parse("AAAA").is_err());
    }

    #[test]
    fn private_key_is_redacted() {
        let k = PrivateKey::parse("yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=").unwrap();
        for s in [format!("{k:?}"), format!("{k}"), format!("{k:#?}")] {
            assert!(!s.contains("yAnz5"), "{s}");
            assert!(s.contains("redacted"), "{s}");
        }
    }

    #[test]
    fn private_key_serialises_as_plain_string() {
        let k = PrivateKey::new("yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=");
        let j = serde_json::to_string(&k).unwrap();
        assert_eq!(j, "\"yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=\"");
        assert_eq!(serde_json::from_str::<PrivateKey>(&j).unwrap(), k);
    }
}
