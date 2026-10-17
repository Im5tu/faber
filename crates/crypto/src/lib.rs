//! Envelope encryption: data keys sealed under a cluster key.
//!
//! A [`ClusterKey`] wraps [`DataKey`]s ([`wrap`], [`unwrap`]); a data key
//! seals values ([`seal`], [`open`]). Both use XChaCha20-Poly1305 with a
//! random 24-byte nonce. The associated data is length-prefixed and starts
//! with a label of its own for each use, so a wrapped key cannot pass as a
//! sealed value and no two lists of parts encode alike.
//!
//! [`env_digest`] is an HMAC of the plaintext under the data key. It lets a
//! caller tell that an env map is unchanged without comparing ciphertexts,
//! which differ on every seal, and without a guessable hash of a secret.
//!
//! [`ClusterKey`] and [`DataKey`] zeroize on drop and print as `[redacted]`.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

/// Bytes in a key.
pub const KEY_LEN: usize = 32;
/// Bytes in a nonce.
const NONCE_LEN: usize = 24;

const WRAP_LABEL: &str = "faber/wrap/v1";
const SEAL_LABEL: &str = "faber/seal/v1";
const KEY_ID_LABEL: &[u8] = b"faber/key-id/v1";
const DIGEST_LABEL: &[u8] = b"faber/env-digest/v2";
/// Bytes of the HMAC kept in a key id.
const KEY_ID_BYTES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CryptoError {
    /// The key or the encoded value has the wrong length or is not base64.
    #[error("malformed key or sealed value")]
    Malformed,
    /// Wrong key, wrong associated data, or a changed ciphertext.
    #[error("decryption failed: wrong key, wrong scope or damaged data")]
    Open,
    /// The system has no randomness, or the input is too long to seal.
    #[error("encryption failed")]
    Seal,
}

macro_rules! key_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name([u8; KEY_LEN]);

        impl $name {
            /// A new random key.
            pub fn generate() -> Result<Self, CryptoError> {
                let mut bytes = [0u8; KEY_LEN];
                getrandom::fill(&mut bytes).map_err(|_| CryptoError::Seal)?;
                Ok(Self(bytes))
            }

            pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
                Self(bytes)
            }

            /// Decodes standard base64 of exactly 32 bytes; surrounding
            /// whitespace is ignored, as in a key file.
            pub fn from_base64(text: &str) -> Result<Self, CryptoError> {
                let mut raw = STANDARD
                    .decode(text.trim())
                    .map_err(|_| CryptoError::Malformed)?;
                let bytes = <[u8; KEY_LEN]>::try_from(raw.as_slice());
                raw.zeroize();
                bytes.map(Self).map_err(|_| CryptoError::Malformed)
            }

            /// The standard base64 of the key. The result is a secret and
            /// is wiped when dropped.
            pub fn to_base64(&self) -> Zeroizing<String> {
                Zeroizing::new(STANDARD.encode(self.0))
            }
        }

        impl Cipher for $name {
            fn cipher(&self) -> XChaCha20Poly1305 {
                XChaCha20Poly1305::new(&Key::from(self.0))
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                self.0.zeroize();
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([redacted])"))
            }
        }
    };
}

key_type! {
    /// The key that wraps data keys. One per cluster, kept in a file.
    ClusterKey
}
key_type! {
    /// A key that seals values: one per scope.
    DataKey
}

trait Cipher {
    fn cipher(&self) -> XChaCha20Poly1305;
}

/// A nonce and the ciphertext with its tag. Serializes as one base64
/// string.
#[derive(Clone, PartialEq, Eq)]
struct Blob {
    nonce: [u8; NONCE_LEN],
    ciphertext: Vec<u8>,
}

impl Blob {
    fn encrypt(key: &impl Cipher, plaintext: &[u8], aad: &[u8]) -> Result<Self, CryptoError> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|_| CryptoError::Seal)?;
        let ciphertext = key
            .cipher()
            .encrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| CryptoError::Seal)?;
        Ok(Self { nonce, ciphertext })
    }

    fn decrypt(&self, key: &impl Cipher, aad: &[u8]) -> Result<Vec<u8>, CryptoError> {
        key.cipher()
            .decrypt(
                &XNonce::from(self.nonce),
                Payload {
                    msg: &self.ciphertext,
                    aad,
                },
            )
            .map_err(|_| CryptoError::Open)
    }

    fn encode(&self) -> String {
        let mut raw = Vec::with_capacity(NONCE_LEN + self.ciphertext.len());
        raw.extend_from_slice(&self.nonce);
        raw.extend_from_slice(&self.ciphertext);
        STANDARD.encode(raw)
    }

    fn decode(text: &str) -> Result<Self, CryptoError> {
        let raw = STANDARD.decode(text).map_err(|_| CryptoError::Malformed)?;
        let (nonce, ciphertext) = raw
            .split_at_checked(NONCE_LEN)
            .ok_or(CryptoError::Malformed)?;
        Ok(Self {
            nonce: nonce.try_into().map_err(|_| CryptoError::Malformed)?,
            ciphertext: ciphertext.to_vec(),
        })
    }
}

macro_rules! blob_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name(Blob);

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({} bytes)", stringify!($name), self.0.ciphertext.len())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.0.encode())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let text = String::deserialize(d)?;
                Blob::decode(&text).map(Self).map_err(de::Error::custom)
            }
        }
    };
}

blob_type! {
    /// A data key sealed under the cluster key.
    WrappedKey
}
blob_type! {
    /// A value sealed under a data key.
    Sealed
}

/// Length-prefixes `parts` after `label`, so that no two lists encode alike.
fn aad(label: &str, parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in std::iter::once(&label).chain(parts) {
        out.extend_from_slice(&(part.len() as u64).to_be_bytes());
        out.extend_from_slice(part.as_bytes());
    }
    out
}

/// Seals `data_key` under `cluster_key`, bound to `scope`, the
/// `<project>/<environment>` it belongs to.
pub fn wrap(
    cluster_key: &ClusterKey,
    data_key: &DataKey,
    scope: &str,
) -> Result<WrappedKey, CryptoError> {
    Blob::encrypt(cluster_key, &data_key.0, &aad(WRAP_LABEL, &[scope])).map(WrappedKey)
}

/// Opens a [`WrappedKey`]. Fails with [`CryptoError::Open`] for another
/// cluster key, another scope or a changed ciphertext.
pub fn unwrap(
    cluster_key: &ClusterKey,
    wrapped: &WrappedKey,
    scope: &str,
) -> Result<DataKey, CryptoError> {
    let mut raw = wrapped.0.decrypt(cluster_key, &aad(WRAP_LABEL, &[scope]))?;
    let key = <[u8; KEY_LEN]>::try_from(raw.as_slice()).map(DataKey);
    raw.zeroize();
    key.map_err(|_| CryptoError::Malformed)
}

/// Seals `plaintext` under `data_key`, bound to the parts of `aad_parts`.
/// Open it with the same parts in the same order.
pub fn seal(
    data_key: &DataKey,
    plaintext: &[u8],
    aad_parts: &[&str],
) -> Result<Sealed, CryptoError> {
    Blob::encrypt(data_key, plaintext, &aad(SEAL_LABEL, aad_parts)).map(Sealed)
}

/// Opens a [`Sealed`] value.
pub fn open(
    data_key: &DataKey,
    sealed: &Sealed,
    aad_parts: &[&str],
) -> Result<Vec<u8>, CryptoError> {
    sealed.0.decrypt(data_key, &aad(SEAL_LABEL, aad_parts))
}

fn hmac(key: &[u8; KEY_LEN]) -> Hmac<Sha256> {
    // HMAC takes a key of any length, so this never fails.
    <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(key).expect("HMAC accepts any key length")
}

/// An id for the cluster key that is safe to store: the first 8 bytes of an
/// HMAC of a fixed label, in hex. Nodes compare it to notice a wrong key
/// file.
pub fn key_id(cluster_key: &ClusterKey) -> String {
    let mut mac = hmac(&cluster_key.0);
    mac.update(KEY_ID_LABEL);
    let tag = mac.finalize().into_bytes();
    tag[..KEY_ID_BYTES]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// An HMAC-SHA256 under `data_key` of a canonical, length-prefixed encoding
/// of `location` (project, environment, workload, container) and `env`.
/// Equal maps at one location give equal digests whatever order they were
/// built in; a changed name or value, or another location, gives another.
pub fn env_digest(
    data_key: &DataKey,
    location: [&str; 4],
    env: &BTreeMap<String, String>,
) -> [u8; 32] {
    let mut mac = hmac(&data_key.0);
    mac.update(DIGEST_LABEL);
    for part in location {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part.as_bytes());
    }
    mac.update(&(env.len() as u64).to_be_bytes());
    for (name, value) in env {
        mac.update(&(name.len() as u64).to_be_bytes());
        mac.update(name.as_bytes());
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value.as_bytes());
    }
    mac.finalize().into_bytes().into()
}

/// `#[serde(with = "faber_crypto::digest_serde")]` for a digest: one base64
/// string.
pub mod digest_serde {
    use super::*;

    pub fn serialize<S: Serializer>(digest: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(digest))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(d)?;
        let raw = STANDARD.decode(text).map_err(de::Error::custom)?;
        <[u8; 32]>::try_from(raw.as_slice())
            .map_err(|_| de::Error::custom("digest is not 32 bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster() -> ClusterKey {
        ClusterKey::generate().unwrap()
    }

    #[test]
    fn a_wrapped_key_round_trips() {
        let (ck, dk) = (cluster(), DataKey::generate().unwrap());
        let wrapped = wrap(&ck, &dk, "shop/staging").unwrap();
        assert_eq!(unwrap(&ck, &wrapped, "shop/staging").unwrap(), dk);
    }

    #[test]
    fn unwrap_fails_for_another_scope_key_or_changed_bytes() {
        let (ck, dk) = (cluster(), DataKey::generate().unwrap());
        let wrapped = wrap(&ck, &dk, "shop/staging").unwrap();
        assert_eq!(
            unwrap(&ck, &wrapped, "shop/production").unwrap_err(),
            CryptoError::Open
        );
        assert_eq!(
            unwrap(&ck, &wrapped, "shop").unwrap_err(),
            CryptoError::Open
        );
        assert_eq!(
            unwrap(&cluster(), &wrapped, "shop/staging").unwrap_err(),
            CryptoError::Open
        );
        let mut damaged = wrapped.clone();
        damaged.0.ciphertext[0] ^= 1;
        assert!(unwrap(&ck, &damaged, "shop/staging").is_err());
    }

    #[test]
    fn sealed_values_round_trip_and_differ_per_seal() {
        let dk = DataKey::generate().unwrap();
        let aad = ["shop", "staging", "web", "app", "TOKEN"];
        let a = seal(&dk, b"hunter2", &aad).unwrap();
        let b = seal(&dk, b"hunter2", &aad).unwrap();
        assert_ne!(a, b);
        assert_eq!(open(&dk, &a, &aad).unwrap(), b"hunter2");
        assert_eq!(open(&dk, &b, &aad).unwrap(), b"hunter2");
    }

    #[test]
    fn open_fails_for_other_aad_key_or_changed_bytes() {
        let dk = DataKey::generate().unwrap();
        let sealed = seal(&dk, b"hunter2", &["a", "b"]).unwrap();
        for other in [&["a"][..], &["b", "a"], &["a", "b", ""], &["ab"]] {
            assert_eq!(open(&dk, &sealed, other).unwrap_err(), CryptoError::Open);
        }
        assert!(open(&DataKey::generate().unwrap(), &sealed, &["a", "b"]).is_err());
        let mut damaged = sealed.clone();
        *damaged.0.ciphertext.last_mut().unwrap() ^= 1;
        assert!(open(&dk, &damaged, &["a", "b"]).is_err());
        let mut nonce = sealed.clone();
        nonce.0.nonce[0] ^= 1;
        assert!(open(&dk, &nonce, &["a", "b"]).is_err());
    }

    #[test]
    fn a_wrapped_key_is_not_a_sealed_value() {
        let (ck, dk) = (cluster(), DataKey::generate().unwrap());
        let wrapped = wrap(&ck, &dk, "shop/staging").unwrap();
        let as_value = Sealed(wrapped.0.clone());
        let same_bytes = DataKey::from_bytes(ck.0);
        assert!(open(&same_bytes, &as_value, &["shop/staging"]).is_err());
    }

    #[test]
    fn key_ids_differ_per_key_and_are_stable() {
        let (a, b) = (cluster(), cluster());
        assert_ne!(key_id(&a), key_id(&b));
        assert_eq!(key_id(&a), key_id(&a.clone()));
        assert_eq!(key_id(&a).len(), KEY_ID_BYTES * 2);
    }

    const AT: [&str; 4] = ["shop", "staging", "web", "app"];

    #[test]
    fn the_digest_ignores_order_and_follows_content() {
        let dk = DataKey::generate().unwrap();
        let build = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let one = build(&[("A", "1"), ("B", "2")]);
        let two = build(&[("B", "2"), ("A", "1")]);
        assert_eq!(env_digest(&dk, AT, &one), env_digest(&dk, AT, &two));
        assert_ne!(
            env_digest(&dk, AT, &one),
            env_digest(&dk, AT, &build(&[("A", "2"), ("B", "2")]))
        );
        // Moving a byte between name and value changes it.
        assert_ne!(
            env_digest(&dk, AT, &build(&[("AB", "")])),
            env_digest(&dk, AT, &build(&[("A", "B")]))
        );
        assert_ne!(
            env_digest(&dk, AT, &one),
            env_digest(&DataKey::generate().unwrap(), AT, &one)
        );
    }

    #[test]
    fn the_digest_is_bound_to_its_location() {
        let dk = DataKey::generate().unwrap();
        let env = BTreeMap::from([("A".to_string(), "1".to_string())]);
        let same = env_digest(&dk, AT, &env);
        assert_eq!(same, env_digest(&dk, AT, &env.clone()));
        for other in [
            ["blog", "staging", "web", "app"],
            ["shop", "production", "web", "app"],
            ["shop", "staging", "api", "app"],
            ["shop", "staging", "web", "db"],
            // Parts are length-prefixed, so moving a byte between two
            // parts changes the digest.
            ["shops", "taging", "web", "app"],
        ] {
            assert_ne!(same, env_digest(&dk, other, &env), "{other:?}");
        }
    }

    #[test]
    fn serde_forms_are_base64_strings() {
        let (ck, dk) = (cluster(), DataKey::generate().unwrap());
        let wrapped = wrap(&ck, &dk, "shop/staging").unwrap();
        let json = serde_json::to_value(&wrapped).unwrap();
        assert!(json.is_string());
        let back: WrappedKey = serde_json::from_value(json).unwrap();
        assert_eq!(back, wrapped);

        let sealed = seal(&dk, b"x", &[]).unwrap();
        let back: Sealed = serde_json::from_str(&serde_json::to_string(&sealed).unwrap()).unwrap();
        assert_eq!(back, sealed);

        assert!(serde_json::from_str::<Sealed>("\"!!\"").is_err());
        assert!(serde_json::from_str::<Sealed>("\"AAAA\"").is_err());
    }

    #[test]
    fn keys_encode_as_base64_and_never_print() {
        let ck = cluster();
        let text = ck.to_base64();
        let text: &str = &text;
        assert_eq!(ClusterKey::from_base64(&format!("{text}\n")).unwrap(), ck);
        assert_eq!(
            ClusterKey::from_base64("AAAA").unwrap_err(),
            CryptoError::Malformed
        );
        assert_eq!(
            ClusterKey::from_base64("%%").unwrap_err(),
            CryptoError::Malformed
        );
        let shown = format!("{ck:?} {:?}", DataKey::generate().unwrap());
        assert!(shown.contains("redacted"));
        assert!(!shown.contains(text));
    }
}
