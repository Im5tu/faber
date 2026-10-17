use std::fmt;

use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::v1::AuthConfig;

/// Registry credentials for an image pull. `Debug` redacts every secret, so
/// the value can sit in logs and call records. Converted to the generated
/// [`AuthConfig`] only when the gRPC request is built. Every field is zeroed
/// on drop.
#[derive(Clone, Default, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct RegistryCredentials {
    pub server_address: String,
    pub username: String,
    pub password: String,
    /// Base64 `username:password`, as in Docker config files.
    pub auth: String,
    pub identity_token: String,
    pub registry_token: String,
}

impl fmt::Debug for RegistryCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryCredentials")
            .field("server_address", &self.server_address)
            .finish_non_exhaustive()
    }
}

/// The generated [`AuthConfig`] is plain prost and is not zeroized, nor is the request that carries it.
impl From<RegistryCredentials> for AuthConfig {
    fn from(mut c: RegistryCredentials) -> Self {
        // Taken, not moved: the type zeroizes on drop, so the emptied fields
        // are what gets wiped.
        Self {
            username: std::mem::take(&mut c.username),
            password: std::mem::take(&mut c.password),
            auth: std::mem::take(&mut c.auth),
            server_address: std::mem::take(&mut c.server_address),
            identity_token: std::mem::take(&mut c.identity_token),
            registry_token: std::mem::take(&mut c.registry_token),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_zeroized() {
        fn zeroizes_on_drop<T: ZeroizeOnDrop>() {}
        zeroizes_on_drop::<RegistryCredentials>();
        let mut creds = RegistryCredentials {
            server_address: String::new(),
            username: String::new(),
            password: "pw-secret".to_owned(),
            auth: String::new(),
            identity_token: String::new(),
            registry_token: "reg-secret".to_owned(),
        };
        creds.zeroize();
        assert!(creds.password.is_empty() && creds.registry_token.is_empty());
    }

    #[test]
    fn debug_shows_no_credentials() {
        let creds = RegistryCredentials {
            server_address: "reg.example".to_owned(),
            username: "user-secret".to_owned(),
            password: "pw-secret".to_owned(),
            auth: "auth-secret".to_owned(),
            identity_token: "id-secret".to_owned(),
            registry_token: "reg-secret".to_owned(),
        };
        for text in [format!("{creds:?}"), format!("{creds:#?}")] {
            assert!(text.contains("reg.example"));
            assert!(!text.contains("secret"), "{text}");
        }
        let wire = AuthConfig::from(creds);
        assert_eq!(wire.password, "pw-secret");
        assert_eq!(wire.registry_token, "reg-secret");
    }
}
