use std::fmt;

use crate::v1::AuthConfig;

/// Registry credentials for an image pull. `Debug` redacts every secret, so
/// the value can sit in logs and call records. Converted to the generated
/// [`AuthConfig`] only when the gRPC request is built.
#[derive(Clone, Default, PartialEq, Eq)]
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

impl From<RegistryCredentials> for AuthConfig {
    fn from(c: RegistryCredentials) -> Self {
        Self {
            username: c.username,
            password: c.password,
            auth: c.auth,
            server_address: c.server_address,
            identity_token: c.identity_token,
            registry_token: c.registry_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
