//! Image pulls through the CRI, with a registry auth extension point.
//!
//! Image references are passed to the runtime as given; no normalisation.

use faber_cri::{AuthConfig, BoxFuture, CriClient, CriError};

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("image {image}: {source}")]
    Cri {
        image: String,
        #[source]
        source: CriError,
    },
    #[error("image {image}: {source}")]
    Auth {
        image: String,
        #[source]
        source: AuthError,
    },
    #[error("image {0}: the runtime returned no image reference")]
    EmptyReference(String),
}

/// Credentials could not be obtained for a registry.
#[derive(Debug, thiserror::Error)]
#[error("registry auth: {0}")]
pub struct AuthError(pub String);

/// Supplies registry credentials for an image reference.
pub trait RegistryAuth: Send + Sync {
    /// `None` means pull anonymously.
    fn auth_for<'a>(
        &'a self,
        image: &'a str,
    ) -> BoxFuture<'a, Result<Option<AuthConfig>, AuthError>>;
}

/// No credentials, for public registries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Anonymous;

impl RegistryAuth for Anonymous {
    fn auth_for<'a>(
        &'a self,
        _image: &'a str,
    ) -> BoxFuture<'a, Result<Option<AuthConfig>, AuthError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Makes sure images are present on the node, pulling only when absent.
pub struct ImagePuller<C, A = Anonymous> {
    cri: C,
    auth: A,
}

impl<C: CriClient> ImagePuller<C> {
    pub fn new(cri: C) -> Self {
        Self {
            cri,
            auth: Anonymous,
        }
    }
}

impl<C: CriClient, A: RegistryAuth> ImagePuller<C, A> {
    pub fn with_auth(cri: C, auth: A) -> Self {
        Self { cri, auth }
    }

    /// Returns the image id, pulling `image` first if the runtime does not
    /// have it.
    pub async fn ensure(&self, image: &str) -> Result<String, ImageError> {
        let wrap = |source| ImageError::Cri {
            image: image.to_owned(),
            source,
        };
        if let Some(present) = self.cri.image_status(image).await.map_err(wrap)? {
            return Ok(present.id);
        }
        let auth = self
            .auth
            .auth_for(image)
            .await
            .map_err(|source| ImageError::Auth {
                image: image.to_owned(),
                source,
            })?;
        let id = self.cri.pull_image(image, auth).await.map_err(wrap)?;
        if id.is_empty() {
            return Err(ImageError::EmptyReference(image.to_owned()));
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faber_cri::fake::{Call, FakeCri};

    struct Fixed;

    impl RegistryAuth for Fixed {
        fn auth_for<'a>(
            &'a self,
            image: &'a str,
        ) -> BoxFuture<'a, Result<Option<AuthConfig>, AuthError>> {
            Box::pin(async move {
                Ok(Some(AuthConfig {
                    username: "bot".to_owned(),
                    server_address: image.split('/').next().unwrap_or_default().to_owned(),
                    ..Default::default()
                }))
            })
        }
    }

    struct Broken;

    impl RegistryAuth for Broken {
        fn auth_for<'a>(
            &'a self,
            _image: &'a str,
        ) -> BoxFuture<'a, Result<Option<AuthConfig>, AuthError>> {
            Box::pin(async { Err(AuthError("vault sealed".to_owned())) })
        }
    }

    #[tokio::test]
    async fn auth_failure_skips_the_pull() {
        let cri = FakeCri::new();
        let err = ImagePuller::with_auth(cri.clone(), Broken)
            .ensure("reg.example/app:2")
            .await
            .unwrap_err();
        assert!(matches!(err, ImageError::Auth { .. }));
        assert_eq!(
            err.to_string(),
            "image reg.example/app:2: registry auth: vault sealed"
        );
        assert_eq!(
            cri.calls(),
            vec![Call::ImageStatus("reg.example/app:2".to_owned())]
        );
    }

    #[tokio::test]
    async fn pulls_when_absent() {
        let cri = FakeCri::new();
        let id = ImagePuller::new(cri.clone())
            .ensure("nginx:1")
            .await
            .unwrap();
        assert!(id.starts_with("sha256:"));
        assert_eq!(
            cri.calls(),
            vec![
                Call::ImageStatus("nginx:1".to_owned()),
                Call::PullImage {
                    image: "nginx:1".to_owned(),
                    auth: None
                },
            ]
        );
    }

    #[tokio::test]
    async fn skips_pull_when_present() {
        let cri = FakeCri::new();
        let existing = cri.add_image("nginx:1");
        let id = ImagePuller::new(cri.clone())
            .ensure("nginx:1")
            .await
            .unwrap();
        assert_eq!(id, existing);
        assert_eq!(cri.calls(), vec![Call::ImageStatus("nginx:1".to_owned())]);
    }

    #[tokio::test]
    async fn passes_auth_to_the_pull() {
        let cri = FakeCri::new();
        ImagePuller::with_auth(cri.clone(), Fixed)
            .ensure("reg.example/app:2")
            .await
            .unwrap();
        let Call::PullImage { auth, .. } = &cri.calls()[1] else {
            panic!("expected a pull");
        };
        let auth = auth.as_ref().unwrap();
        assert_eq!(auth.username, "bot");
        assert_eq!(auth.server_address, "reg.example");
    }

    #[tokio::test]
    async fn pull_errors_carry_the_image_and_cause() {
        let cri = FakeCri::new();
        cri.fail_next("pull_image", CriError::NotFound("no such tag".to_owned()));
        let err = ImagePuller::new(cri)
            .ensure("nginx:nope")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ImageError::Cri {
                source: CriError::NotFound(_),
                ..
            }
        ));
        assert_eq!(err.to_string(), "image nginx:nope: not found: no such tag");
    }

    #[tokio::test]
    async fn status_errors_stop_before_pulling() {
        let cri = FakeCri::new();
        cri.fail_next("image_status", CriError::Unavailable("down".to_owned()));
        let err = ImagePuller::new(cri.clone()).ensure("x").await.unwrap_err();
        assert!(matches!(err, ImageError::Cri { .. }));
        assert_eq!(cri.calls().len(), 1);
    }
}
