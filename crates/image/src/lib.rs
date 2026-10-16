//! Image pulls through the CRI, with a registry auth extension point.
//!
//! Image references are passed to the runtime as given; no normalisation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use faber_cri::{BoxFuture, CriClient, CriError, RegistryCredentials};

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
    ) -> BoxFuture<'a, Result<Option<RegistryCredentials>, AuthError>>;
}

/// No credentials, for public registries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Anonymous;

impl RegistryAuth for Anonymous {
    fn auth_for<'a>(
        &'a self,
        _image: &'a str,
    ) -> BoxFuture<'a, Result<Option<RegistryCredentials>, AuthError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Makes sure images are present on the node, pulling only when absent.
pub struct ImagePuller<C, A = Anonymous> {
    cri: C,
    auth: A,
    /// One lock per image reference being ensured, so concurrent callers for
    /// the same reference share a single pull.
    in_flight: InFlightMap,
}

type InFlightMap = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

/// A caller's hold on the lock-map entry for one image. Dropping it, also
/// when the `ensure` future is cancelled, removes the entry once nobody else
/// holds or waits on it.
struct InFlight<'a> {
    map: &'a InFlightMap,
    image: &'a str,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl<'a> InFlight<'a> {
    fn enter(map: &'a InFlightMap, image: &'a str) -> Self {
        let lock = {
            let mut entries = map.lock().unwrap_or_else(|e| e.into_inner());
            Arc::clone(entries.entry(image.to_owned()).or_default())
        };
        Self { map, image, lock }
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        // Callers clone the Arc under the map lock, so a count of 2 (map and
        // ours) means no one else holds or waits on the entry.
        let mut entries = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::strong_count(&self.lock) == 2 {
            entries.remove(self.image);
        }
    }
}

impl<C: CriClient> ImagePuller<C> {
    pub fn new(cri: C) -> Self {
        Self::with_auth(cri, Anonymous)
    }
}

impl<C: CriClient, A: RegistryAuth> ImagePuller<C, A> {
    pub fn with_auth(cri: C, auth: A) -> Self {
        Self {
            cri,
            auth,
            in_flight: Mutex::default(),
        }
    }

    /// Returns the image id, pulling `image` first if the runtime does not
    /// have it.
    ///
    /// Concurrent calls for the same reference are serialised, so the image
    /// is pulled once and the others find it present.
    pub async fn ensure(&self, image: &str) -> Result<String, ImageError> {
        // Declared before the guard so the guard is released first.
        let entry = InFlight::enter(&self.in_flight, image);
        let _guard = entry.lock.lock().await;
        self.ensure_locked(image).await
    }

    async fn ensure_locked(&self, image: &str) -> Result<String, ImageError> {
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
        ) -> BoxFuture<'a, Result<Option<RegistryCredentials>, AuthError>> {
            Box::pin(async move {
                Ok(Some(RegistryCredentials {
                    username: "bot".to_owned(),
                    server_address: image.split('/').next().unwrap_or_default().to_owned(),
                    password: String::new(),
                    auth: String::new(),
                    identity_token: String::new(),
                    registry_token: String::new(),
                }))
            })
        }
    }

    struct Broken;

    impl RegistryAuth for Broken {
        fn auth_for<'a>(
            &'a self,
            _image: &'a str,
        ) -> BoxFuture<'a, Result<Option<RegistryCredentials>, AuthError>> {
            Box::pin(async { Err(AuthError("vault sealed".to_owned())) })
        }
    }

    /// Never answers on the first call, so a pull stalls; answers after.
    struct StallsOnce(std::sync::atomic::AtomicBool);

    impl RegistryAuth for StallsOnce {
        fn auth_for<'a>(
            &'a self,
            _image: &'a str,
        ) -> BoxFuture<'a, Result<Option<RegistryCredentials>, AuthError>> {
            let first = !self.0.swap(true, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                if first {
                    std::future::pending::<()>().await;
                }
                Ok(None)
            })
        }
    }

    #[tokio::test]
    async fn cancelled_ensure_leaves_no_entry_and_a_later_pull_works() {
        let cri = FakeCri::new();
        let puller = ImagePuller::with_auth(cri.clone(), StallsOnce(Default::default()));
        tokio::select! {
            biased;
            _ = puller.ensure("nginx:1") => panic!("the stalled pull finished"),
            _ = std::future::ready(()) => {}
        }
        assert!(puller.in_flight.lock().unwrap().is_empty());
        let id = puller.ensure("nginx:1").await.unwrap();
        assert!(id.starts_with("sha256:"));
        assert!(puller.in_flight.lock().unwrap().is_empty());
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
    async fn concurrent_ensure_pulls_once() {
        let cri = FakeCri::new();
        let puller = ImagePuller::new(cri.clone());
        let (a, b, c) = tokio::join!(
            puller.ensure("nginx:1"),
            puller.ensure("nginx:1"),
            puller.ensure("nginx:1")
        );
        let (a, b, c) = (a.unwrap(), b.unwrap(), c.unwrap());
        assert_eq!((&a, &b), (&b, &c));
        let pulls = cri
            .calls()
            .iter()
            .filter(|c| matches!(c, Call::PullImage { .. }))
            .count();
        assert_eq!(pulls, 1);
        assert!(puller.in_flight.lock().unwrap().is_empty());
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
