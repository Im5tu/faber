//! `faber-node`: the root helper that performs CRI, volume and mount
//! operations on behalf of `fabercp`.
//!
//! It is the trust boundary. `fabercp` runs unprivileged and may be
//! compromised, so every request is checked here against `faber-node`'s own
//! configuration, whatever `fabercp` sends: the user namespace is always
//! on and always inside the configured uid range, containers are never
//! privileged and hold only allow-listed capabilities, volume paths
//! derive only from validated names under the volume root, and the
//! containerd and CNI config that root loads is written here, to paths named
//! in `faber-node`'s own config.

pub mod config;
pub mod connection;
pub mod cri_config;
#[cfg(target_os = "linux")]
pub mod listener;
pub mod node;
#[cfg(test)]
mod node_tests;
pub mod peer;
pub mod trust;
pub mod volume;
