//! Typed request and response frames between `fabercp` and `faber-node`.
//!
//! A frame is a `u32` big-endian length followed by that many bytes of JSON.
//! One request frame is answered by one response frame; a connection may
//! carry any number of such exchanges in sequence.
//!
//! `faber-node` is the trust boundary: nothing in a request is taken on
//! trust, and the types here only fix the shape of what may be asked.

mod frame;
mod message;
mod replica;

pub use frame::{FrameError, MAX_FRAME_BYTES, encode_frame, read_frame, write_frame};
pub use message::{
    ContainerInfo, ContainerState, DnsConfig, ErrorCode, NodeError, PodNetwork, ReplicaInfo,
    Request, Response, SandboxState, SecretEnv, VolumeInfo, VolumeKind,
};
pub use replica::{
    LABEL_CONTAINER, LABEL_ENV, LABEL_MANAGED, LABEL_PROJECT, LABEL_REPLICA, LABEL_REVISION,
    LABEL_WORKLOAD, ReplicaRef,
};

/// Bumped on any incompatible change to the frames.
pub const PROTOCOL_VERSION: u32 = 1;

/// Width of the user namespace of every pod: exactly this many ids.
pub const USERNS_LENGTH: u32 = 65536;
