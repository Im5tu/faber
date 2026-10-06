//! WireGuard mesh: the replicated mesh state, the invite token, keys and the
//! local node identity.
//!
//! `state` holds the records stored in `sm_data` and the pure functions that
//! `apply` runs for the mesh log commands. `token` is the invite token a new
//! node joins with. `keys` generates and checks WireGuard keys. `identity` is
//! this node's `identity.json` and the bootstrap choice between an existing
//! identity, an invite token and a new mesh. `endpoint` parses WireGuard
//! endpoints, `iface` records that this node created its WireGuard
//! interface, and `bootstrap` is the marker that says why a join was given
//! up.

pub mod bootstrap;
pub mod endpoint;
pub(crate) mod files;
pub mod identity;
pub mod iface;
pub mod keys;
pub mod state;
pub mod token;

pub use bootstrap::BootstrapError;
pub use endpoint::{EndpointError, parse_endpoint, parse_full_endpoint};
pub use identity::{
    BootstrapInput, IDENTITY_FILE, Identity, IdentityError, JoinPending, PendingJoin,
};
pub use keys::{KeyError, PrivateKey, generate_keypair, public_key_of, validate_public_key};
pub use state::{Allocation, InviteRecord, MeshConfig, MeshState, NodeRecord, Rejection};
pub use token::{InviteToken, TOKEN_PREFIX, TokenError, TokenPeer};
