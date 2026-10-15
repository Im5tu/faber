//! Builds the CRI configs from validated input. Pure functions: every
//! security setting is fixed here and none is read from the request.

use std::collections::HashMap;
use std::path::PathBuf;

use faber_api_types::workload::{ContainerSpec, PortSpec, Protocol};
use faber_cri::v1::security_profile::ProfileType;
use faber_cri::v1::{
    Capability, ContainerMetadata, DnsConfig as CriDns, IdMapping, ImageSpec, Int64Value, KeyValue,
    LinuxContainerConfig, LinuxContainerResources, LinuxContainerSecurityContext,
    LinuxPodSandboxConfig, LinuxSandboxSecurityContext, Mount, MountPropagation, NamespaceMode,
    NamespaceOption, PodSandboxMetadata, Protocol as CriProtocol, SecurityProfile, UserNamespace,
};
use faber_cri::{ContainerConfig, PodSandboxConfig};
use faber_node_protocol::{DnsConfig, LABEL_CONTAINER, ReplicaRef, SecretEnv, USERNS_LENGTH};

/// Annotation on the sandbox recording the base of its user namespace.
pub const ANNOTATION_UID_BASE: &str = "faber.uid_base";

/// Annotation on a container: a digest of everything it was created from,
/// env values included, so a retried create reuses it only when unchanged.
pub const ANNOTATION_SPEC_HASH: &str = "faber.spec_hash";

const CPU_PERIOD_MICROS: i64 = 100_000;

/// Container 0 maps to host `uid_base`, for exactly [`USERNS_LENGTH`] ids,
/// for users and groups alike.
fn id_mappings(uid_base: u32) -> Vec<IdMapping> {
    vec![IdMapping {
        host_id: uid_base,
        container_id: 0,
        length: USERNS_LENGTH,
    }]
}

fn userns(uid_base: u32) -> UserNamespace {
    UserNamespace {
        mode: NamespaceMode::Pod as i32,
        uids: id_mappings(uid_base),
        gids: id_mappings(uid_base),
    }
}

/// Never `Node`: the pod has its own network, ipc, pid and user namespaces.
fn namespace_options(uid_base: u32) -> NamespaceOption {
    NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Container as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(userns(uid_base)),
    }
}

fn runtime_default_seccomp() -> SecurityProfile {
    SecurityProfile {
        profile_type: ProfileType::RuntimeDefault as i32,
        localhost_ref: String::new(),
    }
}

fn labels(replica: &ReplicaRef) -> HashMap<String, String> {
    replica.labels().into_iter().collect()
}

pub fn sandbox_config(
    replica: &ReplicaRef,
    uid_base: u32,
    dns: &DnsConfig,
    ports: &[PortSpec],
) -> PodSandboxConfig {
    let index = replica.replica_index;
    let mut hostname = format!("{}-{index}", replica.workload);
    hostname.truncate(63);
    PodSandboxConfig {
        metadata: Some(PodSandboxMetadata {
            name: format!("{}-{}-{index}", replica.workload, replica.revision),
            // Dots cannot occur in a DNS label, so this is unambiguous.
            uid: format!(
                "{}.{}.{}.{}.{index}",
                replica.project, replica.env, replica.workload, replica.revision
            ),
            namespace: format!("{}.{}", replica.project, replica.env),
            attempt: 0,
        }),
        hostname,
        dns_config: Some(CriDns {
            servers: dns.nameservers.iter().map(ToString::to_string).collect(),
            searches: dns.searches.clone(),
            options: Vec::new(),
        }),
        port_mappings: ports
            .iter()
            .map(|p| faber_cri::v1::PortMapping {
                protocol: match p.protocol {
                    Protocol::Tcp => CriProtocol::Tcp,
                    Protocol::Udp => CriProtocol::Udp,
                } as i32,
                container_port: i32::from(p.port),
                // 0: nothing is published on the host.
                host_port: 0,
                host_ip: String::new(),
            })
            .collect(),
        labels: labels(replica),
        annotations: HashMap::from([(ANNOTATION_UID_BASE.to_owned(), uid_base.to_string())]),
        linux: Some(LinuxPodSandboxConfig {
            security_context: Some(LinuxSandboxSecurityContext {
                namespace_options: Some(namespace_options(uid_base)),
                privileged: false,
                readonly_rootfs: false,
                seccomp: Some(runtime_default_seccomp()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The sandbox config passed along with a container create. It is rebuilt
/// from the replica rather than echoed back from the runtime, so it carries
/// the pod's hostname, user namespace and security context, and keeps the
/// runtime's own `metadata` so the pod is found by its identity. The DNS
/// settings stay empty: they were applied when the sandbox ran.
pub fn container_sandbox_config(
    replica: &ReplicaRef,
    uid_base: u32,
    metadata: Option<PodSandboxMetadata>,
) -> PodSandboxConfig {
    let no_dns = DnsConfig {
        nameservers: Vec::new(),
        searches: Vec::new(),
    };
    PodSandboxConfig {
        metadata,
        dns_config: None,
        ..sandbox_config(replica, uid_base, &no_dns, &[])
    }
}

/// One mount of a volume into the container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMount {
    pub container_path: String,
    pub host_path: PathBuf,
    pub read_only: bool,
}
/// `(uid, gid, user name)` of `uid`, `uid:gid` or a user name. Anything
/// else is an error: a user that cannot be read must not become root.
fn run_as(user: Option<&str>) -> Result<(Option<i64>, Option<i64>, String), String> {
    let Some(user) = user else {
        return Ok((None, None, String::new()));
    };
    let bad = || format!("user {user:?} is not a uid, uid:gid or a user name");
    match user.split_once(':') {
        Some((uid, gid)) => Ok((
            Some(uid.parse().map_err(|_| bad())?),
            Some(gid.parse().map_err(|_| bad())?),
            String::new(),
        )),
        None if user.bytes().all(|b| b.is_ascii_digit()) && !user.is_empty() => {
            Ok((Some(user.parse().map_err(|_| bad())?), None, String::new()))
        }
        None if user.is_empty() => Err(bad()),
        None => Ok((None, None, user.to_owned())),
    }
}

fn limit(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub fn container_config(
    replica: &ReplicaRef,
    container: &ContainerSpec,
    env: &SecretEnv,
    uid_base: u32,
    attempt: u32,
    mounts: &[ResolvedMount],
) -> Result<ContainerConfig, String> {
    let (uid, gid, username) = run_as(container.user.as_deref())?;
    let mut labels = labels(replica);
    labels.insert(LABEL_CONTAINER.to_owned(), container.name.clone());
    let cpu_millis = limit(container.resources.cpu_millis);
    let memory = limit(container.resources.memory_bytes);
    Ok(ContainerConfig {
        metadata: Some(ContainerMetadata {
            name: container_name(replica, &container.name),
            attempt,
        }),
        image: Some(ImageSpec {
            image: container.image.clone(),
            ..Default::default()
        }),
        command: container.command.clone(),
        args: container.args.clone(),
        working_dir: container.working_dir.clone().unwrap_or_default(),
        envs: env
            .0
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        mounts: mounts
            .iter()
            .map(|m| Mount {
                container_path: m.container_path.clone(),
                host_path: m.host_path.to_string_lossy().into_owned(),
                readonly: m.read_only,
                selinux_relabel: false,
                propagation: MountPropagation::PropagationPrivate as i32,
                // See the module docs of `volume`: the on-disk ids are the
                // host ids the pod maps, so no idmapped mount is requested.
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            })
            .collect(),
        labels,
        linux: Some(LinuxContainerConfig {
            resources: Some(LinuxContainerResources {
                cpu_period: CPU_PERIOD_MICROS,
                cpu_quota: cpu_millis.saturating_mul(CPU_PERIOD_MICROS / 1000),
                cpu_shares: (cpu_millis.saturating_mul(1024) / 1000).max(2),
                memory_limit_in_bytes: memory,
                // As much swap as memory: no swap.
                memory_swap_limit_in_bytes: memory,
                ..Default::default()
            }),
            security_context: Some(LinuxContainerSecurityContext {
                capabilities: Some(Capability {
                    add_capabilities: container.capabilities.clone(),
                    drop_capabilities: vec!["ALL".to_owned()],
                    add_ambient_capabilities: Vec::new(),
                }),
                privileged: false,
                namespace_options: Some(namespace_options(uid_base)),
                run_as_user: uid.map(|value| Int64Value { value }),
                run_as_group: gid.map(|value| Int64Value { value }),
                run_as_username: username,
                readonly_rootfs: container.read_only_root_fs,
                no_new_privs: true,
                seccomp: Some(runtime_default_seccomp()),
                ..Default::default()
            }),
        }),
        ..Default::default()
    })
}

/// A digest of everything a container is created from: its spec, its mounts,
/// the owner and every env name and value. Env values go in so a changed
/// secret counts as a change; only the digest is stored.
pub fn spec_hash(
    container: &ContainerSpec,
    env: &SecretEnv,
    uid_base: u32,
    mounts: &[ResolvedMount],
) -> String {
    use sha2::{Digest, Sha256};

    let mut spec = container.clone();
    // The request's env replaces the spec's, which is not used.
    spec.env.clear();
    let mut hasher = Sha256::new();
    let mut part = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    part(&serde_json::to_vec(&spec).unwrap_or_default());
    part(&uid_base.to_be_bytes());
    for m in mounts {
        part(m.container_path.as_bytes());
        part(m.host_path.to_string_lossy().as_bytes());
        part(&[u8::from(m.read_only)]);
    }
    for (key, value) in &env.0 {
        part(key.as_bytes());
        part(value.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// `<workload>-<revision>-<container>`: unique within the pod.
pub fn container_name(replica: &ReplicaRef, container: &str) -> String {
    format!("{}-{}-{container}", replica.workload, replica.revision)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use faber_api_types::workload::Resources;

    use super::*;

    /// The tests use valid users except where they say otherwise.
    fn container_config(
        replica: &ReplicaRef,
        container: &ContainerSpec,
        env: &SecretEnv,
        uid_base: u32,
        attempt: u32,
        mounts: &[ResolvedMount],
    ) -> ContainerConfig {
        super::container_config(replica, container, env, uid_base, attempt, mounts).unwrap()
    }

    #[test]
    fn an_unreadable_user_is_an_error_not_root() {
        for bad in [
            "1000:",
            ":1000",
            "a:b",
            "1000:x",
            "",
            "99999999999999999999",
            ":",
        ] {
            let mut spec = ContainerSpec::new("c", "i");
            spec.user = Some(bad.into());
            let r =
                super::container_config(&replica(), &spec, &SecretEnv::default(), 65536, 0, &[]);
            assert!(r.is_err(), "{bad:?}");
        }
    }

    fn replica() -> ReplicaRef {
        ReplicaRef {
            project: "shop".into(),
            env: "prod".into(),
            workload: "redis".into(),
            revision: "a1b2c3d4".into(),
            replica_index: 3,
        }
    }

    fn dns() -> DnsConfig {
        DnsConfig {
            nameservers: vec![Ipv4Addr::new(10, 200, 0, 1)],
            searches: vec!["prod.shop.faber.local".into()],
        }
    }

    fn assert_userns(opts: &NamespaceOption, base: u32) {
        let userns = opts.userns_options.as_ref().expect("userns_options set");
        assert_eq!(userns.mode, NamespaceMode::Pod as i32);
        let want = vec![IdMapping {
            host_id: base,
            container_id: 0,
            length: 65536,
        }];
        assert_eq!(userns.uids, want);
        assert_eq!(userns.gids, want);
        for mode in [opts.network, opts.pid, opts.ipc] {
            assert_ne!(mode, NamespaceMode::Node as i32);
            assert_ne!(mode, NamespaceMode::Target as i32);
        }
    }

    #[test]
    fn sandbox_has_userns_seccomp_labels_dns_and_is_not_privileged() {
        let ports = [PortSpec {
            name: None,
            port: 6379,
            protocol: Protocol::Tcp,
        }];
        let cfg = sandbox_config(&replica(), 131072, &dns(), &ports);
        let sc = cfg.linux.unwrap().security_context.unwrap();
        assert!(!sc.privileged);
        assert_userns(sc.namespace_options.as_ref().unwrap(), 131072);
        assert_eq!(
            sc.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
        let want: HashMap<String, String> = [
            ("faber.managed", "true"),
            ("faber.project", "shop"),
            ("faber.env", "prod"),
            ("faber.workload", "redis"),
            ("faber.revision", "a1b2c3d4"),
            ("faber.replica", "3"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        assert_eq!(cfg.labels, want);
        assert_eq!(cfg.annotations[ANNOTATION_UID_BASE], "131072");
        let d = cfg.dns_config.unwrap();
        assert_eq!(d.servers, vec!["10.200.0.1"]);
        assert_eq!(d.searches, vec!["prod.shop.faber.local"]);
        assert_eq!(cfg.port_mappings[0].container_port, 6379);
        assert_eq!(cfg.port_mappings[0].host_port, 0);
        assert_eq!(cfg.metadata.unwrap().name, "redis-a1b2c3d4-3");
    }

    #[test]
    fn the_sandbox_config_sent_with_a_container_keeps_hostname_userns_and_identity() {
        let identity = PodSandboxMetadata {
            name: "from-the-runtime".into(),
            ..Default::default()
        };
        let cfg = container_sandbox_config(&replica(), 131072, Some(identity.clone()));
        assert_eq!(cfg.hostname, "redis-3");
        assert_eq!(cfg.metadata, Some(identity));
        assert!(cfg.dns_config.is_none());
        assert_eq!(cfg.labels["faber.replica"], "3");
        let sc = cfg.linux.unwrap().security_context.unwrap();
        assert!(!sc.privileged);
        assert_userns(sc.namespace_options.as_ref().unwrap(), 131072);
    }

    #[test]
    fn container_drops_all_caps_then_adds_the_listed_ones() {
        let mut spec = ContainerSpec::new("redis", "redis:7");
        spec.capabilities = vec!["CHOWN".into(), "SETUID".into()];
        let cfg = container_config(&replica(), &spec, &SecretEnv::default(), 131072, 0, &[]);
        let sc = cfg.linux.unwrap().security_context.unwrap();
        let caps = sc.capabilities.unwrap();
        assert_eq!(caps.drop_capabilities, vec!["ALL"]);
        assert_eq!(caps.add_capabilities, vec!["CHOWN", "SETUID"]);
        assert!(caps.add_ambient_capabilities.is_empty());
        // No capabilities listed still drops everything.
        let none = container_config(
            &replica(),
            &ContainerSpec::new("redis", "redis:7"),
            &SecretEnv::default(),
            131072,
            0,
            &[],
        );
        let caps = none
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .capabilities
            .unwrap();
        assert_eq!(caps.drop_capabilities, vec!["ALL"]);
        assert!(caps.add_capabilities.is_empty());
    }

    #[test]
    fn container_security_context_is_locked_down() {
        let mut spec = ContainerSpec::new("redis", "redis:7");
        spec.read_only_root_fs = true;
        let cfg = container_config(&replica(), &spec, &SecretEnv::default(), 196608, 0, &[]);
        let sc = cfg.linux.unwrap().security_context.unwrap();
        assert!(!sc.privileged);
        assert!(sc.no_new_privs);
        assert!(sc.readonly_rootfs);
        assert_eq!(
            sc.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
        assert_userns(sc.namespace_options.as_ref().unwrap(), 196608);

        let writable = container_config(
            &replica(),
            &ContainerSpec::new("redis", "redis:7"),
            &SecretEnv::default(),
            196608,
            0,
            &[],
        );
        assert!(
            !writable
                .linux
                .unwrap()
                .security_context
                .unwrap()
                .readonly_rootfs
        );
    }

    #[test]
    fn limits_come_from_resources() {
        let mut spec = ContainerSpec::new("redis", "redis:7");
        spec.resources = Resources {
            cpu_millis: 1500,
            memory_bytes: 512 << 20,
        };
        let cfg = container_config(&replica(), &spec, &SecretEnv::default(), 65536, 0, &[]);
        let r = cfg.linux.unwrap().resources.unwrap();
        assert_eq!((r.cpu_period, r.cpu_quota), (100_000, 150_000));
        assert_eq!(r.memory_limit_in_bytes, 512 << 20);
        assert_eq!(r.memory_swap_limit_in_bytes, 512 << 20);
        assert_eq!(r.cpu_shares, 1536);
    }

    #[test]
    fn naming_labels_attempt_env_and_command() {
        let mut spec = ContainerSpec::new("cache", "redis:7");
        spec.command = vec!["redis-server".into()];
        spec.args = vec!["--save".into()];
        spec.working_dir = Some("/data".into());
        let env = SecretEnv([("A".to_owned(), "1".to_owned())].into());
        let cfg = container_config(&replica(), &spec, &env, 65536, 4, &[]);
        let m = cfg.metadata.unwrap();
        assert_eq!((m.name.as_str(), m.attempt), ("redis-a1b2c3d4-cache", 4));
        assert_eq!(cfg.labels["faber.container"], "cache");
        assert_eq!(cfg.labels["faber.replica"], "3");
        assert_eq!(cfg.envs.len(), 1);
        assert_eq!(
            (cfg.envs[0].key.as_str(), cfg.envs[0].value.as_str()),
            ("A", "1")
        );
        assert_eq!(cfg.command, vec!["redis-server"]);
        assert_eq!(cfg.args, vec!["--save"]);
        assert_eq!(cfg.working_dir, "/data");
        assert_eq!(cfg.image.unwrap().image, "redis:7");
    }

    #[test]
    fn users_map_to_run_as_fields() {
        let cases = [
            (None, None, None, ""),
            (Some("1000"), Some(1000), None, ""),
            (Some("1000:2000"), Some(1000), Some(2000), ""),
            (Some("redis"), None, None, "redis"),
        ];
        for (user, uid, gid, name) in cases {
            let mut spec = ContainerSpec::new("c", "i");
            spec.user = user.map(str::to_owned);
            let sc = container_config(&replica(), &spec, &SecretEnv::default(), 65536, 0, &[])
                .linux
                .unwrap()
                .security_context
                .unwrap();
            assert_eq!(sc.run_as_user.map(|v| v.value), uid, "{user:?}");
            assert_eq!(sc.run_as_group.map(|v| v.value), gid, "{user:?}");
            assert_eq!(sc.run_as_username, name, "{user:?}");
        }
    }

    #[test]
    fn mounts_carry_paths_and_read_only_and_no_idmap() {
        let mounts = [ResolvedMount {
            container_path: "/data".into(),
            host_path: PathBuf::from("/var/lib/faber-node/volumes/shop/prod/redis/3/data/mnt"),
            read_only: true,
        }];
        let spec = ContainerSpec::new("redis", "redis:7");
        let cfg = container_config(&replica(), &spec, &SecretEnv::default(), 65536, 0, &mounts);
        let m = &cfg.mounts[0];
        assert_eq!(m.container_path, "/data");
        assert_eq!(
            m.host_path,
            "/var/lib/faber-node/volumes/shop/prod/redis/3/data/mnt"
        );
        assert!(m.readonly);
        assert_eq!(m.propagation, MountPropagation::PropagationPrivate as i32);
        assert!(m.uid_mappings.is_empty() && m.gid_mappings.is_empty());
    }
}
