//! The calls that start, restart and remove replicas through `faber-node`.
//! The containers step and the supervisor both use them.
//!
//! Each of [`launch`], [`restart_container`] and [`retire`] holds the
//! replica's lock ([`RunnerState::replica_lock`]) for its whole run and looks
//! at the replica's entry once it has the lock, so a start never runs next
//! to a restart or a removal of the same replica, and a replica that was
//! asked to stop is not brought back by a restart that was already waiting.

use std::collections::BTreeSet;

use faber_node_protocol::{
    ContainerInfo, ContainerState, ReplicaInfo, ReplicaRef, Request, VolumeKind,
};

use super::desired::{DesiredContainer, DesiredReplica, revision_string};
use super::env::EnvSource;
use super::node::{
    NodeCallError, NodeClient, call_container, call_done, call_sandbox, list_volumes, pull_image,
};
use super::state::{RunnerState, slot_of, workload_of};

/// The container of the pod that was made from the spec container `name`.
pub fn find_container<'a>(info: &'a ReplicaInfo, name: &str) -> Option<&'a ContainerInfo> {
    info.containers.iter().find(|c| {
        c.labels
            .get(faber_node_protocol::LABEL_CONTAINER)
            .map(String::as_str)
            == Some(name)
    })
}

async fn ensure_sandbox<C: NodeClient>(
    client: &C,
    d: &DesiredReplica,
) -> Result<String, NodeCallError> {
    call_sandbox(
        client,
        Request::EnsureSandbox {
            replica: d.replica.clone(),
            uid_base: d.uid_base,
            dns: d.dns.clone(),
            ports: d.ports(),
        },
    )
    .await
}

/// Creates and starts `container` in the sandbox. The id of the container.
/// The env is fetched here and goes out in the request; nothing keeps it.
async fn create_and_start<C: NodeClient>(
    client: &C,
    env: &dyn EnvSource,
    d: &DesiredReplica,
    container: &DesiredContainer,
    sandbox_id: &str,
) -> Result<String, NodeCallError> {
    let env = env
        .env(&d.replica, &container.spec.name)
        .map_err(NodeCallError::Env)?;
    let id = call_container(
        client,
        Request::CreateContainer {
            replica: d.replica.clone(),
            sandbox_id: sandbox_id.to_owned(),
            container: container.spec.clone(),
            env,
            uid_base: d.uid_base,
        },
    )
    .await?;
    call_done(
        client,
        Request::StartContainer {
            container_id: id.clone(),
        },
        "start_container",
    )
    .await?;
    Ok(id)
}

/// Brings the replica up for the first time: its volumes, its images, its
/// sandbox, and every container that is not there yet. `existing` is what
/// `ListReplicas` showed of the replica.
///
/// Called only for a replica that has not been launched. A container that
/// later exits or vanishes is the supervisor's: whether it runs again, and
/// when, is decided by the restart policy and the backoff, not here.
///
/// Does nothing when the replica is no longer wanted.
pub async fn launch<C: NodeClient>(
    client: &C,
    state: &RunnerState,
    env: &dyn EnvSource,
    d: &DesiredReplica,
    existing: Option<&ReplicaInfo>,
) -> Result<(), NodeCallError> {
    let lock = state.replica_lock(&d.replica);
    let _held = lock.lock().await;
    if !state
        .entry(&d.replica)
        .is_some_and(|e| e.stop.is_none() && !e.launched)
    {
        return Ok(());
    }
    {
        // The collector of ramdisks must not remove one between this entry
        // being checked and the volume being made.
        let slot_lock = state.slot_lock(&slot_of(&d.replica));
        let _slot = slot_lock.lock().await;
        for volume in &d.volumes {
            call_done(
                client,
                Request::EnsureVolume {
                    replica: d.replica.clone(),
                    volume_name: volume.name.clone(),
                    kind: volume.kind,
                    size_bytes: volume.size_bytes,
                    uid_base: d.uid_base,
                },
                "ensure_volume",
            )
            .await?;
        }
    }
    let images: BTreeSet<&str> = d.containers.iter().map(|c| c.spec.image.as_str()).collect();
    for image in images {
        pull_image(client, image).await?;
    }
    let sandbox_id = ensure_sandbox(client, d).await?;
    let same_pod = existing.filter(|e| e.sandbox_id == sandbox_id);
    for container in &d.containers {
        match same_pod.and_then(|pod| find_container(pod, &container.spec.name)) {
            Some(c) if c.state == ContainerState::Created => {
                call_done(
                    client,
                    Request::StartContainer {
                        container_id: c.id.clone(),
                    },
                    "start_container",
                )
                .await?;
            }
            // Running, or exited and the supervisor's to restart.
            Some(_) => {}
            None => {
                create_and_start(client, env, d, container, &sandbox_id).await?;
            }
        }
    }
    state.mark_launched(&d.replica);
    Ok(())
}

/// Starts the container `container` of the replica again: stops and removes
/// what is left of the old one and makes a new one. A pod whose sandbox is
/// gone gets a new sandbox first.
///
/// `Ok(false)` when nothing was done: the replica is no longer there to
/// restart (it was removed, or a stop was asked for), or its lock is held
/// by a start or a removal that may take long (a pull). The caller asks
/// again on its next tick.
pub async fn restart_container<C: NodeClient>(
    client: &C,
    state: &RunnerState,
    env: &dyn EnvSource,
    d: &DesiredReplica,
    container: &DesiredContainer,
    existing: Option<&ReplicaInfo>,
) -> Result<bool, NodeCallError> {
    let lock = state.replica_lock(&d.replica);
    let Ok(_held) = lock.try_lock() else {
        return Ok(false);
    };
    if !state
        .entry(&d.replica)
        .is_some_and(|e| e.stop.is_none() && e.launched)
    {
        return Ok(false);
    }
    let sandbox_id = ensure_sandbox(client, d).await?;
    let old = existing
        .filter(|e| e.sandbox_id == sandbox_id)
        .and_then(|pod| find_container(pod, &container.spec.name));
    if let Some(old) = old {
        if old.state == ContainerState::Running {
            stop_container(client, &old.id, d.stop_grace_secs).await?;
        }
        call_done(
            client,
            Request::RemoveContainer {
                container_id: old.id.clone(),
            },
            "remove_container",
        )
        .await?;
    }
    create_and_start(client, env, d, container, &sandbox_id).await?;
    Ok(true)
}

pub async fn stop_container<C: NodeClient>(
    client: &C,
    container_id: &str,
    grace_secs: u32,
) -> Result<(), NodeCallError> {
    call_done(
        client,
        Request::StopContainer {
            container_id: container_id.to_owned(),
            grace_secs,
        },
        "stop_container",
    )
    .await
}

/// Signals the stop of a pod: every container that runs, then the sandbox.
/// The replica is forgotten once its sandbox is gone; if that fails it
/// stays, with its stop request, to be tried again.
///
/// The caller has waited the drain period; the replica has read
/// `unavailable` since the stop was asked for.
pub async fn retire<C: NodeClient>(
    client: &C,
    state: &RunnerState,
    replica: &ReplicaRef,
    pod: Option<&ReplicaInfo>,
    grace_secs: u32,
) -> Result<(), NodeCallError> {
    let lock = state.replica_lock(replica);
    let _held = lock.lock().await;
    // The stop may have been withdrawn while this waited for the lock.
    if state.entry(replica).is_none_or(|e| e.stop.is_none()) {
        return Ok(());
    }
    if let Some(pod) = pod {
        for c in &pod.containers {
            if matches!(c.state, ContainerState::Running | ContainerState::Created) {
                // Removing the sandbox stops what is left.
                if let Err(e) = stop_container(client, &c.id, grace_secs).await {
                    tracing::warn!(container = %c.name, error = %e, "could not stop a container");
                }
            }
        }
        call_done(
            client,
            Request::RemoveSandbox {
                sandbox_id: pod.sandbox_id.clone(),
            },
            "remove_sandbox",
        )
        .await?;
    }
    state.forget(replica);
    Ok(())
}

/// Removes the ramdisks of replicas that are not on this node any more. A
/// ramdisk is host tmpfs until `RemoveVolume`, so one that is left behind
/// keeps its memory. They are found by asking faber-node what is on disk,
/// so those a crashed or restarted fabercp left are found too.
///
/// Kept: block volumes, which live as long as the workload; any volume of a
/// slot (replica index) that still has a replica of any revision, because
/// revisions share volumes; and any volume of a held workload.
///
/// Tries all of them; the first failure is returned, and the volume is
/// found again by the next call.
pub async fn remove_unused_ramdisks<C: NodeClient>(
    client: &C,
    state: &RunnerState,
) -> Result<(), NodeCallError> {
    let mut first = None;
    for v in list_volumes(client).await? {
        if v.kind != VolumeKind::Ramdisk {
            continue;
        }
        let replica = ReplicaRef {
            project: v.project,
            env: v.env,
            workload: v.workload,
            // Volumes are not scoped by revision; the name only has to be valid.
            revision: revision_string(0),
            replica_index: v.replica_index,
        };
        let slot = slot_of(&replica);
        // Under the slot's lock the check and the removal are one step: a
        // start of the slot waits, or has registered before the check.
        let slot_lock = state.slot_lock(&slot);
        let _slot = slot_lock.lock().await;
        if state.used_slots().contains(&slot) || state.held().contains(&workload_of(&replica)) {
            continue;
        }
        let result = call_done(
            client,
            Request::RemoveVolume {
                replica,
                volume_name: v.volume,
            },
            "remove_volume",
        )
        .await;
        if let Err(e) = result {
            first = first.or(Some(e));
        }
    }
    first.map_or(Ok(()), Err)
}
