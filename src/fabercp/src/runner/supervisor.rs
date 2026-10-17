//! The supervisor keeps the replicas of this node running and says how they
//! are.
//!
//! It runs on its own ticker, independent of the reconcile loop's resync.
//! Each tick it asks `faber-node` for the replicas ([`list_replicas`]),
//! compares them with what the containers step registered in
//! [`RunnerState`], and
//!
//! - restarts containers that exited, as the restart policy says, after a
//!   backoff ([`Backoff`]);
//! - probes each running container at its own interval, the first probe one
//!   interval after the container came up. One failure makes the replica
//!   `unavailable`; [`PROBE_FAILURES_BEFORE_RESTART`] in a row stop the
//!   container, which then restarts like one that exited with a failure;
//! - asks for the stop of the replicas of an older revision once the newer
//!   revision is available on this node;
//! - signals every stop (that one, a replica placement took away, a
//!   replica a newer block-volume revision replaces) in the same order: the
//!   replica first reads `unavailable`, so that the virtual IP drops it,
//!   then the drain period ([`DRAIN_PERIOD`]) passes, then the stop is
//!   signalled. Stops, removals and launches run off the tick;
//! - starts a replica that waited for an older revision to give up its
//!   block volume;
//! - publishes the state of every replica for the status reporter.
//!
//! The decisions are made by [`ReplicaRun::advance`] and the methods next to
//! it, which are plain functions of the time and what `faber-node` showed,
//! and never wait. [`Supervisor::tick`] carries them out.

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use faber_api_types::workload::limits::PROBE_FAILURES_BEFORE_RESTART;
use faber_api_types::workload::{ReplicaState, RestartPolicy};
use faber_node_protocol::{ContainerState, ReplicaInfo, ReplicaRef, SandboxState};
use ipnet::Ipv4Net;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use super::backoff::Backoff;
use super::clock::Clock;
use super::desired::DesiredReplica;
use super::env::EnvSource;
use super::launch::{
    find_container, launch, remove_unused_ramdisks, restart_container, retire, stop_container,
};
use super::node::{NodeClient, list_replicas};
use super::probe::{ProbeOutcome, ProbeTarget, Prober};
use super::state::{Observed, RunnerState, StopReason, slot_of, workload_of};
use super::{DEFAULT_STOP_GRACE_SECS, DRAIN_PERIOD, POLL_INTERVAL};

/// The exit code a container is booked with when a probe stopped it: what a
/// killed process shows. It is a failure for `on_failure`.
const PROBE_KILL_CODE: i32 = 137;

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    /// Time between ticks.
    pub poll: Duration,
    /// How long an older revision runs on after the newer one is available.
    pub drain: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            poll: POLL_INTERVAL,
            drain: DRAIN_PERIOD,
        }
    }
}

/// Where a container is in its life, as the supervisor sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Not seen running yet.
    Pending,
    Up {
        since: Duration,
    },
    /// Exited; restarts at `until`.
    Waiting {
        until: Duration,
    },
    /// Exited and not coming back: the restart policy says no.
    Done,
}

/// What `faber-node` showed of one container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    Running,
    /// Made, not started.
    Created,
    Exited(Option<i32>),
    /// Not there, or not known to be alive.
    Gone,
}

#[derive(Debug)]
struct ContainerRun {
    phase: Phase,
    backoff: Backoff,
    next_probe: Duration,
    /// Failed probes in a row.
    failures: u32,
    /// The last probe: `None` before the first.
    probe_ok: Option<bool>,
    /// Why the container is not well, for people.
    note: Option<String>,
}

impl ContainerRun {
    fn new() -> Self {
        Self {
            phase: Phase::Pending,
            backoff: Backoff::default(),
            next_probe: Duration::ZERO,
            failures: 0,
            probe_ok: None,
            note: None,
        }
    }

    fn up(&mut self, now: Duration, interval: Option<Duration>) {
        self.phase = Phase::Up { since: now };
        // The first probe waits one interval.
        self.next_probe = now + interval.unwrap_or_default();
        self.failures = 0;
        self.probe_ok = None;
        self.note = None;
    }

    /// The container is down. `code` is `None` when it vanished.
    fn exited(&mut self, now: Duration, policy: RestartPolicy, code: Option<i32>) {
        let up = match self.phase {
            Phase::Up { since } => now.saturating_sub(since),
            _ => Duration::ZERO,
        };
        let restart = match policy {
            RestartPolicy::Always => true,
            RestartPolicy::OnFailure => code != Some(0),
            RestartPolicy::Never => false,
        };
        self.probe_ok = None;
        self.failures = 0;
        self.note = Some(match code {
            Some(code) => format!("exited with code {code}"),
            None => "is gone".to_owned(),
        });
        self.phase = if restart {
            Phase::Waiting {
                until: now + self.backoff.on_exit(up),
            }
        } else {
            Phase::Done
        };
    }
}

/// What a tick must do for a container, found by [`ReplicaRun::advance`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Due {
    Restart {
        container: usize,
    },
    Probe {
        container: usize,
        container_id: String,
        target: ProbeTarget,
    },
}

fn seen(pod: Option<&ReplicaInfo>, name: &str) -> Seen {
    let Some(pod) = pod.filter(|p| p.state == SandboxState::Ready) else {
        return Seen::Gone;
    };
    match find_container(pod, name) {
        None => Seen::Gone,
        Some(c) => match c.state {
            ContainerState::Running => Seen::Running,
            ContainerState::Created => Seen::Created,
            ContainerState::Exited => Seen::Exited(c.exit_code),
            ContainerState::Unknown => Seen::Gone,
        },
    }
}

/// The pod's address, if it is a host address in this node's container
/// subnet. Nothing else is probed or reported: the address comes from the
/// runtime, and a probe goes wherever it points.
fn pod_ip(pod: Option<&ReplicaInfo>, subnet: Option<Ipv4Net>) -> Option<Ipv4Addr> {
    let ip: Ipv4Addr = pod?.pod_ip.as_deref()?.parse().ok()?;
    let net = subnet?;
    (net.contains(&ip) && ip != net.network() && ip != net.broadcast()).then_some(ip)
}

/// One replica under supervision: a run per container, in spec order.
#[derive(Debug)]
struct ReplicaRun {
    containers: Vec<ContainerRun>,
}

impl ReplicaRun {
    fn new(d: &DesiredReplica) -> Self {
        Self {
            containers: d.containers.iter().map(|_| ContainerRun::new()).collect(),
        }
    }

    /// Looks at `pod` at time `now` and returns what is due. Changes only
    /// the books; calls nothing.
    fn advance(
        &mut self,
        now: Duration,
        d: &DesiredReplica,
        pod: Option<&ReplicaInfo>,
        subnet: Option<Ipv4Net>,
    ) -> Vec<Due> {
        let mut due = Vec::new();
        for (index, (run, container)) in self.containers.iter_mut().zip(&d.containers).enumerate() {
            let interval = container
                .spec
                .probe
                .as_ref()
                .map(|p| Duration::from_secs(u64::from(p.interval_secs)));
            let now_seen = seen(pod, &container.spec.name);
            match now_seen {
                Seen::Running => match run.phase {
                    Phase::Up { .. } => {}
                    Phase::Pending => run.up(now, interval),
                    // Still running although it is to go: a stop that the
                    // probes asked for is under way, or failed. The restart
                    // stops it first.
                    Phase::Waiting { until } if now >= until => {
                        due.push(Due::Restart { container: index });
                    }
                    Phase::Waiting { .. } | Phase::Done => {}
                },
                Seen::Created => {
                    if !matches!(run.phase, Phase::Waiting { until } if now < until) {
                        due.push(Due::Restart { container: index });
                    }
                }
                Seen::Exited(_) | Seen::Gone => {
                    let code = match now_seen {
                        Seen::Exited(code) => code,
                        _ => None,
                    };
                    match run.phase {
                        Phase::Pending | Phase::Up { .. } => run.exited(now, d.restart, code),
                        Phase::Waiting { until } if now >= until => {
                            due.push(Due::Restart { container: index });
                        }
                        Phase::Waiting { .. } | Phase::Done => {}
                    }
                }
            }
            if let (Phase::Up { .. }, Seen::Running, Some(probe), Some(interval)) =
                (run.phase, now_seen, container.spec.probe.as_ref(), interval)
                && now >= run.next_probe
            {
                run.next_probe = now + interval;
                if let (Some(ip), Some(found)) = (
                    pod_ip(pod, subnet),
                    pod.and_then(|p| find_container(p, &container.spec.name)),
                ) {
                    due.push(Due::Probe {
                        container: index,
                        container_id: found.id.clone(),
                        target: ProbeTarget {
                            ip,
                            action: probe.action.clone(),
                            timeout: Duration::from_secs(u64::from(probe.timeout_secs)),
                        },
                    });
                }
            }
        }
        due
    }

    /// Books a probe. Returns whether the container must be stopped now: the
    /// third failure in a row.
    fn probed(
        &mut self,
        container: usize,
        now: Duration,
        d: &DesiredReplica,
        outcome: ProbeOutcome,
    ) -> bool {
        let run = &mut self.containers[container];
        match outcome {
            ProbeOutcome::Pass => {
                run.failures = 0;
                run.probe_ok = Some(true);
                run.note = None;
                false
            }
            ProbeOutcome::Fail(why) => {
                run.failures += 1;
                run.probe_ok = Some(false);
                if run.failures >= PROBE_FAILURES_BEFORE_RESTART {
                    // The probe stops the container; the policy decides the rest.
                    run.exited(now, d.restart, Some(PROBE_KILL_CODE));
                    run.note = Some(format!("stopped after failed probes: {why}"));
                    true
                } else {
                    run.note = Some(format!("probe failed: {why}"));
                    false
                }
            }
        }
    }

    fn restarted(&mut self, container: usize, now: Duration, d: &DesiredReplica) {
        let interval = d.containers[container]
            .spec
            .probe
            .as_ref()
            .map(|p| Duration::from_secs(u64::from(p.interval_secs)));
        self.containers[container].up(now, interval);
    }

    fn restart_failed(&mut self, container: usize, now: Duration, why: String) {
        let run = &mut self.containers[container];
        run.phase = Phase::Waiting {
            until: now + run.backoff.on_exit(Duration::ZERO),
        };
        run.note = Some(format!("could not restart: {why}"));
    }

    fn report(
        &self,
        d: &DesiredReplica,
        pod: Option<&ReplicaInfo>,
        subnet: Option<Ipv4Net>,
    ) -> Observed {
        let mut state = ReplicaState::Available;
        let mut detail = None;
        for (run, container) in self.containers.iter().zip(&d.containers) {
            let health = match run.phase {
                Phase::Pending => ReplicaState::Deploying,
                Phase::Up { .. } if container.spec.probe.is_none() => ReplicaState::Available,
                Phase::Up { .. } => match run.probe_ok {
                    None => ReplicaState::Deploying,
                    Some(true) => ReplicaState::Available,
                    Some(false) => ReplicaState::Unavailable,
                },
                Phase::Waiting { .. } | Phase::Done => ReplicaState::Unavailable,
            };
            if health == ReplicaState::Unavailable && detail.is_none() {
                detail = run
                    .note
                    .as_ref()
                    .map(|note| format!("container {} {note}", container.spec.name));
            }
            state = match (state, health) {
                (ReplicaState::Unavailable, _) | (_, ReplicaState::Unavailable) => {
                    ReplicaState::Unavailable
                }
                (ReplicaState::Deploying, _) | (_, ReplicaState::Deploying) => {
                    ReplicaState::Deploying
                }
                _ => ReplicaState::Available,
            };
        }
        Observed {
            state,
            pod_ip: pod_ip(pod, subnet),
            detail,
        }
    }
}

/// What a background operation reports when it ends.
enum Finished {
    /// A pod was removed (or failed to be) and its ramdisks collected.
    Retired(ReplicaRef, Result<(), String>),
    /// A deferred launch ran.
    Launched(ReplicaRef, Result<(), String>),
    /// A probe-triggered stop of a container ran.
    Stopped(ReplicaRef, usize),
}

/// A stop that failed probes asked for, waiting out the drain period. The
/// replica reads `unavailable` meanwhile.
#[derive(Debug)]
struct PendingStop {
    container_id: String,
    /// When the stop is signalled.
    due: Duration,
    grace_secs: u32,
    in_flight: bool,
}

pub struct Supervisor<C, K, P> {
    client: C,
    clock: K,
    prober: Arc<P>,
    env: Arc<dyn EnvSource>,
    state: Arc<RunnerState>,
    config: SupervisorConfig,
    runs: HashMap<ReplicaRef, ReplicaRun>,
    /// Since when each replica that is to stop has been waiting to, with the
    /// `stop_gen` of the ask it belongs to.
    stop_since: HashMap<ReplicaRef, (u64, Duration)>,
    /// Stops, removals and deferred launches run here, off the tick: a stop
    /// waits out the container's grace period, a pull takes minutes, and
    /// neither may delay noticing the next exit.
    background: JoinSet<Finished>,
    /// Replicas with a removal or a launch in `background`.
    busy: HashSet<ReplicaRef>,
    /// Containers whose probes failed, by replica and container index. They
    /// are not restarted until the stop has run: the restart would stop the
    /// container again.
    probe_stops: HashMap<(ReplicaRef, usize), PendingStop>,
    /// Deferred launches that failed: the backoff and when to try again.
    launch_retry: HashMap<ReplicaRef, (Backoff, Duration)>,
}

impl<C: NodeClient + Clone, K: Clock, P: Prober> Supervisor<C, K, P> {
    pub fn new(
        client: C,
        clock: K,
        prober: P,
        env: Arc<dyn EnvSource>,
        state: Arc<RunnerState>,
        config: SupervisorConfig,
    ) -> Self {
        Self {
            client,
            clock,
            prober: Arc::new(prober),
            env,
            state,
            config,
            runs: HashMap::new(),
            stop_since: HashMap::new(),
            background: JoinSet::new(),
            busy: HashSet::new(),
            probe_stops: HashMap::new(),
            launch_retry: HashMap::new(),
        }
    }

    /// Ticks until `cancel` fires.
    pub fn spawn(self, cancel: CancellationToken) -> JoinHandle<()>
    where
        C: 'static,
    {
        tokio::spawn(self.run(cancel))
    }

    async fn run(mut self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.config.poll);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {}
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = self.tick() => {}
            }
        }
        self.background.abort_all();
    }

    fn finished(&mut self, joined: Result<Finished, tokio::task::JoinError>) {
        match joined {
            Ok(Finished::Retired(key, result)) => {
                self.busy.remove(&key);
                if let Err(e) = result {
                    // The stop request stays, so the next tick tries again.
                    tracing::warn!(replica = ?key, error = %e, "could not remove a replica");
                }
            }
            Ok(Finished::Launched(key, result)) => {
                self.busy.remove(&key);
                match result {
                    Ok(()) => {
                        self.launch_retry.remove(&key);
                    }
                    Err(_) => {
                        // Again after a pause that grows, as for a container
                        // that keeps exiting.
                        let now = self.clock.now();
                        let (backoff, at) = self.launch_retry.entry(key).or_default();
                        *at = now + backoff.on_exit(Duration::ZERO);
                    }
                }
            }
            Ok(Finished::Stopped(key, container)) => {
                self.probe_stops.remove(&(key, container));
            }
            Err(e) => {
                tracing::error!(error = %e, "a background operation of the supervisor failed");
                self.busy.clear();
                self.probe_stops.clear();
            }
        }
    }

    fn reap(&mut self) {
        while let Some(joined) = self.background.try_join_next() {
            self.finished(joined);
        }
    }

    /// Waits for the background operations. For tests.
    #[cfg(test)]
    pub async fn settle(&mut self) {
        while let Some(joined) = self.background.join_next().await {
            self.finished(joined);
        }
    }

    /// One pass: list, decide, act, publish.
    pub async fn tick(&mut self) {
        let now = self.clock.now();
        self.reap();
        // Entries first, then the listing: an entry marked launched has
        // been started before the listing is read.
        let entries = self.state.entries();
        let listed = match list_replicas(&self.client).await {
            Ok(listed) => listed,
            Err(e) => {
                tracing::warn!(error = %e, "could not list the replicas on this node");
                return;
            }
        };
        let pods: HashMap<&ReplicaRef, &ReplicaInfo> =
            listed.iter().map(|p| (&p.replica, p)).collect();
        let subnet = self.state.subnet();
        let known: HashSet<&ReplicaRef> = entries.iter().map(|(k, _)| k).collect();
        self.runs.retain(|k, _| known.contains(k));

        // Replicas that are to stop are no longer looked after.
        let mut dues = Vec::new();
        for (key, entry) in &entries {
            let Some(d) = entry
                .desired
                .as_ref()
                .filter(|_| entry.launched && entry.stop.is_none())
            else {
                continue;
            };
            let run = self
                .runs
                .entry(key.clone())
                .or_insert_with(|| ReplicaRun::new(d));
            if run.containers.len() != d.containers.len() {
                *run = ReplicaRun::new(d);
            }
            let pod = pods.get(key).copied();
            for due in run.advance(now, d, pod, subnet) {
                dues.push((key.clone(), d.clone(), due));
            }
        }

        // Probes run side by side; restarts one after another meanwhile.
        let mut probes = JoinSet::new();
        for (key, _, due) in &dues {
            if let Due::Probe {
                container,
                container_id,
                target,
            } = due
            {
                let prober = self.prober.clone();
                let (key, container, container_id, target) = (
                    key.clone(),
                    *container,
                    container_id.clone(),
                    target.clone(),
                );
                probes.spawn(async move {
                    let outcome = prober.probe(target).await;
                    (key, container, container_id, outcome)
                });
            }
        }
        for (key, d, due) in &dues {
            let Due::Restart { container } = due else {
                continue;
            };
            if self.probe_stops.contains_key(&(key.clone(), *container)) {
                continue;
            }
            let result = restart_container(
                &self.client,
                &self.state,
                self.env.as_ref(),
                d,
                &d.containers[*container],
                pods.get(key).copied(),
            )
            .await;
            let Some(run) = self.runs.get_mut(key) else {
                continue;
            };
            let after = self.clock.now();
            match result {
                // Removed or told to stop since the snapshot: leave it.
                Ok(false) => {}
                Ok(true) => {
                    tracing::info!(replica = ?key, container = %d.containers[*container].spec.name, "restarted a container");
                    run.restarted(*container, after, d);
                }
                Err(e) => {
                    tracing::warn!(replica = ?key, error = %e, "could not restart a container");
                    run.restart_failed(*container, after, e.to_string());
                }
            }
        }
        while let Some(joined) = probes.join_next().await {
            let Ok((key, container, container_id, outcome)) = joined else {
                continue;
            };
            let Some(d) = entries
                .iter()
                .find(|(k, _)| *k == key)
                .and_then(|(_, e)| e.desired.clone())
            else {
                continue;
            };
            let Some(run) = self.runs.get_mut(&key) else {
                continue;
            };
            if run.probed(container, self.clock.now(), &d, outcome) {
                tracing::warn!(replica = ?key, "a container failed its probes; stopping it after the drain period");
                // Unavailable now (the probe failed); stopped after the drain.
                self.probe_stops.insert(
                    (key, container),
                    PendingStop {
                        container_id,
                        due: self.clock.now() + self.config.drain,
                        grace_secs: d.stop_grace_secs,
                        in_flight: false,
                    },
                );
            }
        }

        let mut observed = HashMap::new();
        for (key, entry) in &entries {
            if let (Some(d), Some(run), true) = (&entry.desired, self.runs.get(key), entry.launched)
            {
                observed.insert(key.clone(), run.report(d, pods.get(key).copied(), subnet));
            }
        }
        self.ask_for_stops(&entries, &observed);
        self.state.set_observed(observed);
        self.stop_probed(now);
        self.stop_due(now, &pods);
        self.start_deferred(now, &pods);
    }

    /// Asks for the stop of the replicas of an older revision once every
    /// replica of the newest revision here is available, and takes the ask
    /// back if that stops being so before the stop is signalled.
    fn ask_for_stops(
        &self,
        entries: &[(ReplicaRef, super::state::Entry)],
        observed: &HashMap<ReplicaRef, Observed>,
    ) {
        for (key, _) in entries.iter().filter(|(_, e)| e.superseded) {
            let mut successors = entries.iter().filter(|(k, e)| {
                !e.superseded
                    && e.stop.is_none()
                    && workload_of(k) == workload_of(key)
                    && e.desired.is_some()
            });
            let ready = successors.clone().next().is_some()
                && successors.all(|(k, e)| {
                    e.launched
                        && observed
                            .get(k)
                            .is_some_and(|o| o.state == ReplicaState::Available)
                });
            if ready {
                self.state.request_stop(key, StopReason::Superseded);
            } else {
                self.state.cancel_superseded_stop(key);
            }
        }
    }

    /// Signals the stops that failed probes asked for, once their drain
    /// period is over. A replica that was removed or is stopping anyway is
    /// left to that.
    fn stop_probed(&mut self, now: Duration) {
        let live: HashSet<ReplicaRef> = self
            .state
            .entries()
            .into_iter()
            .filter(|(_, e)| e.launched && e.stop.is_none())
            .map(|(k, _)| k)
            .collect();
        self.probe_stops
            .retain(|(key, _), p| p.in_flight || live.contains(key));
        for ((key, container), pending) in &mut self.probe_stops {
            if pending.in_flight || now < pending.due {
                continue;
            }
            pending.in_flight = true;
            let (client, id, grace) = (
                self.client.clone(),
                pending.container_id.clone(),
                pending.grace_secs,
            );
            let (key, container) = (key.clone(), *container);
            self.background.spawn(async move {
                if let Err(e) = stop_container(&client, &id, grace).await {
                    tracing::warn!(error = %e, "could not stop a container");
                }
                Finished::Stopped(key, container)
            });
        }
    }

    /// Signals the stop of the replicas that have waited the drain period
    /// since they read `unavailable`.
    fn stop_due(&mut self, now: Duration, pods: &HashMap<&ReplicaRef, &ReplicaInfo>) {
        let stopping: Vec<_> = self
            .state
            .entries()
            .into_iter()
            .filter(|(_, e)| e.stop.is_some())
            .collect();
        let names: HashSet<&ReplicaRef> = stopping.iter().map(|(k, _)| k).collect();
        self.stop_since.retain(|k, _| names.contains(k));
        for (key, entry) in &stopping {
            let since = self
                .stop_since
                .entry(key.clone())
                .or_insert((entry.stop_gen, now));
            if since.0 != entry.stop_gen {
                // A stop that was withdrawn and asked for again starts its
                // drain period over.
                *since = (entry.stop_gen, now);
            }
            let since = since.1;
            if now.saturating_sub(since) < self.config.drain || self.busy.contains(key) {
                continue;
            }
            let grace = entry
                .desired
                .as_ref()
                .map_or(DEFAULT_STOP_GRACE_SECS, |d| d.stop_grace_secs);
            let (client, state) = (self.client.clone(), self.state.clone());
            let (replica, pod) = (key.clone(), pods.get(key).map(|p| (*p).clone()));
            self.busy.insert(key.clone());
            tracing::info!(replica = ?key, reason = ?entry.stop, "stopping a replica");
            self.background.spawn(async move {
                let mut result = retire(&client, &state, &replica, pod.as_ref(), grace)
                    .await
                    .map_err(|e| e.to_string());
                if result.is_ok() {
                    result = remove_unused_ramdisks(&client, &state)
                        .await
                        .map_err(|e| e.to_string());
                }
                Finished::Retired(replica, result)
            });
        }
    }

    /// Starts the replicas that waited for an older revision to give up its
    /// block volume, now that nothing else is in their slot.
    fn start_deferred(&mut self, now: Duration, pods: &HashMap<&ReplicaRef, &ReplicaInfo>) {
        for (key, entry) in self.state.entries() {
            let Some(d) = entry
                .desired
                .clone()
                .filter(|_| entry.deferred && !entry.launched && entry.stop.is_none())
            else {
                continue;
            };
            let slot = slot_of(&key);
            let held_by_pod = pods.keys().any(|p| **p != key && slot_of(p) == slot);
            if self.busy.contains(&key)
                || self.launch_retry.get(&key).is_some_and(|(_, at)| now < *at)
                || held_by_pod
                || !self.state.others_in_slot(&slot, &key).is_empty()
            {
                continue;
            }
            let (client, state, env) = (self.client.clone(), self.state.clone(), self.env.clone());
            self.busy.insert(key.clone());
            self.background.spawn(async move {
                let result = launch(&client, &state, env.as_ref(), &d, None)
                    .await
                    .map_err(|e| e.to_string());
                if let Err(e) = &result {
                    tracing::warn!(replica = ?d.replica, error = %e, "could not start a replica");
                    state.mark_failed(&d.replica, e.clone());
                }
                Finished::Launched(d.replica.clone(), result)
            });
        }
    }
}

#[cfg(test)]
mod tests;
