//! Reconcile steps.
//!
//! Host reconciliation is an ordered list of independent steps. A step maps
//! the replicated state to its own desired value ([`ReconcileStep::plan`])
//! and applies it ([`ReconcileStep::apply`]). The [`StepRunner`] remembers,
//! per step, the last value that step applied successfully, and applies a
//! step only when its new desired value differs from that, when it failed the
//! last time, or when the caller forces a resync. A failing step is logged,
//! the steps after it still run, and the run as a whole reports an error so
//! the loop backs off.

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use faber_container_runtime::ContainerRuntimeSpec;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{ReconcileTiming, next_invite_expiry, run_loop};
use crate::mesh::state::MeshState;
use crate::raft::store::{ReadError, StateReader};

/// The future of [`ReconcileStep::apply`].
pub type StepFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

/// The replicated state as of one plan, read once and shared by every step.
#[derive(Debug, Clone)]
pub struct ReplicatedStateView {
    /// `None` before `InitCluster`.
    pub mesh: Option<MeshState>,
    /// The replicated container runtime settings, `None` before `InitCluster`.
    pub containerd: Option<ContainerRuntimeSpec>,
    /// Unix seconds when the view was read.
    pub now: u64,
}

impl ReplicatedStateView {
    /// Reads the view from `reader`.
    pub fn read(reader: &StateReader, now: u64) -> Result<Self, ReadError> {
        Ok(Self {
            mesh: reader.mesh()?,
            containerd: reader.containerd()?,
            now,
        })
    }

    /// [`next_invite_expiry`]: when the view changes with no state change.
    pub fn next_change(&self) -> Option<Duration> {
        next_invite_expiry(self.mesh.as_ref(), self.now)
    }
}

/// What [`ReconcileStep::plan`] returns.
#[derive(Debug)]
pub struct Planned<D> {
    pub desired: D,
    /// Lines for the operator: something was skipped or looks wrong.
    pub warnings: Vec<String>,
}

/// One independent part of host reconciliation.
pub trait ReconcileStep: Send + Sync + 'static {
    /// What the step wants applied. Compared with the last value it applied.
    type Desired: Clone + PartialEq + Debug + Send + Sync + 'static;

    /// Used in logs.
    fn name(&self) -> &'static str;

    /// The desired value for `view`. Pure: nothing is applied.
    fn plan(&self, view: &ReplicatedStateView) -> Planned<Self::Desired>;

    /// Makes the host match `desired`.
    fn apply<'a>(&'a self, desired: &'a Self::Desired) -> StepFuture<'a>;
}

/// Object-safe view of a desired value: clone, compare and downcast.
trait DynValue: Send + Sync {
    fn clone_box(&self) -> Box<dyn DynValue>;
    fn eq_dyn(&self, other: &dyn DynValue) -> bool;
    fn as_any(&self) -> &dyn Any;
    fn fmt_dyn(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

impl<T: Clone + PartialEq + Debug + Send + Sync + 'static> DynValue for T {
    fn clone_box(&self) -> Box<dyn DynValue> {
        Box::new(self.clone())
    }

    fn eq_dyn(&self, other: &dyn DynValue) -> bool {
        other.as_any().downcast_ref::<T>() == Some(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_dyn(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt(f)
    }
}

/// A step's desired value with its type erased. Two values are equal when
/// they have the same type and are equal.
pub struct StepValue(Box<dyn DynValue>);

impl StepValue {
    pub fn new<T: Clone + PartialEq + Debug + Send + Sync + 'static>(value: T) -> Self {
        Self(Box::new(value))
    }

    /// The value, if it is a `T`.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.as_any().downcast_ref()
    }
}

impl Clone for StepValue {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

impl PartialEq for StepValue {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_dyn(&*other.0)
    }
}

impl Debug for StepValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt_dyn(f)
    }
}

/// [`ReconcileStep`] with the desired type erased.
trait ErasedStep: Send + Sync {
    fn name(&self) -> &'static str;
    fn plan(&self, view: &ReplicatedStateView) -> (StepValue, Vec<String>);
    fn apply<'a>(&'a self, desired: &'a StepValue) -> StepFuture<'a>;
}

struct Typed<S>(S);

impl<S: ReconcileStep> ErasedStep for Typed<S> {
    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn plan(&self, view: &ReplicatedStateView) -> (StepValue, Vec<String>) {
        let planned = self.0.plan(view);
        (StepValue::new(planned.desired), planned.warnings)
    }

    fn apply<'a>(&'a self, desired: &'a StepValue) -> StepFuture<'a> {
        match desired.downcast_ref::<S::Desired>() {
            Some(desired) => self.0.apply(desired),
            None => {
                let name = self.0.name();
                Box::pin(async move { anyhow::bail!("{name}: desired value of another step") })
            }
        }
    }
}

/// [`StepList::plan`]: every step's desired value, in step order.
#[derive(Debug)]
pub struct StepsPlan {
    pub desired: Vec<StepValue>,
    /// The warnings of every step, in step order.
    pub warnings: Vec<String>,
}

/// The steps, in the order they run.
#[derive(Default)]
pub struct StepList(Vec<Box<dyn ErasedStep>>);

impl StepList {
    /// Appends `step` after the ones already in the list.
    pub fn with<S: ReconcileStep>(mut self, step: S) -> Self {
        self.0.push(Box::new(Typed(step)));
        self
    }

    /// Plans every step against `view`.
    pub fn plan(&self, view: &ReplicatedStateView) -> StepsPlan {
        let mut desired = Vec::with_capacity(self.0.len());
        let mut warnings = Vec::new();
        for step in &self.0 {
            let (value, mut step_warnings) = step.plan(view);
            desired.push(value);
            warnings.append(&mut step_warnings);
        }
        StepsPlan { desired, warnings }
    }
}

/// The steps plus what each of them last applied. Clones share that record.
///
/// Applies must not overlap (the loop runs one at a time). The record is
/// locked only for a moment before and after a step runs, never while it
/// does.
#[derive(Clone)]
pub struct StepRunner {
    steps: Arc<StepList>,
    /// Per step: the last value it applied successfully, `None` after a
    /// failure or before the first apply.
    applied: Arc<Mutex<Vec<Option<StepValue>>>>,
}

impl StepRunner {
    /// A runner for `steps`; nothing is applied yet.
    pub fn new(steps: StepList) -> Self {
        let applied = vec![None; steps.0.len()];
        Self {
            steps: Arc::new(steps),
            applied: Arc::new(Mutex::new(applied)),
        }
    }

    /// The steps, for planning without the applied state.
    pub fn steps(&self) -> Arc<StepList> {
        self.steps.clone()
    }

    /// [`StepList::plan`].
    pub fn plan(&self, view: &ReplicatedStateView) -> StepsPlan {
        self.steps.plan(view)
    }

    fn record(&self) -> MutexGuard<'_, Vec<Option<StepValue>>> {
        // Nothing here can leave the record half written.
        self.applied.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether step `index` has to run for `value`. If so, its record is
    /// cleared, so that it counts as failed until it succeeds.
    fn begin(&self, index: usize, value: &StepValue, force: bool) -> bool {
        let mut record = self.record();
        if !force && record[index].as_ref() == Some(value) {
            return false;
        }
        record[index] = None;
        true
    }

    fn succeeded(&self, index: usize, value: &StepValue) {
        self.record()[index] = Some(value.clone());
    }

    /// Applies `desired` in step order and stops at the first failure. For
    /// startup, where nothing works without the earlier steps. Steps that
    /// succeeded are remembered as applied.
    pub async fn bring_up(&self, desired: &[StepValue]) -> anyhow::Result<()> {
        debug_assert_eq!(desired.len(), self.steps.0.len());
        for (index, (step, value)) in self.steps.0.iter().zip(desired).enumerate() {
            self.begin(index, value, true);
            step.apply(value).await?;
            self.succeeded(index, value);
        }
        Ok(())
    }

    /// Applies `desired` in step order. A step runs when its value differs
    /// from the one it last applied, when its last attempt failed, or when
    /// `force` is set. A failing step is logged and the steps after it still
    /// run; the result is an error if any step failed.
    pub async fn apply(&self, desired: &[StepValue], force: bool) -> anyhow::Result<()> {
        debug_assert_eq!(desired.len(), self.steps.0.len());
        let mut failures: Vec<(&'static str, anyhow::Error)> = Vec::new();
        for (index, (step, value)) in self.steps.0.iter().zip(desired).enumerate() {
            if !self.begin(index, value, force) {
                continue;
            }
            match step.apply(value).await {
                Ok(()) => self.succeeded(index, value),
                Err(e) => {
                    tracing::warn!(
                        step = step.name(),
                        error = %format!("{e:#}"),
                        "host apply step failed"
                    );
                    failures.push((step.name(), e));
                }
            }
        }
        match failures.len() {
            0 => Ok(()),
            1 => Err(failures.remove(0).1),
            _ => Err(anyhow::anyhow!(
                "{}",
                failures
                    .iter()
                    .map(|(name, e)| format!("{name}: {e:#}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            )),
        }
    }
}

/// Spawns the loop that plans `runner`'s steps from `read_view` and applies
/// them. `applied` is what startup already brought up (the runner must have
/// applied it too), `None` to apply everything on the first pass. The loop
/// and its triggers are [`run_loop`].
pub fn spawn_steps<R>(
    name: &'static str,
    changes: watch::Receiver<u64>,
    timing: ReconcileTiming,
    runner: StepRunner,
    applied: Option<Vec<StepValue>>,
    read_view: R,
    shutdown: CancellationToken,
) -> JoinHandle<()>
where
    R: Fn() -> Result<ReplicatedStateView, ReadError> + Send + 'static,
{
    let steps = runner.steps();
    let compute = move || match read_view() {
        Ok(view) => {
            let plan = steps.plan(&view);
            for w in plan.warnings {
                tracing::debug!("{w}");
            }
            (Some(plan.desired), view.next_change())
        }
        Err(_) => (None, None),
    };
    let apply = move |desired: Vec<StepValue>, force: bool| {
        let runner = runner.clone();
        async move { runner.apply(&desired, force).await }
    };
    tokio::spawn(run_loop(
        name, changes, timing, applied, compute, apply, shutdown,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::time::{Instant, timeout};

    use super::*;

    /// A step over a number the test controls. Every apply is logged as
    /// `name=value`; the first `fail` applies fail.
    struct Fake {
        name: &'static str,
        value: Arc<AtomicU32>,
        log: Arc<Mutex<Vec<String>>>,
        fail: Arc<AtomicU32>,
        stamps: Arc<Mutex<Vec<Instant>>>,
    }

    impl ReconcileStep for Fake {
        type Desired = u32;

        fn name(&self) -> &'static str {
            self.name
        }

        fn plan(&self, _view: &ReplicatedStateView) -> Planned<u32> {
            Planned {
                desired: self.value.load(Ordering::SeqCst),
                warnings: vec![],
            }
        }

        fn apply<'a>(&'a self, desired: &'a u32) -> StepFuture<'a> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}={desired}", self.name));
            self.stamps.lock().unwrap().push(Instant::now());
            let failing = self
                .fail
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            Box::pin(async move {
                if failing {
                    anyhow::bail!("{} failed", self.name);
                }
                Ok(())
            })
        }
    }

    type Knobs = (Arc<AtomicU32>, Arc<AtomicU32>, Arc<Mutex<Vec<Instant>>>);

    struct Rig {
        runner: StepRunner,
        log: Arc<Mutex<Vec<String>>>,
        /// Per step: (value, failures left, apply times).
        knobs: Vec<Knobs>,
    }

    fn rig(names: &[&'static str]) -> Rig {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut list = StepList::default();
        let mut knobs = Vec::new();
        for &name in names {
            let (value, fail, stamps) = (
                Arc::new(AtomicU32::new(1)),
                Arc::new(AtomicU32::new(0)),
                Arc::new(Mutex::new(Vec::new())),
            );
            knobs.push((value.clone(), fail.clone(), stamps.clone()));
            list = list.with(Fake {
                name,
                value,
                log: log.clone(),
                fail,
                stamps,
            });
        }
        Rig {
            runner: StepRunner::new(list),
            log,
            knobs,
        }
    }

    impl Rig {
        fn take_log(&self) -> Vec<String> {
            std::mem::take(&mut *self.log.lock().unwrap())
        }

        fn plan(&self) -> Vec<StepValue> {
            self.runner.plan(&view()).desired
        }
    }

    fn view() -> ReplicatedStateView {
        ReplicatedStateView {
            mesh: None,
            containerd: None,
            now: 0,
        }
    }

    #[tokio::test]
    async fn steps_run_in_order() {
        let rig = rig(&["network", "runtime", "containers"]);
        let desired = rig.plan();
        rig.runner.apply(&desired, false).await.unwrap();
        assert_eq!(rig.take_log(), ["network=1", "runtime=1", "containers=1"]);
    }

    #[tokio::test]
    async fn a_failed_step_does_not_stop_later_steps() {
        let rig = rig(&["a", "b", "c"]);
        rig.knobs[0].1.store(1, Ordering::SeqCst);
        let desired = rig.plan();
        let err = rig.runner.apply(&desired, false).await.unwrap_err();
        assert_eq!(format!("{err:#}"), "a failed");
        assert_eq!(rig.take_log(), ["a=1", "b=1", "c=1"]);

        rig.knobs[0].1.store(1, Ordering::SeqCst);
        rig.knobs[2].1.store(1, Ordering::SeqCst);
        rig.knobs[2].0.store(2, Ordering::SeqCst);
        let desired = rig.plan();
        let err = rig.runner.apply(&desired, false).await.unwrap_err();
        assert_eq!(format!("{err:#}"), "a: a failed; c: c failed");
    }

    #[tokio::test]
    async fn an_unchanged_step_is_not_applied() {
        let rig = rig(&["a", "b"]);
        let desired = rig.plan();
        rig.runner.apply(&desired, false).await.unwrap();
        rig.take_log();

        rig.runner.apply(&desired, false).await.unwrap();
        assert!(rig.take_log().is_empty());

        rig.knobs[1].0.store(2, Ordering::SeqCst);
        let desired = rig.plan();
        rig.runner.apply(&desired, false).await.unwrap();
        assert_eq!(rig.take_log(), ["b=2"]);
    }

    #[tokio::test]
    async fn force_applies_every_step_again() {
        let rig = rig(&["a", "b"]);
        let desired = rig.plan();
        rig.runner.apply(&desired, false).await.unwrap();
        rig.take_log();

        rig.runner.apply(&desired, true).await.unwrap();
        assert_eq!(rig.take_log(), ["a=1", "b=1"]);
    }

    #[tokio::test]
    async fn a_failed_step_is_retried_with_the_same_value() {
        let rig = rig(&["a", "b"]);
        rig.knobs[1].1.store(1, Ordering::SeqCst);
        let desired = rig.plan();
        rig.runner.apply(&desired, false).await.unwrap_err();
        rig.take_log();

        rig.runner.apply(&desired, false).await.unwrap();
        assert_eq!(rig.take_log(), ["b=1"]);
        rig.runner.apply(&desired, false).await.unwrap();
        assert!(rig.take_log().is_empty());
    }

    #[tokio::test]
    async fn bring_up_stops_at_the_first_failure_and_seeds_the_runner() {
        let rig = rig(&["a", "b", "c"]);
        rig.knobs[1].1.store(1, Ordering::SeqCst);
        let desired = rig.plan();
        rig.runner.bring_up(&desired).await.unwrap_err();
        assert_eq!(rig.take_log(), ["a=1", "b=1"]);

        rig.runner.bring_up(&desired).await.unwrap();
        rig.take_log();
        rig.runner.apply(&desired, false).await.unwrap();
        assert!(rig.take_log().is_empty());
    }

    #[tokio::test]
    async fn the_loop_backs_off_after_a_failed_step_and_retries_only_it() {
        let rig = rig(&["a", "b"]);
        rig.knobs[0].1.store(1, Ordering::SeqCst);
        let a_stamps = rig.knobs[0].2.clone();
        let (tx, rx) = watch::channel(0u64);
        let shutdown = CancellationToken::new();
        let timing = ReconcileTiming {
            resync: Duration::from_secs(3600),
            backoff_min: Duration::from_millis(40),
            backoff_max: Duration::from_millis(80),
            debounce_quiet: Duration::ZERO,
            debounce_max: Duration::ZERO,
        };
        let log = rig.log.clone();
        let handle = spawn_steps(
            "test",
            rx,
            timing,
            rig.runner,
            None,
            || Ok(view()),
            shutdown.clone(),
        );

        let started = Instant::now();
        timeout(Duration::from_secs(5), async {
            while log.lock().unwrap().len() < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a retry");
        // `b` ran once on the first pass; only `a` was retried, after the backoff.
        assert_eq!(*log.lock().unwrap(), ["a=1", "b=1", "a=1"]);
        let retried_at = a_stamps.lock().unwrap()[1];
        assert!(retried_at - started >= Duration::from_millis(40));

        drop(tx);
        shutdown.cancel();
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("loop stops")
            .unwrap();
    }
}
