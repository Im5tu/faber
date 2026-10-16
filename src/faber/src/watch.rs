//! `faber app show --watch`: polls one workload and draws it again when what
//! it shows changes.

use std::future::Future;
use std::io::Write;
use std::time::Duration;

use faber_api_types::workload::{WorkloadResponse, WorkloadSpec};
use faber_api_types::{ClientError, FaberClient};

use crate::output::{self, OutputFormat};

/// Clears the screen and moves the cursor to the top left.
const CLEAR_SCREEN: &str = "\x1b[2J\x1b[H";

/// Where a watch reads the workload from.
pub(crate) trait WorkloadSource {
    fn get(&self, reveal: bool) -> impl Future<Output = Result<WorkloadResponse, ClientError>>;
}

/// How a watch waits between polls.
pub(crate) trait Sleeper {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()>;
}

/// One workload on the control plane.
pub(crate) struct ClientSource<'a> {
    pub client: &'a FaberClient,
    pub project: &'a str,
    pub environment: &'a str,
    pub workload: &'a str,
}

impl WorkloadSource for ClientSource<'_> {
    async fn get(&self, reveal: bool) -> Result<WorkloadResponse, ClientError> {
        self.client
            .workload_get(self.project, self.environment, self.workload, reveal)
            .await
    }
}

pub(crate) struct TokioSleeper;

impl Sleeper for TokioSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

pub(crate) struct Options<'a> {
    pub interval: Duration,
    pub format: OutputFormat,
    /// Clear the screen before each view (text on a terminal only).
    pub clear: bool,
    /// Turns a client error into the message to show.
    pub describe: &'a dyn Fn(ClientError) -> anyhow::Error,
}

/// A failure that may pass by itself: no answer, or a server fault.
fn retryable(e: &ClientError) -> bool {
    match e {
        ClientError::Http(_) => true,
        ClientError::Api { status, .. } => *status >= 500,
        ClientError::Decode(_) | ClientError::Invalid(_) => false,
    }
}

/// One poll. The env is opened (`reveal=true`) on the first poll and when
/// the stored revision has moved since the last time it was; the other polls
/// ask for it masked and show the env opened before, as the same revision
/// has the same env.
async fn poll(
    source: &impl WorkloadSource,
    revealed: &mut Option<(u64, WorkloadSpec)>,
) -> Result<WorkloadResponse, ClientError> {
    if let Some((revision, spec)) = revealed.as_ref() {
        let mut found = source.get(false).await?;
        if found.revision == *revision {
            found.spec = spec.clone();
            return Ok(found);
        }
    }
    let found = source.get(true).await?;
    *revealed = Some((found.revision, found.spec.clone()));
    Ok(found)
}

fn render(format: OutputFormat, view: &WorkloadResponse) -> anyhow::Result<String> {
    match format {
        OutputFormat::Json => Ok(format!("{}\n", serde_json::to_string(view)?)),
        OutputFormat::Text => Ok(output::workload(format, view)?),
    }
}

/// Polls until `stop` resolves (then returns `Ok`) or a failure that cannot
/// pass by itself (then returns it). Each change of the rendered view is
/// written to `out`; failures that may pass are noted on `err` and tried
/// again after the interval.
pub(crate) async fn watch(
    source: &impl WorkloadSource,
    sleeper: &impl Sleeper,
    opts: &Options<'_>,
    stop: impl Future<Output = ()>,
    out: &mut impl Write,
    err: &mut impl Write,
) -> anyhow::Result<()> {
    tokio::pin!(stop);
    let mut revealed = None;
    let mut last: Option<String> = None;
    loop {
        let polled = tokio::select! {
            biased;
            () = &mut stop => return Ok(()),
            polled = poll(source, &mut revealed) => polled,
        };
        match polled {
            Ok(view) => {
                let text = render(opts.format, &view)?;
                if last.as_deref() != Some(text.as_str()) {
                    if opts.clear {
                        out.write_all(CLEAR_SCREEN.as_bytes())?;
                    } else if last.is_some() && opts.format == OutputFormat::Text {
                        out.write_all(b"\n")?;
                    }
                    out.write_all(text.as_bytes())?;
                    out.flush()?;
                    last = Some(text);
                }
            }
            Err(e) if retryable(&e) => {
                writeln!(err, "{:#}; retrying", (opts.describe)(e))?;
                err.flush()?;
            }
            Err(e) => return Err((opts.describe)(e)),
        }
        tokio::select! {
            biased;
            () = &mut stop => return Ok(()),
            () = sleeper.sleep(opts.interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use faber_api_types::ErrorBody;
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;

    /// The workload at `revision`, its replicas as `(index, revision, state)`.
    /// Env values read `***` unless `revealed`.
    fn view(revision: u64, replicas: &[(u32, u64, &str)], revealed: bool) -> WorkloadResponse {
        let replicas: Vec<_> = replicas
            .iter()
            .map(|(index, rev, state)| {
                json!({"node": 1, "replica_index": index, "revision": rev, "state": state})
            })
            .collect();
        let value = if revealed {
            format!("secret{revision}")
        } else {
            "***".to_string()
        };
        serde_json::from_value(json!({
            "project": "shop", "environment": "staging", "name": "web",
            "fqdn": "web.staging.shop.faber.local",
            "revision": revision, "generation": revision,
            "spec": {"containers": [{
                "name": "web", "image": "nginx", "env": {"KEY": value},
            }]},
            "replicas": replicas,
        }))
        .unwrap()
    }

    fn api_error(status: u16, error: &str) -> ClientError {
        ClientError::Api {
            status,
            body: ErrorBody::new(error),
        }
    }

    /// Answers from a script; once the last answer is given the watch is told
    /// to stop. Records the `reveal` of every request.
    struct Script {
        answers: Mutex<VecDeque<Result<WorkloadResponse, ClientError>>>,
        reveals: Mutex<Vec<bool>>,
        done: Notify,
    }

    impl Script {
        fn new(answers: Vec<Result<WorkloadResponse, ClientError>>) -> Self {
            Self {
                answers: Mutex::new(answers.into()),
                reveals: Mutex::default(),
                done: Notify::new(),
            }
        }
    }

    impl WorkloadSource for Script {
        async fn get(&self, reveal: bool) -> Result<WorkloadResponse, ClientError> {
            self.reveals.lock().unwrap().push(reveal);
            let mut answers = self.answers.lock().unwrap();
            let next = answers.pop_front().expect("polled past the script");
            if answers.is_empty() {
                self.done.notify_one();
            }
            next
        }
    }

    #[derive(Default)]
    struct Sleeps(Mutex<Vec<Duration>>);

    impl Sleeper for Sleeps {
        async fn sleep(&self, duration: Duration) {
            self.0.lock().unwrap().push(duration);
        }
    }

    struct Run {
        result: anyhow::Result<()>,
        out: String,
        err: String,
        reveals: Vec<bool>,
        sleeps: Vec<Duration>,
    }

    async fn run(
        answers: Vec<Result<WorkloadResponse, ClientError>>,
        format: OutputFormat,
        clear: bool,
    ) -> Run {
        let script = Script::new(answers);
        let sleeps = Sleeps::default();
        let describe = |e: ClientError| anyhow::anyhow!("{e}");
        let opts = Options {
            interval: Duration::from_secs(2),
            format,
            clear,
            describe: &describe,
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let result = watch(
            &script,
            &sleeps,
            &opts,
            script.done.notified(),
            &mut out,
            &mut err,
        )
        .await;
        Run {
            result,
            out: String::from_utf8(out).unwrap(),
            err: String::from_utf8(err).unwrap(),
            reveals: script.reveals.into_inner().unwrap(),
            sleeps: sleeps.0.into_inner().unwrap(),
        }
    }

    #[tokio::test]
    async fn it_draws_again_only_when_the_view_changes() {
        let a = [(0, 1, "available")];
        let b = [(0, 1, "unavailable")];
        let got = run(
            vec![
                Ok(view(1, &a, true)),
                Ok(view(1, &a, false)),
                Ok(view(1, &b, false)),
                Ok(view(1, &b, false)),
            ],
            OutputFormat::Text,
            false,
        )
        .await;
        got.result.unwrap();
        let first = output::workload(OutputFormat::Text, &view(1, &a, true)).unwrap();
        let second = output::workload(OutputFormat::Text, &view(1, &b, true)).unwrap();
        // Separated by a blank line, and the unchanged polls draw nothing.
        assert_eq!(got.out, format!("{first}\n{second}"));
        assert!(got.out.contains("KEY=secret1"), "{}", got.out);
        assert!(!got.out.contains("***"));
        assert_eq!(got.err, "");
        // Stopped before the sleep after the last answer.
        assert_eq!(got.sleeps, vec![Duration::from_secs(2); 3]);
    }

    #[tokio::test]
    async fn on_a_terminal_each_view_clears_the_screen_first() {
        let a = [(0, 1, "available")];
        let b = [(0, 1, "unavailable")];
        let got = run(
            vec![Ok(view(1, &a, true)), Ok(view(1, &b, false))],
            OutputFormat::Text,
            true,
        )
        .await;
        got.result.unwrap();
        assert_eq!(got.out.matches(CLEAR_SCREEN).count(), 2);
        assert!(got.out.starts_with(CLEAR_SCREEN));
        assert!(!got.out.contains("\n\n"), "no blank line separator");
    }

    #[tokio::test]
    async fn the_env_is_opened_on_the_first_poll_and_when_the_revision_moves() {
        let a = [(0, 1, "available")];
        let rolling = [(0, 2, "deploying"), (0, 1, "available")];
        let done = [(0, 2, "available")];
        let got = run(
            vec![
                Ok(view(1, &a, true)),
                Ok(view(1, &a, false)),
                // The revision moved: asked again, this time opened.
                Ok(view(2, &rolling, false)),
                Ok(view(2, &rolling, true)),
                Ok(view(2, &done, false)),
            ],
            OutputFormat::Text,
            false,
        )
        .await;
        got.result.unwrap();
        assert_eq!(got.reveals, [true, false, false, true, false]);
        assert!(got.out.contains("KEY=secret2"), "{}", got.out);
        assert!(!got.out.contains("***"));
        // The roll shows both revisions.
        assert_eq!(got.out.matches("deploying").count(), 1);
    }

    #[tokio::test]
    async fn json_is_one_compact_object_per_change() {
        let a = [(0, 1, "available")];
        let b = [(0, 1, "unavailable")];
        let got = run(
            vec![
                Ok(view(1, &a, true)),
                Ok(view(1, &a, false)),
                Ok(view(1, &b, false)),
            ],
            OutputFormat::Json,
            false,
        )
        .await;
        got.result.unwrap();
        let lines: Vec<_> = got.out.lines().collect();
        assert_eq!(lines.len(), 2, "{}", got.out);
        for (line, want) in lines.iter().zip([&a, &b]) {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(v, serde_json::to_value(view(1, want, true)).unwrap());
        }
        assert!(got.out.ends_with('\n'));
    }

    #[tokio::test]
    async fn a_client_error_ends_the_watch_with_that_error() {
        let a = [(0, 1, "available")];
        let got = run(
            vec![Ok(view(1, &a, true)), Err(api_error(404, "not_found"))],
            OutputFormat::Text,
            false,
        )
        .await;
        let e = got.result.unwrap_err();
        assert!(e.to_string().contains("not_found"), "{e}");
        assert!(got.out.contains("name:"));
        assert_eq!(got.err, "");
        // No sleep after the failure.
        assert_eq!(got.sleeps.len(), 1);
    }

    #[tokio::test]
    async fn a_server_error_is_noted_and_tried_again() {
        let a = [(0, 1, "available")];
        let got = run(
            vec![
                Err(api_error(503, "unavailable")),
                Err(api_error(500, "internal")),
                Ok(view(1, &a, true)),
            ],
            OutputFormat::Text,
            false,
        )
        .await;
        got.result.unwrap();
        assert_eq!(got.err.lines().count(), 2, "{}", got.err);
        assert!(got.err.contains("unavailable") && got.err.contains("retrying"));
        assert_eq!(got.out.matches("name:").count(), 1);
        // Nothing was opened yet, so the next poll still asks for it.
        assert_eq!(got.reveals, [true, true, true]);
    }

    #[tokio::test]
    async fn an_error_in_between_keeps_the_view_and_the_opened_env() {
        let a = [(0, 1, "available")];
        let got = run(
            vec![
                Ok(view(1, &a, true)),
                Err(api_error(502, "bad_gateway")),
                Ok(view(1, &a, false)),
            ],
            OutputFormat::Text,
            false,
        )
        .await;
        got.result.unwrap();
        assert_eq!(got.out.matches("name:").count(), 1);
        assert_eq!(got.reveals, [true, false, false]);
    }

    #[tokio::test]
    async fn a_stop_during_a_poll_ends_the_watch_cleanly() {
        struct Never;
        impl WorkloadSource for Never {
            async fn get(&self, _: bool) -> Result<WorkloadResponse, ClientError> {
                std::future::pending().await
            }
        }
        let describe = |e: ClientError| anyhow::anyhow!("{e}");
        let opts = Options {
            interval: Duration::from_secs(2),
            format: OutputFormat::Text,
            clear: false,
            describe: &describe,
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        watch(
            &Never,
            &Sleeps::default(),
            &opts,
            std::future::ready(()),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert!(out.is_empty() && err.is_empty());
    }
}
