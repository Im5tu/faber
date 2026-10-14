//! Argument parsing tests. None of these touch the network.

use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use faber::{
    AppCommand, Cli, ClusterCommand, Command, DeployCommand, DeployOptions, OutputFormat,
    ProjectCommand, ProjectEnvCommand, Target,
};

fn parse(args: &[&str]) -> Cli {
    let mut full = vec!["faber"];
    full.extend_from_slice(args);
    Cli::try_parse_from(full).unwrap_or_else(|e| panic!("parse {args:?}: {e}"))
}

fn parse_err(args: &[&str]) -> String {
    let mut full = vec!["faber"];
    full.extend_from_slice(args);
    match Cli::try_parse_from(full) {
        Ok(_) => panic!("expected {args:?} to fail"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn command_definition_is_valid() {
    Cli::command().debug_assert();
}

#[test]
fn endpoint_defaults_to_local_default_port() {
    // Only meaningful when FABER_ENDPOINT is unset in the test environment.
    if std::env::var_os("FABER_ENDPOINT").is_some() {
        return;
    }
    let cli = parse(&["health"]);
    assert_eq!(
        cli.endpoint,
        format!("http://127.0.0.1:{}", faber_api_types::DEFAULT_PORT)
    );
    assert_eq!(cli.output, OutputFormat::Text);
}

#[test]
fn endpoint_reads_faber_endpoint_env() {
    let cmd = Cli::command();
    let arg = cmd
        .get_arguments()
        .find(|a| a.get_id() == "endpoint")
        .expect("endpoint arg");
    assert_eq!(arg.get_env(), Some(std::ffi::OsStr::new("FABER_ENDPOINT")));
}

#[test]
fn global_flags_short_and_long() {
    let cli = parse(&["-e", "http://h:1", "-o", "json", "health"]);
    assert_eq!(cli.endpoint, "http://h:1");
    assert_eq!(cli.output, OutputFormat::Json);

    let cli = parse(&["health", "--endpoint", "http://h:2", "--output", "text"]);
    assert_eq!(cli.endpoint, "http://h:2");
    assert_eq!(cli.output, OutputFormat::Text);
}

#[test]
fn invalid_output_format_is_rejected() {
    parse_err(&["-o", "yaml", "health"]);
}

#[test]
fn health_parses() {
    assert_eq!(parse(&["health"]).command, Command::Health);
}

#[test]
fn cluster_subcommands_parse() {
    let cases: Vec<(Vec<&str>, Command)> = vec![
        (
            vec!["cluster", "status"],
            Command::Cluster(ClusterCommand::Status),
        ),
        (
            vec!["cluster", "init"],
            Command::Cluster(ClusterCommand::Init),
        ),
        (
            vec!["cluster", "join", "10.8.0.1:7401"],
            Command::Cluster(ClusterCommand::Join {
                peer: "10.8.0.1:7401".parse().unwrap(),
            }),
        ),
        (
            vec!["cluster", "join", "[fd00::1]:7401"],
            Command::Cluster(ClusterCommand::Join {
                peer: "[fd00::1]:7401".parse().unwrap(),
            }),
        ),
    ];
    for (args, want) in cases {
        assert_eq!(parse(&args).command, want, "args {args:?}");
    }
}

#[test]
fn join_requires_an_ip_and_port() {
    parse_err(&["cluster", "join"]);
    for bad in [
        "10.8.0.1",
        "node1:7401",
        "http://10.8.0.1:7401",
        "10.8.0.1:x",
    ] {
        parse_err(&["cluster", "join", bad]);
    }
}

#[test]
fn removed_commands_are_rejected() {
    parse_err(&["kv", "get", "a"]);
    parse_err(&["cluster", "add-learner", "--id", "2", "--addr", "h:1"]);
    parse_err(&["cluster", "members", "--set", "1,2"]);
    parse_err(&["cluster", "init", "--node", "1=h:1"]);
}

#[test]
fn deploy_compose_and_k8s_are_rejected() {
    for kind in ["compose", "k8s"] {
        let message = parse_err(&["deploy", kind, "--project", "shop", "--env", "staging"]);
        assert!(message.contains("unrecognized subcommand"), "{message}");
    }
}

#[test]
fn missing_subcommand_is_rejected() {
    parse_err(&[]);
    parse_err(&["cluster"]);
}

#[test]
fn mesh_invite_parses() {
    use faber::MeshCommand;
    let cli = parse(&[
        "mesh",
        "invite",
        "--name",
        "node-2",
        "--wg-endpoint",
        "198.51.100.2",
    ]);
    assert_eq!(
        cli.command,
        Command::Mesh(MeshCommand::Invite {
            name: "node-2".into(),
            wg_endpoint: "198.51.100.2".into(),
            ttl: None,
        })
    );
    for (ttl, secs) in [
        ("90", 90),
        ("90s", 90),
        ("15m", 900),
        ("1h", 3600),
        ("7d", 604_800),
    ] {
        let cli = parse(&[
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "[fd00::2]:51000",
            "--ttl",
            ttl,
        ]);
        assert_eq!(
            cli.command,
            Command::Mesh(MeshCommand::Invite {
                name: "node-2".into(),
                wg_endpoint: "[fd00::2]:51000".into(),
                ttl: Some(secs),
            }),
            "ttl {ttl}"
        );
    }
}

#[test]
fn mesh_lease_parses() {
    use faber::MeshCommand;
    assert_eq!(
        parse(&["mesh", "lease"]).command,
        Command::Mesh(MeshCommand::Lease { set: None })
    );
    for (duration, secs) in [
        ("600", 600),
        ("600s", 600),
        ("45m", 2700),
        ("8h", 28_800),
        ("30d", 2_592_000),
        // Outside the server's bounds: left for the server to refuse.
        ("1s", 1),
        ("31d", 2_678_400),
    ] {
        assert_eq!(
            parse(&["mesh", "lease", "--set", duration]).command,
            Command::Mesh(MeshCommand::Lease { set: Some(secs) }),
            "duration {duration}"
        );
    }
}

#[test]
fn mesh_lease_requires_a_sane_duration() {
    parse_err(&["mesh", "lease", "--set"]);
    parse_err(&["mesh", "lease", "8h"]);
    for bad in [
        "",
        "0",
        "0h",
        "1w",
        "h",
        "-1h",
        "1.5h",
        "99999999999999999999d",
    ] {
        parse_err(&["mesh", "lease", "--set", bad]);
    }
}

#[test]
fn mesh_lease_help_explains_the_lease() {
    let mut cmd = Cli::command();
    let lease = cmd
        .find_subcommand_mut("mesh")
        .and_then(|mesh| mesh.find_subcommand_mut("lease"))
        .expect("mesh lease subcommand");
    let help = lease.render_long_help().to_string();
    let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
    for want in [
        "without renewing before the cluster removes it",
        "Each renewal restarts",
        "restarts every node's lease",
    ] {
        assert!(help.contains(want), "missing {want:?} in: {help}");
    }
}

#[test]
fn mesh_invite_requires_name_endpoint_and_a_sane_ttl() {
    parse_err(&["mesh", "invite"]);
    parse_err(&["mesh", "invite", "--wg-endpoint", "198.51.100.2"]);
    parse_err(&["mesh", "invite", "--name", "node-2"]);
    parse_err(&["mesh"]);
    for bad in [
        "",
        "0",
        "0h",
        "1w",
        "h",
        "-1h",
        "1.5h",
        "99999999999999999999d",
    ] {
        parse_err(&[
            "mesh",
            "invite",
            "--name",
            "node-2",
            "--wg-endpoint",
            "198.51.100.2",
            "--ttl",
            bad,
        ]);
    }
}

// ---- project, deploy, app ----

fn target(project: &str, environment: &str) -> Target {
    Target {
        project: project.into(),
        environment: environment.into(),
    }
}

fn no_options() -> DeployOptions {
    DeployOptions {
        expose: vec![],
        prune: false,
        dry_run: false,
        strict: false,
    }
}

#[test]
fn project_subcommands_parse() {
    let cases: Vec<(Vec<&str>, ProjectCommand)> = vec![
        (
            vec!["project", "create", "shop"],
            ProjectCommand::Create {
                name: "shop".into(),
                environments: vec![],
            },
        ),
        (
            vec!["project", "create", "shop", "--env", "dev", "--env", "prod"],
            ProjectCommand::Create {
                name: "shop".into(),
                environments: vec!["dev".into(), "prod".into()],
            },
        ),
        (vec!["project", "ls"], ProjectCommand::Ls),
        (
            vec!["project", "show", "shop"],
            ProjectCommand::Show {
                name: "shop".into(),
            },
        ),
        (
            vec!["project", "rm", "shop"],
            ProjectCommand::Rm {
                name: "shop".into(),
            },
        ),
        (
            vec!["project", "env", "add", "shop", "dev"],
            ProjectCommand::Env(ProjectEnvCommand::Add {
                project: "shop".into(),
                environment: "dev".into(),
            }),
        ),
        (
            vec!["project", "env", "rm", "shop", "dev"],
            ProjectCommand::Env(ProjectEnvCommand::Rm {
                project: "shop".into(),
                environment: "dev".into(),
            }),
        ),
    ];
    for (args, want) in cases {
        assert_eq!(
            parse(&args).command,
            Command::Project(want),
            "args {args:?}"
        );
    }
}

#[test]
fn project_subcommands_require_their_names() {
    for bad in [
        &["project"][..],
        &["project", "create"],
        &["project", "create", "--env", "dev"],
        &["project", "create", "shop", "--env"],
        &["project", "create", "shop", "extra"],
        &["project", "ls", "shop"],
        &["project", "show"],
        &["project", "rm"],
        &["project", "env"],
        &["project", "env", "add", "shop"],
        &["project", "env", "rm", "shop"],
        &["project", "env", "add", "shop", "dev", "extra"],
    ] {
        parse_err(bad);
    }
}

#[test]
fn deploy_spec_takes_one_file_or_stdin() {
    for file in ["spec.json", "-"] {
        assert_eq!(
            parse(&[
                "deploy",
                "spec",
                file,
                "--project",
                "shop",
                "--env",
                "staging",
                "--dry-run",
            ])
            .command,
            Command::Deploy(DeployCommand::Spec {
                file: PathBuf::from(file),
                target: target("shop", "staging"),
                options: DeployOptions {
                    dry_run: true,
                    ..no_options()
                },
            }),
            "file {file}"
        );
    }
    parse_err(&["deploy", "spec", "--project", "shop", "--env", "staging"]);
    parse_err(&[
        "deploy",
        "spec",
        "a.json",
        "b.json",
        "--project",
        "shop",
        "--env",
        "staging",
    ]);
    parse_err(&["deploy"]);
    parse_err(&["deploy", "helm", "chart"]);
}

/// No deploy has a default project or environment.
#[test]
fn every_deploy_requires_project_and_env() {
    for file in ["s.json", "-"] {
        let kind = ["spec", file];
        let mut args = vec!["deploy"];
        args.extend_from_slice(&kind);

        let mut without_env = args.clone();
        without_env.extend_from_slice(&["--project", "shop"]);
        let message = parse_err(&without_env);
        assert!(message.contains("--env <ENV>"), "{kind:?}: {message}");

        let mut without_project = args.clone();
        without_project.extend_from_slice(&["--env", "staging"]);
        let message = parse_err(&without_project);
        assert!(
            message.contains("--project <PROJECT>"),
            "{kind:?}: {message}"
        );
        parse_err(&args);
    }
}

#[test]
fn app_subcommands_parse() {
    assert_eq!(
        parse(&["app", "ls", "--project", "shop", "--env", "staging"]).command,
        Command::App(AppCommand::Ls {
            project: Some("shop".into()),
            environment: Some("staging".into()),
        })
    );
    for show_env in [false, true] {
        let mut args = vec![
            "app",
            "show",
            "web",
            "--project",
            "shop",
            "--env",
            "staging",
        ];
        if show_env {
            args.push("--show-env");
        }
        assert_eq!(
            parse(&args).command,
            Command::App(AppCommand::Show {
                workload: "web".into(),
                target: target("shop", "staging"),
                show_env,
            })
        );
    }
    assert_eq!(
        parse(&[
            "app",
            "rm",
            "web",
            "db",
            "--project",
            "shop",
            "--env",
            "staging"
        ])
        .command,
        Command::App(AppCommand::Rm {
            workloads: vec!["web".into(), "db".into()],
            target: target("shop", "staging"),
        })
    );
}

#[test]
fn app_ls_filters_are_optional() {
    assert_eq!(
        parse(&["app", "ls", "--project", "shop"]).command,
        Command::App(AppCommand::Ls {
            project: Some("shop".into()),
            environment: None,
        })
    );
    assert_eq!(
        parse(&["app", "ls"]).command,
        Command::App(AppCommand::Ls {
            project: None,
            environment: None,
        })
    );
    assert_eq!(
        parse(&["app", "ls", "--env", "staging"]).command,
        Command::App(AppCommand::Ls {
            project: None,
            environment: Some("staging".into()),
        })
    );
}

#[test]
fn app_show_and_rm_require_a_workload_project_and_env() {
    let both = ["--project", "shop", "--env", "staging"];
    parse_err(&["app"]);
    parse_err(&[&["app", "show"][..], &both].concat());
    parse_err(&[&["app", "rm"][..], &both].concat());
    parse_err(&[&["app", "show", "web", "db"][..], &both].concat());
    // --show-env belongs to show.
    parse_err(&[&["app", "rm", "web", "--show-env"][..], &both].concat());
    for sub in ["show", "rm"] {
        let message = parse_err(&["app", sub, "web", "--project", "shop"]);
        assert!(message.contains("--env <ENV>"), "{sub}: {message}");
        let message = parse_err(&["app", sub, "web", "--env", "staging"]);
        assert!(message.contains("--project <PROJECT>"), "{sub}: {message}");
    }
}

/// `--project` and `--env` are always given on the command line: neither
/// reads a variable, and neither has a short flag, so `-e` stays the
/// endpoint.
#[test]
fn project_and_env_read_no_variable_and_have_no_short_flag() {
    fn check(cmd: &clap::Command, path: &str, seen: &mut (usize, usize)) {
        for arg in cmd.get_arguments() {
            let long = arg.get_long();
            if long == Some("project") || long == Some("env") {
                if long == Some("project") {
                    seen.0 += 1;
                } else {
                    seen.1 += 1;
                }
                assert_eq!(arg.get_env(), None, "{path} --{long:?}");
                assert_eq!(arg.get_short(), None, "{path} --{long:?}");
            }
        }
        for sub in cmd.get_subcommands() {
            check(sub, &format!("{path} {}", sub.get_name()), seen);
        }
    }
    let mut seen = (0, 0);
    check(&Cli::command(), "faber", &mut seen);
    // deploy spec, and app ls, show and rm. `--env` is
    // also on project create.
    assert_eq!(seen, (4, 5));
}

/// A `FABER_PROJECT` in the environment is not a project.
#[test]
fn no_help_text_mentions_faber_project() {
    fn check(cmd: &mut clap::Command) {
        let help = cmd.render_long_help().to_string();
        assert!(!help.contains("FABER_PROJECT"), "{help}");
        for sub in cmd.get_subcommands_mut() {
            check(sub);
        }
    }
    check(&mut Cli::command());
}

#[test]
fn short_e_is_still_the_endpoint_on_the_new_commands() {
    let cli = parse(&[
        "deploy",
        "spec",
        "s.json",
        "-e",
        "http://h:1",
        "--project",
        "shop",
        "--env",
        "staging",
    ]);
    assert_eq!(cli.endpoint, "http://h:1");
    assert_eq!(
        cli.command,
        Command::Deploy(DeployCommand::Spec {
            file: PathBuf::from("s.json"),
            target: target("shop", "staging"),
            options: no_options(),
        })
    );

    let cli = parse(&["app", "ls", "-e", "http://h:2", "--env", "staging"]);
    assert_eq!(cli.endpoint, "http://h:2");
    let cli = parse(&[
        "project",
        "create",
        "shop",
        "-e",
        "http://h:3",
        "--env",
        "dev",
    ]);
    assert_eq!(cli.endpoint, "http://h:3");
    assert_eq!(
        cli.command,
        Command::Project(ProjectCommand::Create {
            name: "shop".into(),
            environments: vec!["dev".into()],
        })
    );
}

#[test]
fn show_env_help_says_json_is_not_masked() {
    let mut cmd = Cli::command();
    let show = cmd
        .find_subcommand_mut("app")
        .and_then(|app| app.find_subcommand_mut("show"))
        .expect("app show subcommand");
    let help = show.render_long_help().to_string();
    let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        help.contains("JSON output (-o json) is never masked"),
        "{help}"
    );
}
