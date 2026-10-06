//! The converter's tables: one row per row of the contract.

use serde_json::json;

use super::*;

/// What one row of a table expects. Paths are relative to the part of the
/// manifest the row's snippet goes into.
enum Want {
    /// No finding, and this value at this JSON pointer of the spec.
    Mapped(&'static str, Value),
    /// The same, with one warning at the path.
    MappedWarn(&'static str, Value, &'static str),
    /// No finding, and the spec is the one without the snippet.
    Ignored,
    /// One warning at the path, and the spec is the one without the
    /// snippet.
    Warn(&'static str),
    /// Exactly one error, at the path.
    Error(&'static str),
}
use Want::{Error, Ignored, Mapped, MappedWarn, Warn};

const POD: &str = "spec.template.spec";
const CONTAINER: &str = "spec.template.spec.containers[0]";

/// A Deployment `web` with one container `app`, plus what the rows add to
/// its pod spec and to its container.
fn deployment(pod: &Value, container: &Value) -> Value {
    let mut doc = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "web"},
        "spec": {"template": {"spec": {
            "containers": [{"name": "app", "image": "nginx"}],
        }}},
    });
    let spec = &mut doc["spec"]["template"]["spec"];
    merge(&mut spec["containers"][0], container);
    merge(spec, pod);
    doc
}

fn plain() -> Value {
    deployment(&json!({}), &json!({}))
}

fn merge(target: &mut Value, snippet: &Value) {
    for (key, value) in snippet.as_object().into_iter().flatten() {
        target[key] = value.clone();
    }
}

/// The ConfigMaps every row may refer to. None of them causes a finding
/// by being there.
fn config_maps() -> Vec<Value> {
    vec![
        json!({"kind": "ConfigMap", "metadata": {"name": "plain"},
               "data": {"A": "1", "B": "2"}}),
        json!({"kind": "ConfigMap", "metadata": {"name": "other"},
               "data": {"A": "9"}}),
        json!({"kind": "ConfigMap", "metadata": {"name": "odd"},
               "data": {"MODE": "prod", "not a name": "x"},
               "binaryData": {"blob": "AAEC"}}),
    ]
}

fn stream(source: &str, docs: &[Value]) -> SourceDocument {
    let text = docs
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n---\n");
    SourceDocument {
        source: source.to_string(),
        text,
    }
}

/// Converts the values as one stream named `m.yaml`. JSON is YAML.
fn run(docs: &[Value]) -> Result<Conversion, ConvertError> {
    convert(&[stream("m.yaml", docs)])
}

fn run_yaml(text: &str) -> Result<Conversion, ConvertError> {
    convert(&[SourceDocument {
        source: "m.yaml".to_string(),
        text: text.to_string(),
    }])
}

fn paths(found: &[Diagnostic]) -> Vec<String> {
    found.iter().map(|d| d.path.clone()).collect()
}

fn messages(found: &[Diagnostic]) -> Vec<String> {
    found.iter().map(ToString::to_string).collect()
}

fn join(prefix: &str, path: &str) -> String {
    match path {
        "" => prefix.to_string(),
        _ if prefix.is_empty() => path.to_string(),
        _ => format!("{prefix}.{path}"),
    }
}

/// Checks one row: `doc` against the spec of `plain`, the same manifest
/// without the row's snippet.
fn check(label: &str, doc: Value, plain: Value, prefix: &str, want: &Want) {
    let mut docs = config_maps();
    docs.push(plain);
    let plain = run(&docs).unwrap_or_else(|e| panic!("{label}: plain: {e}"));
    let plain = serde_json::to_value(&plain.workloads["web"]).unwrap();
    *docs.last_mut().unwrap() = doc;
    let result = run(&docs);

    if let Error(path) = want {
        let err = match result {
            Err(err) => err,
            Ok(done) => panic!("{label}: no error, got {:?}", done.workloads),
        };
        assert_eq!(
            paths(&err.errors),
            [join(prefix, path)],
            "{label}: {:?}",
            messages(&err.errors)
        );
        return;
    }
    let done = result.unwrap_or_else(|e| panic!("{label}: {e}"));
    let spec = serde_json::to_value(&done.workloads["web"]).unwrap();
    let warned = paths(&done.warnings);
    let no_warning: [String; 0] = [];
    match want {
        Mapped(pointer, value) => {
            assert_eq!(warned, no_warning, "{label}");
            assert_eq!(spec.pointer(pointer), Some(value), "{label}: {spec}");
        }
        MappedWarn(pointer, value, path) => {
            assert_eq!(warned, [join(prefix, path)], "{label}");
            assert_eq!(spec.pointer(pointer), Some(value), "{label}: {spec}");
        }
        Ignored => {
            assert_eq!(warned, no_warning, "{label}");
            assert_eq!(spec, plain, "{label}");
        }
        Warn(path) => {
            assert_eq!(warned, [join(prefix, path)], "{label}");
            assert_eq!(spec, plain, "{label}");
        }
        Error(_) => unreachable!(),
    }
}

/// Rows whose snippets go into the pod spec and into the container. Paths
/// are relative to the pod spec.
fn check_pod_rows(rows: Vec<(Value, Value, Want)>) {
    for (pod, container, want) in rows {
        let label = format!("pod {pod} container {container}");
        check(&label, deployment(&pod, &container), plain(), POD, &want);
    }
}

/// Rows whose snippet goes into the container. Paths are relative to the
/// container.
fn check_container_rows(rows: Vec<(Value, Want)>) {
    for (container, want) in rows {
        let label = container.to_string();
        let doc = deployment(&json!({}), &container);
        check(&label, doc, plain(), CONTAINER, &want);
    }
}

fn probe_json(action: Value) -> Value {
    json!({
        "action": action,
        "initial_delay_secs": 0, "period_secs": 10, "timeout_secs": 1,
        "success_threshold": 1, "failure_threshold": 3,
    })
}

#[test]
fn expansion_follows_the_kubelet() {
    let env = BTreeMap::from([
        ("A".to_string(), "1".to_string()),
        ("EMPTY".to_string(), String::new()),
        ("my.var-x".to_string(), "dots".to_string()),
    ]);
    for (input, want) in [
        ("", ""),
        ("plain", "plain"),
        ("$(A)", "1"),
        ("x$(A)y$(A)", "x1y1"),
        ("$(EMPTY)", ""),
        ("$(my.var-x)", "dots"),
        // Not set: stays as written.
        ("$(B)", "$(B)"),
        ("$()", "$()"),
        // Escaped.
        ("$$(A)", "$(A)"),
        ("$$$(A)", "$1"),
        ("$$$$(A)", "$$(A)"),
        ("$$", "$"),
        // Never closed, or no reference at all.
        ("$(A", "$(A"),
        ("$A", "$A"),
        ("${A}", "${A}"),
        ("$", "$"),
        ("cost: 5$", "cost: 5$"),
        ("$ (A)", "$ (A)"),
        // The first `)` closes, and nothing is expanded twice.
        ("$(A))", "1)"),
        ("$($(A))", "$($(A))"),
        ("é$(A)é", "é1é"),
    ] {
        assert_eq!(expand(input, &env).as_deref(), Some(want), "{input:?}");
    }
    // A value is not expanded again.
    let nested = BTreeMap::from([("A".to_string(), "$(A)".to_string())]);
    assert_eq!(expand("$(A)", &nested).as_deref(), Some("$(A)"));
}

/// One string may grow to 64 KiB and no further, and a string of
/// references that never close is read once, not once per reference.
#[test]
fn expansion_is_bounded() {
    let env = BTreeMap::from([("A".to_string(), "x".repeat(1024))]);
    assert_eq!(
        expand(&"$(A)".repeat(64), &env).map(|s| s.len()),
        Some(65_536)
    );
    assert_eq!(expand(&"$(A)".repeat(65), &env), None);
    assert_eq!(expand(&format!("{}y", "$(A)".repeat(64)), &env), None);
    // No reference, and still too long.
    assert_eq!(expand(&"y".repeat(65_537), &env), None);

    let unclosed = "$(".repeat(30_000);
    let started = std::time::Instant::now();
    assert_eq!(expand(&unclosed, &env).as_deref(), Some(unclosed.as_str()));
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn a_string_that_expands_past_the_limit_is_an_error_at_its_path() {
    let big = "x".repeat(30_000);
    let container = json!({
        "env": [
            {"name": "A", "value": big},
            {"name": "B", "value": "$(A)$(A)"},
            {"name": "C", "value": "$(A)$(A)$(A)"},
        ],
        "command": ["run", "$(B)$(B)"],
        "args": ["$(A)", "$(B)$(A)"],
    });
    let err = run(&[deployment(&json!({}), &container)]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml: spec.template.spec.containers[0].env[2].value: \
             is longer than 64 KiB once `$(VAR)` references are replaced",
            "m.yaml: spec.template.spec.containers[0].command[1]: \
             is longer than 64 KiB once `$(VAR)` references are replaced",
            "m.yaml: spec.template.spec.containers[0].args[1]: \
             is longer than 64 KiB once `$(VAR)` references are replaced",
        ]
    );
}

#[test]
fn container_fields() {
    check_container_rows(vec![
        (
            json!({"workingDir": "/srv"}),
            Mapped("/containers/0/working_dir", json!("/srv")),
        ),
        // One directory has one spelling in the model.
        (
            json!({"workingDir": "/srv/"}),
            Mapped("/containers/0/working_dir", json!("/srv")),
        ),
        (
            json!({"workingDir": "//srv/./app//"}),
            Mapped("/containers/0/working_dir", json!("/srv/app")),
        ),
        (
            json!({"workingDir": "/"}),
            Mapped("/containers/0/working_dir", json!("/")),
        ),
        (
            json!({"name": "api"}),
            Mapped("/containers/0/name", json!("api")),
        ),
        (
            json!({"image": "redis:7"}),
            Mapped("/containers/0/image", json!("redis:7")),
        ),
        (json!({"name": "My_App"}), Error("name")),
        (json!({"name": null}), Error("")),
        (json!({"image": null}), Error("")),
        (
            json!({"command": ["sh", "-c"]}),
            Mapped("/containers/0/command", json!(["sh", "-c"])),
        ),
        (
            json!({"args": ["--port", "80"]}),
            Mapped("/containers/0/args", json!(["--port", "80"])),
        ),
        (json!({"args": ["--port", 80]}), Error("args[1]")),
        (
            json!({"command": [], "args": [], "env": [], "ports": [], "volumeMounts": []}),
            Ignored,
        ),
        // A key in no table is refused, not dropped.
        (json!({"shiny": 1}), Error("shiny")),
        (
            json!({"lifecycle": {"preStop": {"exec": {"command": ["x"]}}}}),
            Error("lifecycle"),
        ),
        (json!({"lifecycle": {}}), Ignored),
        (
            json!({"volumeDevices": [{"name": "d", "devicePath": "/dev/x"}]}),
            Error("volumeDevices"),
        ),
        (json!({"restartPolicy": "Always"}), Error("restartPolicy")),
        (json!({"stdin": true}), Error("stdin")),
        (json!({"stdinOnce": true}), Error("stdinOnce")),
        (json!({"tty": true}), Error("tty")),
        (
            json!({"stdin": false, "stdinOnce": false, "tty": false}),
            Ignored,
        ),
        (json!({"imagePullPolicy": "IfNotPresent"}), Ignored),
        (
            json!({"imagePullPolicy": "Always"}),
            Warn("imagePullPolicy"),
        ),
        (json!({"imagePullPolicy": "Never"}), Warn("imagePullPolicy")),
        (
            json!({"terminationMessagePath": "/dev/termination-log"}),
            Ignored,
        ),
        (json!({"terminationMessagePolicy": "File"}), Ignored),
        (
            json!({"resizePolicy": [{"resourceName": "cpu", "restartPolicy": "NotRequired"}]}),
            Ignored,
        ),
    ]);
}

#[test]
fn container_env() {
    let key_ref = |map: &str, key: &str, optional: bool| {
        json!({"env": [{"name": "V", "valueFrom": {"configMapKeyRef":
            {"name": map, "key": key, "optional": optional}}}]})
    };
    let at_ref = "env[0].valueFrom.configMapKeyRef";
    let env = "/containers/0/env";
    check_container_rows(vec![
        (
            json!({"env": [{"name": "A", "value": "1"}]}),
            Mapped(env, json!({"A": "1"})),
        ),
        (
            json!({"env": [{"name": "A"}]}),
            Mapped(env, json!({"A": ""})),
        ),
        (
            json!({"env": [{"name": "A", "value": ""}]}),
            Mapped(env, json!({"A": ""})),
        ),
        (
            json!({"env": [{"name": "A", "value": 8080}]}),
            Error("env[0].value"),
        ),
        (
            json!({"env": [{"name": "A", "value": 1.5}]}),
            Error("env[0].value"),
        ),
        (
            json!({"env": [{"name": "A", "value": true}]}),
            Error("env[0].value"),
        ),
        (json!({"env": [{"value": "1"}]}), Error("env[0]")),
        (
            json!({"env": [{"name": "A", "value": "1"}, {"name": "A", "value": "2"}]}),
            MappedWarn(env, json!({"A": "2"}), "env[1].name"),
        ),
        (
            key_ref("odd", "MODE", false),
            Mapped(env, json!({"V": "prod"})),
        ),
        (key_ref("absent", "MODE", true), Warn(at_ref)),
        (key_ref("absent", "MODE", false), Error(at_ref)),
        (key_ref("odd", "NOPE", true), Warn(at_ref)),
        (key_ref("odd", "NOPE", false), Error(at_ref)),
        // `binaryData` is only a problem once something refers to it.
        (key_ref("odd", "blob", false), Error(at_ref)),
        (
            json!({"env": [{"name": "V", "value": "x", "valueFrom":
                {"configMapKeyRef": {"name": "odd", "key": "MODE"}}}]}),
            Error("env[0].valueFrom"),
        ),
        (
            json!({"env": [{"name": "V", "valueFrom":
                {"secretKeyRef": {"name": "s", "key": "k"}}}]}),
            Error("env[0].valueFrom.secretKeyRef"),
        ),
        (
            json!({"env": [{"name": "V", "valueFrom":
                {"fieldRef": {"fieldPath": "status.podIP"}}}]}),
            Error("env[0].valueFrom.fieldRef"),
        ),
        (
            json!({"env": [{"name": "V", "valueFrom":
                {"resourceFieldRef": {"resource": "limits.cpu"}}}]}),
            Error("env[0].valueFrom.resourceFieldRef"),
        ),
        (
            json!({"envFrom": [{"configMapRef": {"name": "plain"}}]}),
            Mapped(env, json!({"A": "1", "B": "2"})),
        ),
        (
            json!({"envFrom": [{"prefix": "CFG_", "configMapRef": {"name": "plain"}}]}),
            Mapped(env, json!({"CFG_A": "1", "CFG_B": "2"})),
        ),
        // A key that is no variable name is skipped, the rest is kept.
        (
            json!({"envFrom": [{"configMapRef": {"name": "odd"}}]}),
            MappedWarn(env, json!({"MODE": "prod"}), "envFrom[0].configMapRef"),
        ),
        // The prefix is part of the name that is checked.
        (
            json!({"envFrom": [{"prefix": "1", "configMapRef": {"name": "other"}}]}),
            Warn("envFrom[0].configMapRef"),
        ),
        (
            json!({"envFrom": [{"configMapRef": {"name": "absent", "optional": true}}]}),
            Warn("envFrom[0].configMapRef"),
        ),
        (
            json!({"envFrom": [{"configMapRef": {"name": "absent"}}]}),
            Error("envFrom[0].configMapRef"),
        ),
        (
            json!({"envFrom": [{"secretRef": {"name": "s"}}]}),
            Error("envFrom[0].secretRef"),
        ),
        // Later `envFrom` wins over earlier, and `env` wins over both.
        (
            json!({"envFrom": [{"configMapRef": {"name": "plain"}},
                               {"configMapRef": {"name": "other"}}]}),
            Mapped(env, json!({"A": "9", "B": "2"})),
        ),
        (
            json!({"envFrom": [{"configMapRef": {"name": "plain"}}],
                   "env": [{"name": "B", "value": "mine"}]}),
            Mapped(env, json!({"A": "1", "B": "mine"})),
        ),
    ]);
}

#[test]
fn the_secret_errors_say_there_is_no_secret_store() {
    for container in [
        json!({"env": [{"name": "V", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}}]}),
        json!({"envFrom": [{"secretRef": {"name": "s"}}]}),
    ] {
        let err = run(&[deployment(&json!({}), &container)]).unwrap_err();
        assert_eq!(
            err.errors[0].message,
            "secrets are not supported: faber has no secret store yet"
        );
    }
}

#[test]
fn container_expansion() {
    check_container_rows(vec![
        // A value sees `envFrom` and the variables declared before it.
        (
            json!({"envFrom": [{"configMapRef": {"name": "plain"}}],
            "env": [
                {"name": "FIRST", "value": "$(A)-$(LATER)"},
                {"name": "LATER", "value": "x"},
                {"name": "SECOND", "value": "$(FIRST)/$(LATER)"},
                {"name": "ESCAPED", "value": "$$(A)"},
            ]}),
            Mapped(
                "/containers/0/env",
                json!({
                    "A": "1", "B": "2",
                    "FIRST": "1-$(LATER)",
                    "LATER": "x",
                    "SECOND": "1-$(LATER)/x",
                    "ESCAPED": "$(A)",
                }),
            ),
        ),
        // A value taken from a ConfigMap is available to later ones.
        (
            json!({"env": [
                {"name": "M", "valueFrom": {"configMapKeyRef": {"name": "odd", "key": "MODE"}}},
                {"name": "URL", "value": "http://$(M)"},
            ]}),
            Mapped(
                "/containers/0/env",
                json!({"M": "prod", "URL": "http://prod"}),
            ),
        ),
        // `command` and `args` see the finished environment.
        (
            json!({"command": ["run", "$(LATER)"],
                   "env": [{"name": "LATER", "value": "x"}]}),
            Mapped("/containers/0/command", json!(["run", "x"])),
        ),
        (
            json!({"args": ["--a=$(A)", "--b=$(NOPE)", "$$(A)"],
                   "env": [{"name": "A", "value": "1"}]}),
            Mapped(
                "/containers/0/args",
                json!(["--a=1", "--b=$(NOPE)", "$(A)"]),
            ),
        ),
    ]);
}

#[test]
fn container_ports() {
    check_container_rows(vec![
        (
            json!({"ports": [{"containerPort": 8080}]}),
            Mapped(
                "/containers/0/ports",
                json!([{"port": 8080, "protocol": "tcp"}]),
            ),
        ),
        (
            json!({"ports": [{"containerPort": 53, "name": "dns", "protocol": "UDP"}]}),
            Mapped(
                "/containers/0/ports",
                json!([{"name": "dns", "port": 53, "protocol": "udp"}]),
            ),
        ),
        (
            json!({"ports": [{"containerPort": 80, "protocol": "TCP"}]}),
            Mapped("/containers/0/ports/0/protocol", json!("tcp")),
        ),
        (
            json!({"ports": [{"containerPort": 80, "hostPort": 8080}]}),
            MappedWarn("/containers/0/ports/0/port", json!(80), "ports[0].hostPort"),
        ),
        (
            json!({"ports": [{"containerPort": 80, "hostIP": "10.0.0.1"}]}),
            MappedWarn("/containers/0/ports/0/port", json!(80), "ports[0].hostIP"),
        ),
        (
            json!({"ports": [{"containerPort": 80, "protocol": "SCTP"}]}),
            Error("ports[0].protocol"),
        ),
        (
            json!({"ports": [{"containerPort": 0}]}),
            Error("ports[0].containerPort"),
        ),
        (
            json!({"ports": [{"containerPort": 65536}]}),
            Error("ports[0].containerPort"),
        ),
        (json!({"ports": [{"name": "http"}]}), Error("ports[0]")),
    ]);
}

#[test]
fn container_resources() {
    check_container_rows(vec![
        (
            json!({"resources": {"limits": {"cpu": "500m", "memory": "128Mi"}}}),
            Mapped(
                "/containers/0/resources",
                json!({
                    "limits": {"cpu_millis": 500, "memory_bytes": 134_217_728},
                    "requests": {},
                }),
            ),
        ),
        // A request with no limit is fine.
        (
            json!({"resources": {"requests": {"cpu": "250m", "memory": "64M"}}}),
            Mapped(
                "/containers/0/resources",
                json!({
                    "limits": {},
                    "requests": {"cpu_millis": 250, "memory_bytes": 64_000_000},
                }),
            ),
        ),
        // Unquoted numbers arrive as numbers.
        (
            json!({"resources": {"limits": {"cpu": 1, "memory": 1_073_741_824}}}),
            Mapped(
                "/containers/0/resources/limits",
                json!({"cpu_millis": 1000, "memory_bytes": 1_073_741_824}),
            ),
        ),
        (
            json!({"resources": {"limits": {"cpu": 0.5, "memory": 1e9}}}),
            Mapped(
                "/containers/0/resources/limits",
                json!({"cpu_millis": 500, "memory_bytes": 1_000_000_000}),
            ),
        ),
        (json!({"resources": {}}), Ignored),
        (
            json!({"resources": {"limits": {}, "requests": {}, "claims": []}}),
            Ignored,
        ),
        (
            json!({"resources": {"limits": {"memory": "128m"}}}),
            Error("resources.limits.memory"),
        ),
        // Zero is how Kubernetes writes "not set".
        (
            json!({"resources": {"limits": {"cpu": "0", "memory": 0}}}),
            Ignored,
        ),
        (
            json!({"resources": {"requests": {"cpu": 0.0, "memory": "0Mi"}}}),
            Ignored,
        ),
        (
            json!({"resources": {"limits": {"cpu": "0m", "memory": "0m"},
                                 "requests": {"cpu": "+0.00", "memory": "0e3"}}}),
            Ignored,
        ),
        (
            json!({"resources": {"limits": {"cpu": "0", "memory": "64Mi"}}}),
            Mapped(
                "/containers/0/resources/limits",
                json!({"memory_bytes": 67_108_864}),
            ),
        ),
        (
            json!({"resources": {"limits": {"cpu": "0x"}}}),
            Error("resources.limits.cpu"),
        ),
        (
            json!({"resources": {"limits": {"memory": "0MB"}}}),
            Error("resources.limits.memory"),
        ),
        (
            json!({"resources": {"limits": {"cpu": "-1"}}}),
            Error("resources.limits.cpu"),
        ),
        (
            json!({"resources": {"limits": {"cpu": true}}}),
            Error("resources.limits.cpu"),
        ),
        (
            json!({"resources": {"limits": {"ephemeral-storage": "1Gi"}}}),
            Warn("resources.limits.ephemeral-storage"),
        ),
        (
            json!({"resources": {"requests": {"ephemeral-storage": "1Gi"}}}),
            Warn("resources.requests.ephemeral-storage"),
        ),
        (
            json!({"resources": {"limits": {"nvidia.com/gpu": 1}}}),
            Error("resources.limits[\"nvidia.com/gpu\"]"),
        ),
        (
            json!({"resources": {"requests": {"hugepages-2Mi": "64Mi"}}}),
            Error("resources.requests.hugepages-2Mi"),
        ),
        (
            json!({"resources": {"claims": [{"name": "gpu"}]}}),
            Error("resources.claims"),
        ),
    ]);
}

/// Paths are written the model's way before its rules see them. What is
/// still wrong after that is the model's to refuse, in its own words.
#[test]
fn paths_are_normalised_and_then_checked() {
    let pod = |host: &str, mount: &str| {
        deployment(
            &json!({"volumes": [{"name": "data", "hostPath": {"path": host}}]}),
            &json!({"volumeMounts": [{"name": "data", "mountPath": mount}]}),
        )
    };
    let done = run(&[pod("/srv//data/./", "/usr/share/nginx/html/")]).unwrap();
    let spec = serde_json::to_value(&done.workloads["web"]).unwrap();
    assert_eq!(spec["volumes"]["data"]["path"], "/srv/data");
    assert_eq!(
        spec["containers"][0]["mounts"][0]["path"],
        "/usr/share/nginx/html"
    );

    let mount_at = "web.containers[0].mounts[0].path";
    for (host, mount, want) in [
        (
            "/srv/../etc",
            "/data",
            "web.volumes.data.path: must not contain . or .. components".to_string(),
        ),
        (
            "/srv",
            "/data/../etc",
            format!("{mount_at}: must not contain . or .. components"),
        ),
        (
            "/srv",
            "data/",
            format!("{mount_at}: must be an absolute path"),
        ),
        (
            "//",
            "/data",
            "web.volumes.data.path: must not be /".to_string(),
        ),
        ("/srv", "/", format!("{mount_at}: ")),
        ("/srv", "/proc/", format!("{mount_at}: ")),
        ("/srv", "/sys", format!("{mount_at}: ")),
        ("/srv", "/dev", format!("{mount_at}: ")),
    ] {
        let err = run(&[pod(host, mount)]).unwrap_err();
        let found = messages(&err.errors);
        assert_eq!(found.len(), 1, "{host} {mount}: {found:?}");
        assert!(
            found[0].starts_with(&format!("m.yaml: {want}")),
            "{found:?}"
        );
    }
    assert!(run(&[pod("/srv", "/dev/shm")]).is_ok());
}

#[test]
fn the_memory_suffix_m_gets_its_hint() {
    let container = json!({"resources": {"limits": {"memory": "128m"}}});
    let err = run(&[deployment(&json!({}), &container)]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml: spec.template.spec.containers[0].resources.limits.memory: \
          `128m` means 128 thousandths of a byte; did you mean `128Mi`?"
        ]
    );
}

#[test]
fn container_probes() {
    let http = |extra: Value| {
        let mut get = json!({"port": 8080});
        merge(&mut get, &extra);
        json!({"ports": [{"name": "http", "containerPort": 8080}],
               "readinessProbe": {"httpGet": get}})
    };
    let http_action = |extra: Value| {
        let mut action = json!({
            "type": "http", "port": 8080, "path": "/", "scheme": "http", "headers": {},
        });
        merge(&mut action, &extra);
        probe_json(action)
    };
    check_container_rows(vec![
        (
            json!({"livenessProbe": {"exec": {"command": ["cat", "/tmp/ok"]}}}),
            Mapped(
                "/containers/0/liveness",
                probe_json(json!({"type": "exec", "command": ["cat", "/tmp/ok"]})),
            ),
        ),
        (
            json!({"startupProbe": {"tcpSocket": {"port": 5432}}}),
            Mapped(
                "/containers/0/startup",
                probe_json(json!({"type": "tcp", "port": 5432})),
            ),
        ),
        (
            http(json!({})),
            Mapped("/containers/0/readiness", http_action(json!({}))),
        ),
        (
            http(json!({"path": "/ready", "scheme": "HTTPS",
                        "httpHeaders": [{"name": "X-Probe", "value": "1"},
                                        {"name": "host", "value": "web.local"}]})),
            Mapped(
                "/containers/0/readiness",
                http_action(json!({
                    "path": "/ready", "scheme": "https", "host": "web.local",
                    "headers": {"X-Probe": "1"},
                })),
            ),
        ),
        // The probe goes to the pod. Only the `Host` header can be set.
        (
            http(json!({"host": "10.0.0.1"})),
            Error("readinessProbe.httpGet.host"),
        ),
        (
            http(json!({"host": ""})),
            Mapped("/containers/0/readiness", http_action(json!({}))),
        ),
        (
            http(json!({"httpHeaders": [{"name": "Host", "value": "a"},
                                        {"name": "HOST", "value": "b"}]})),
            Error("readinessProbe.httpGet.httpHeaders[1].name"),
        ),
        // A named port becomes the container's port number.
        (
            http(json!({"port": "http"})),
            Mapped("/containers/0/readiness/action/port", json!(8080)),
        ),
        (
            http(json!({"port": "9090"})),
            Mapped("/containers/0/readiness/action/port", json!(9090)),
        ),
        (
            http(json!({"port": "metrics"})),
            Error("readinessProbe.httpGet.port"),
        ),
        (
            http(json!({"port": 0})),
            Error("readinessProbe.httpGet.port"),
        ),
        (
            http(json!({"scheme": "FTP"})),
            Error("readinessProbe.httpGet.scheme"),
        ),
        (
            http(json!({"httpHeaders": [{"name": "A", "value": "1"},
                                        {"name": "A", "value": "2"}]})),
            Error("readinessProbe.httpGet.httpHeaders[1].name"),
        ),
        (
            json!({"ports": [{"name": "db", "containerPort": 5432}],
                   "livenessProbe": {"tcpSocket": {"port": "db"}}}),
            Mapped(
                "/containers/0/liveness/action",
                json!({"type": "tcp", "port": 5432}),
            ),
        ),
        (
            json!({"livenessProbe": {"tcpSocket": {"port": 80, "host": "10.0.0.1"}}}),
            Error("livenessProbe.tcpSocket.host"),
        ),
        (
            json!({"livenessProbe": {"grpc": {"port": 9000}}}),
            Error("livenessProbe.grpc"),
        ),
        (
            json!({"livenessProbe": {"periodSeconds": 5}}),
            Error("livenessProbe"),
        ),
        (
            json!({"livenessProbe": {"tcpSocket": {"port": 1}, "exec": {"command": ["x"]}}}),
            Error("livenessProbe"),
        ),
        (
            json!({"livenessProbe": {"exec": {}}}),
            Error("livenessProbe.exec"),
        ),
        (json!({"livenessProbe": {}}), Ignored),
        (
            json!({"livenessProbe": {
                "tcpSocket": {"port": 80},
                "initialDelaySeconds": 5, "periodSeconds": 20, "timeoutSeconds": 3,
                "successThreshold": 2, "failureThreshold": 6,
            }}),
            Mapped(
                "/containers/0/liveness",
                json!({
                    "action": {"type": "tcp", "port": 80},
                    "initial_delay_secs": 5, "period_secs": 20, "timeout_secs": 3,
                    "success_threshold": 2, "failure_threshold": 6,
                }),
            ),
        ),
        (
            json!({"livenessProbe": {"tcpSocket": {"port": 80},
                                     "terminationGracePeriodSeconds": 10}}),
            MappedWarn(
                "/containers/0/liveness/action/port",
                json!(80),
                "livenessProbe.terminationGracePeriodSeconds",
            ),
        ),
    ]);
}

#[test]
fn container_security_context() {
    let sc = |context: Value| json!({"securityContext": context});
    check_container_rows(vec![
        (
            sc(json!({"runAsUser": 1000})),
            Mapped("/containers/0/user", json!("1000")),
        ),
        (
            sc(json!({"runAsUser": 0})),
            Mapped("/containers/0/user", json!("0")),
        ),
        (
            sc(json!({"runAsUser": 1000, "runAsGroup": 2000})),
            Mapped("/containers/0/user", json!("1000:2000")),
        ),
        (
            sc(json!({"runAsGroup": 2000})),
            Error("securityContext.runAsGroup"),
        ),
        (
            sc(json!({"runAsUser": -1})),
            Error("securityContext.runAsUser"),
        ),
        (
            sc(json!({"capabilities": {"add":
                ["NET_BIND_SERVICE", "CAP_SETUID", "cap_chown", "kill"]}})),
            Mapped(
                "/containers/0/capabilities",
                json!(["NET_BIND_SERVICE", "SETUID", "CHOWN", "KILL"]),
            ),
        ),
        // A capability named twice is kept once, where it first stood.
        (
            sc(json!({"capabilities": {"add":
                ["CHOWN", "KILL", "cap_chown", "CHOWN", "SETUID", "kill"]}})),
            Mapped(
                "/containers/0/capabilities",
                json!(["CHOWN", "KILL", "SETUID"]),
            ),
        ),
        // A capability the kernel has and faber does not grant.
        (
            sc(json!({"capabilities": {"add": ["CHOWN", "SYS_ADMIN"]}})),
            Error("securityContext.capabilities.add[1]"),
        ),
        (
            sc(json!({"capabilities": {"add": ["CAP_NET_RAW"]}})),
            Error("securityContext.capabilities.add[0]"),
        ),
        (
            sc(json!({"capabilities": {"add": ["FLY"]}})),
            Error("securityContext.capabilities.add[0]"),
        ),
        (
            sc(json!({"capabilities": {"add": ["ALL"]}})),
            Error("securityContext.capabilities.add[0]"),
        ),
        (
            sc(json!({"capabilities": {"add": ["cap_all"]}})),
            Error("securityContext.capabilities.add[0]"),
        ),
        (
            sc(json!({"readOnlyRootFilesystem": true})),
            Mapped("/containers/0/read_only_root_fs", json!(true)),
        ),
        (sc(json!({"readOnlyRootFilesystem": false})), Ignored),
        (sc(json!({"capabilities": {"drop": ["ALL"]}})), Ignored),
        (sc(json!({"allowPrivilegeEscalation": false})), Ignored),
        (sc(json!({"privileged": false})), Ignored),
        (
            sc(json!({"seccompProfile": {"type": "RuntimeDefault"}})),
            Ignored,
        ),
        (sc(json!({"procMount": "Default"})), Ignored),
        (sc(json!({"runAsNonRoot": false})), Ignored),
        (sc(json!({})), Ignored),
        (
            sc(json!({"runAsNonRoot": true})),
            Warn("securityContext.runAsNonRoot"),
        ),
        (
            sc(json!({"seLinuxOptions": {"level": "s0"}})),
            Warn("securityContext.seLinuxOptions"),
        ),
        (
            sc(json!({"appArmorProfile": {"type": "RuntimeDefault"}})),
            Warn("securityContext.appArmorProfile"),
        ),
        (
            sc(json!({"privileged": true})),
            Error("securityContext.privileged"),
        ),
        (
            sc(json!({"allowPrivilegeEscalation": true})),
            Error("securityContext.allowPrivilegeEscalation"),
        ),
        (
            sc(json!({"procMount": "Unmasked"})),
            Error("securityContext.procMount"),
        ),
        (
            sc(json!({"seccompProfile": {"type": "Unconfined"}})),
            Error("securityContext.seccompProfile.type"),
        ),
        (
            sc(json!({"seccompProfile": {"type": "Localhost", "localhostProfile": "p.json"}})),
            Error("securityContext.seccompProfile.type"),
        ),
        (
            sc(json!({"windowsOptions": {"runAsUserName": "x"}})),
            Error("securityContext.windowsOptions"),
        ),
        (sc(json!({"shiny": true})), Error("securityContext.shiny")),
    ]);
}

#[test]
fn pod_fields() {
    let pod = |snippet: Value, want: Want| (snippet, json!({}), want);
    check_pod_rows(vec![
        pod(
            json!({"terminationGracePeriodSeconds": 45}),
            Mapped("/stop_grace_secs", json!(45)),
        ),
        pod(
            json!({"terminationGracePeriodSeconds": 0}),
            Mapped("/stop_grace_secs", json!(0)),
        ),
        pod(
            json!({"nodeName": "node-1"}),
            Mapped("/placement/node", json!("node-1")),
        ),
        pod(
            json!({"nodeSelector": {"kubernetes.io/hostname": "node-2"}}),
            Mapped("/placement/node", json!("node-2")),
        ),
        pod(
            json!({"nodeName": "node-1",
                   "nodeSelector": {"kubernetes.io/hostname": "node-1"}}),
            Mapped("/placement/node", json!("node-1")),
        ),
        pod(
            json!({"nodeName": "node-1",
                   "nodeSelector": {"kubernetes.io/hostname": "node-2"}}),
            Error("nodeSelector[\"kubernetes.io/hostname\"]"),
        ),
        pod(
            json!({"nodeSelector": {"disk": "ssd"}}),
            Error("nodeSelector.disk"),
        ),
        pod(json!({"nodeSelector": {}}), Ignored),
        pod(json!({"restartPolicy": "Always"}), Ignored),
        // Under a controller nothing else is possible.
        pod(
            json!({"restartPolicy": "OnFailure"}),
            Error("restartPolicy"),
        ),
        pod(json!({"restartPolicy": "Never"}), Error("restartPolicy")),
        pod(
            json!({"initContainers": [{"name": "init", "image": "busybox"}]}),
            Error("initContainers"),
        ),
        pod(json!({"initContainers": []}), Ignored),
        pod(
            json!({"ephemeralContainers": [{"name": "debug", "image": "busybox"}]}),
            Error("ephemeralContainers"),
        ),
        pod(json!({"ephemeralContainers": []}), Ignored),
        pod(json!({"containers": []}), Error("")),
        pod(json!({"shiny": 1}), Error("shiny")),
    ]);
}

#[test]
fn pod_scheduling() {
    let pod = |snippet: Value, want: Want| (snippet, json!({}), want);
    let term = json!({"nodeSelectorTerms": [{"matchExpressions": []}]});
    check_pod_rows(vec![
        pod(
            json!({"affinity": {"nodeAffinity":
                {"requiredDuringSchedulingIgnoredDuringExecution": term}}}),
            Error("affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution"),
        ),
        pod(
            json!({"affinity": {"nodeAffinity":
                {"preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 1}]}}}),
            Warn("affinity"),
        ),
        pod(
            json!({"affinity": {"podAntiAffinity":
                {"requiredDuringSchedulingIgnoredDuringExecution": [{"topologyKey": "x"}]}}}),
            Warn("affinity"),
        ),
        pod(
            json!({"affinity": {"podAffinity": {"x": [1]}}}),
            Warn("affinity"),
        ),
        pod(json!({"affinity": {}}), Ignored),
        pod(
            json!({"tolerations": [{"operator": "Exists"}]}),
            Warn("tolerations"),
        ),
        pod(
            json!({"topologySpreadConstraints": [{"maxSkew": 1}]}),
            Warn("topologySpreadConstraints"),
        ),
        pod(
            json!({"priorityClassName": "high"}),
            Warn("priorityClassName"),
        ),
        pod(json!({"priority": 100}), Warn("priority")),
        pod(
            json!({"preemptionPolicy": "Never"}),
            Warn("preemptionPolicy"),
        ),
        pod(json!({"preemptionPolicy": "PreemptLowerPriority"}), Ignored),
        pod(json!({"schedulerName": "custom"}), Warn("schedulerName")),
        pod(json!({"schedulerName": "default-scheduler"}), Ignored),
        pod(
            json!({"readinessGates": [{"conditionType": "x"}]}),
            Warn("readinessGates"),
        ),
        pod(
            json!({"schedulingGates": [{"name": "x"}]}),
            Warn("schedulingGates"),
        ),
        pod(json!({"tolerations": [], "readinessGates": []}), Ignored),
    ]);
}

#[test]
fn pod_security_context() {
    let sc = |context: Value, want: Want| (json!({"securityContext": context}), json!({}), want);
    check_pod_rows(vec![
        sc(
            json!({"runAsUser": 1000}),
            Mapped("/containers/0/user", json!("1000")),
        ),
        sc(
            json!({"runAsUser": 1000, "runAsGroup": 2000}),
            Mapped("/containers/0/user", json!("1000:2000")),
        ),
        sc(
            json!({"runAsGroup": 2000}),
            Error("securityContext.runAsGroup"),
        ),
        // A container's own ids win, one by one.
        (
            json!({"securityContext": {"runAsUser": 1000, "runAsGroup": 2000}}),
            json!({"securityContext": {"runAsUser": 5}}),
            Mapped("/containers/0/user", json!("5:2000")),
        ),
        (
            json!({"securityContext": {"runAsUser": 1000}}),
            json!({"securityContext": {"runAsGroup": 7}}),
            Mapped("/containers/0/user", json!("1000:7")),
        ),
        sc(
            json!({"runAsNonRoot": true}),
            Warn("securityContext.runAsNonRoot"),
        ),
        sc(json!({"fsGroup": 2000}), Warn("securityContext.fsGroup")),
        sc(
            json!({"fsGroupChangePolicy": "OnRootMismatch"}),
            Warn("securityContext.fsGroupChangePolicy"),
        ),
        sc(
            json!({"seLinuxOptions": {"level": "s0"}}),
            Warn("securityContext.seLinuxOptions"),
        ),
        sc(
            json!({"appArmorProfile": {"type": "Unconfined"}}),
            Warn("securityContext.appArmorProfile"),
        ),
        sc(
            json!({"seccompProfile": {"type": "RuntimeDefault"}}),
            Ignored,
        ),
        sc(
            json!({"runAsNonRoot": false, "sysctls": [], "supplementalGroups": []}),
            Ignored,
        ),
        sc(
            json!({"seccompProfile": {"type": "Unconfined"}}),
            Error("securityContext.seccompProfile.type"),
        ),
        sc(
            json!({"seccompProfile": {"type": "Localhost", "localhostProfile": "p.json"}}),
            Error("securityContext.seccompProfile.type"),
        ),
        sc(
            json!({"sysctls": [{"name": "net.core.somaxconn", "value": "1024"}]}),
            Error("securityContext.sysctls"),
        ),
        sc(
            json!({"supplementalGroups": [3000]}),
            Error("securityContext.supplementalGroups"),
        ),
        sc(
            json!({"windowsOptions": {"hostProcess": true}}),
            Error("securityContext.windowsOptions"),
        ),
    ]);
}

#[test]
fn the_fs_group_warning_says_what_is_not_done() {
    let pod = json!({"securityContext": {"fsGroup": 2000, "runAsNonRoot": true}});
    let done = run(&[deployment(&pod, &json!({}))]).unwrap();
    assert_eq!(
        messages(&done.warnings),
        [
            "m.yaml: spec.template.spec.securityContext.runAsNonRoot: not enforced",
            "m.yaml: spec.template.spec.securityContext.fsGroup: not enforced: \
             volume ownership is not changed",
        ]
    );
}

#[test]
fn pod_fields_that_are_refused_skipped_or_ignored() {
    let pod = |snippet: Value, want: Want| (snippet, json!({}), want);
    check_pod_rows(vec![
        pod(json!({"hostNetwork": true}), Error("hostNetwork")),
        pod(json!({"hostPID": true}), Error("hostPID")),
        pod(json!({"hostIPC": true}), Error("hostIPC")),
        pod(
            json!({"shareProcessNamespace": true}),
            Error("shareProcessNamespace"),
        ),
        pod(json!({"hostUsers": false}), Error("hostUsers")),
        pod(
            json!({"hostNetwork": false, "hostPID": false, "hostIPC": false,
                   "shareProcessNamespace": false, "hostUsers": true}),
            Ignored,
        ),
        pod(
            json!({"hostAliases": [{"ip": "10.0.0.1", "hostnames": ["db"]}]}),
            Error("hostAliases"),
        ),
        pod(
            json!({"dnsConfig": {"nameservers": ["1.1.1.1"]}}),
            Error("dnsConfig"),
        ),
        pod(json!({"dnsPolicy": "None"}), Error("dnsPolicy")),
        pod(
            json!({"dnsPolicy": "ClusterFirstWithHostNet"}),
            Error("dnsPolicy"),
        ),
        pod(json!({"dnsPolicy": "ClusterFirst"}), Ignored),
        pod(json!({"dnsPolicy": "Default"}), Ignored),
        pod(
            json!({"runtimeClassName": "gvisor"}),
            Error("runtimeClassName"),
        ),
        pod(
            json!({"resourceClaims": [{"name": "gpu"}]}),
            Error("resourceClaims"),
        ),
        pod(
            json!({"resources": {"limits": {"cpu": "1"}}}),
            Error("resources"),
        ),
        pod(json!({"os": {"name": "windows"}}), Error("os.name")),
        pod(json!({"os": {"name": "linux"}}), Ignored),
        pod(
            json!({"activeDeadlineSeconds": 600}),
            Error("activeDeadlineSeconds"),
        ),
        pod(
            json!({"imagePullSecrets": [{"name": "regcred"}]}),
            Warn("imagePullSecrets"),
        ),
        pod(
            json!({"serviceAccountName": "web"}),
            Warn("serviceAccountName"),
        ),
        pod(json!({"serviceAccount": "web"}), Warn("serviceAccount")),
        pod(json!({"hostname": "web-0"}), Warn("hostname")),
        pod(json!({"subdomain": "web"}), Warn("subdomain")),
        pod(
            json!({"setHostnameAsFQDN": true}),
            Warn("setHostnameAsFQDN"),
        ),
        pod(json!({"automountServiceAccountToken": false}), Ignored),
        pod(json!({"enableServiceLinks": true}), Ignored),
        pod(json!({"overhead": {"cpu": "100m"}}), Ignored),
    ]);
}

#[test]
fn volumes() {
    let vol = |source: Value| {
        let mut entry = json!({"name": "data"});
        merge(&mut entry, &source);
        (json!({"volumes": [entry]}), json!({}))
    };
    let host = |kind: &str| vol(json!({"hostPath": {"path": "/srv/data", "type": kind}}));
    let host_json =
        |create: bool| json!({"type": "host_path", "path": "/srv/data", "create": create});
    let row = |(pod, container): (Value, Value), want: Want| (pod, container, want);
    check_pod_rows(vec![
        row(
            vol(json!({"hostPath": {"path": "/srv/data"}})),
            Mapped("/volumes/data", host_json(false)),
        ),
        row(host(""), Mapped("/volumes/data", host_json(false))),
        row(host("Directory"), Mapped("/volumes/data", host_json(false))),
        row(host("File"), Mapped("/volumes/data", host_json(false))),
        row(
            host("DirectoryOrCreate"),
            Mapped("/volumes/data", host_json(true)),
        ),
        row(host("FileOrCreate"), Error("volumes[0].hostPath.type")),
        row(host("Socket"), Error("volumes[0].hostPath.type")),
        row(host("CharDevice"), Error("volumes[0].hostPath.type")),
        row(host("BlockDevice"), Error("volumes[0].hostPath.type")),
        row(vol(json!({"hostPath": {}})), Error("volumes[0].hostPath")),
        row(
            vol(json!({"emptyDir": {}})),
            Mapped(
                "/volumes/data",
                json!({"type": "ephemeral", "memory": false}),
            ),
        ),
        row(
            vol(json!({"emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"}})),
            Mapped(
                "/volumes/data",
                json!({"type": "ephemeral", "memory": true, "size_bytes": 67_108_864}),
            ),
        ),
        row(
            vol(json!({"emptyDir": {"medium": "", "sizeLimit": 1024}})),
            Mapped(
                "/volumes/data",
                json!({"type": "ephemeral", "memory": false, "size_bytes": 1024}),
            ),
        ),
        // A size of zero is no limit.
        row(
            vol(json!({"emptyDir": {"sizeLimit": 0}})),
            Mapped(
                "/volumes/data",
                json!({"type": "ephemeral", "memory": false}),
            ),
        ),
        row(
            vol(json!({"emptyDir": {"sizeLimit": "0Gi"}})),
            Mapped(
                "/volumes/data",
                json!({"type": "ephemeral", "memory": false}),
            ),
        ),
        row(
            vol(json!({"emptyDir": {"medium": "HugePages"}})),
            Error("volumes[0].emptyDir.medium"),
        ),
        row(
            vol(json!({"emptyDir": {"sizeLimit": "64m"}})),
            Error("volumes[0].emptyDir.sizeLimit"),
        ),
        row(
            vol(json!({"persistentVolumeClaim": {"claimName": "pgdata"}})),
            MappedWarn(
                "/volumes/data",
                json!({"type": "named", "name": "pgdata", "per_replica": false}),
                "volumes[0].persistentVolumeClaim",
            ),
        ),
        row(
            vol(json!({"persistentVolumeClaim": {}})),
            Error("volumes[0].persistentVolumeClaim"),
        ),
        row(vol(json!({})), Error("volumes[0]")),
        row(
            vol(json!({"emptyDir": {}, "hostPath": {"path": "/x"}})),
            Error("volumes[0]"),
        ),
        (
            json!({"volumes": [{"name": "My_Data", "emptyDir": {}}]}),
            json!({}),
            Error("volumes[0].name"),
        ),
        (
            json!({"volumes": [{"name": "data", "emptyDir": {}},
                               {"name": "data", "emptyDir": {}}]}),
            json!({}),
            Error("volumes[1].name"),
        ),
        (json!({"volumes": []}), json!({}), Ignored),
    ]);
}

#[test]
fn volume_mounts() {
    let empty = json!({"volumes": [{"name": "data", "emptyDir": {}}]});
    let mount = |extra: Value| {
        let mut entry = json!({"name": "data", "mountPath": "/data"});
        merge(&mut entry, &extra);
        json!({"volumeMounts": [entry]})
    };
    let at = "containers[0].volumeMounts[0]";
    let row = |container: Value, want: Want| (empty.clone(), container, want);
    check_pod_rows(vec![
        row(
            mount(json!({})),
            Mapped(
                "/containers/0/mounts",
                json!([{"volume": "data", "path": "/data", "read_only": false}]),
            ),
        ),
        row(
            mount(json!({"readOnly": true, "subPath": "web"})),
            Mapped(
                "/containers/0/mounts",
                json!([{"volume": "data", "path": "/data", "read_only": true, "sub_path": "web"}]),
            ),
        ),
        // An empty sub-path is none.
        row(
            mount(json!({"subPath": "", "subPathExpr": ""})),
            Mapped(
                "/containers/0/mounts",
                json!([{"volume": "data", "path": "/data", "read_only": false}]),
            ),
        ),
        row(
            mount(json!({"mountPropagation": "None", "recursiveReadOnly": "Disabled"})),
            Mapped("/containers/0/mounts/0/path", json!("/data")),
        ),
        row(
            mount(json!({"subPathExpr": "$(POD_NAME)"})),
            Error("containers[0].volumeMounts[0].subPathExpr"),
        ),
        row(
            mount(json!({"mountPropagation": "Bidirectional"})),
            Error("containers[0].volumeMounts[0].mountPropagation"),
        ),
        row(
            mount(json!({"mountPropagation": "HostToContainer"})),
            Error("containers[0].volumeMounts[0].mountPropagation"),
        ),
        row(
            mount(json!({"recursiveReadOnly": "Enabled"})),
            MappedWarn(
                "/containers/0/mounts/0/path",
                json!("/data"),
                "containers[0].volumeMounts[0].recursiveReadOnly",
            ),
        ),
        row(json!({"volumeMounts": [{"name": "data"}]}), Error(at)),
        row(
            json!({"volumeMounts": [{"name": "nope", "mountPath": "/data"}]}),
            Error("containers[0].volumeMounts[0].name"),
        ),
        // A read-only claim makes every mount of it read-only.
        (
            json!({"volumes": [{"name": "data", "persistentVolumeClaim":
                {"claimName": "pgdata", "readOnly": true}}]}),
            mount(json!({})),
            MappedWarn(
                "/containers/0/mounts/0/read_only",
                json!(true),
                "volumes[0].persistentVolumeClaim",
            ),
        ),
    ]);
}

/// A source the model does not have is an error once a container mounts
/// it, and a warning while none does.
#[test]
fn unsupported_volume_sources() {
    for (key, source) in [
        ("configMap", json!({"name": "settings"})),
        ("secret", json!({"secretName": "tls"})),
        ("projected", json!({"sources": []})),
        ("downwardAPI", json!({"items": []})),
        ("csi", json!({"driver": "x"})),
        ("nfs", json!({"server": "s", "path": "/"})),
        ("ephemeral", json!({"volumeClaimTemplate": {}})),
        ("image", json!({"reference": "x"})),
        ("iscsi", json!({"targetPortal": "x"})),
        ("someFutureSource", json!({"x": 1})),
    ] {
        let volumes = json!({"volumes": [{"name": "data", key: source}]});
        let mounts = json!({"volumeMounts": [{"name": "data", "mountPath": "/data"}]});
        let path: &'static str = Box::leak(format!("volumes[0].{key}").into_boxed_str());
        check_pod_rows(vec![
            (volumes.clone(), json!({}), Warn(path)),
            (volumes, mounts, Error(path)),
        ]);
    }
}

/// A manifest of `kind` named `web`, with `spec` around the usual pod
/// template.
fn controller_doc(kind: &str, spec: &Value) -> Value {
    let mut doc = plain();
    doc["kind"] = json!(kind);
    merge(&mut doc["spec"], spec);
    doc
}

fn check_controller_rows(kind: &str, rows: Vec<(Value, Want)>) {
    for (spec, want) in rows {
        let label = format!("{kind} {spec}");
        let doc = controller_doc(kind, &spec);
        check(&label, doc, controller_doc(kind, &json!({})), "spec", &want);
    }
}

#[test]
fn replicas_become_the_scale() {
    for kind in ["Deployment", "ReplicaSet", "StatefulSet"] {
        check_controller_rows(
            kind,
            vec![
                (json!({}), Mapped("/scale", json!({"replicas": 1}))),
                (
                    json!({"replicas": 3}),
                    Mapped("/scale", json!({"replicas": 3})),
                ),
                (
                    json!({"replicas": 0}),
                    Mapped("/scale", json!({"replicas": 0})),
                ),
                (json!({"replicas": "3"}), Error("replicas")),
                (json!({"replicas": -1}), Error("replicas")),
            ],
        );
    }
    check_controller_rows(
        "DaemonSet",
        vec![
            (json!({}), Mapped("/scale", json!("per_node"))),
            (json!({"replicas": 3}), Error("replicas")),
            (
                json!({"updateStrategy": {"type": "RollingUpdate"}}),
                Warn("updateStrategy"),
            ),
        ],
    );
}

#[test]
fn controller_fields() {
    check_controller_rows(
        "Deployment",
        vec![
            (
                json!({
                    "selector": {"matchLabels": {"app": "web"}},
                    "revisionHistoryLimit": 5,
                    "progressDeadlineSeconds": 600,
                    "minReadySeconds": 10,
                    "paused": false,
                    "strategy": {},
                }),
                Ignored,
            ),
            (
                json!({"template": {
                    "metadata": {"labels": {"app": "web"}, "annotations": {"note": "x"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                }}),
                Ignored,
            ),
            // The pod template's metadata is not the workload's. Exposing
            // from there would be dropped without a word, so it is refused.
            (
                json!({"template": {
                    "metadata": {"labels": {"app": "web"},
                                 "annotations": {"note": "x", "faber.internal/expose": "cluster"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                }}),
                Error("template.metadata.annotations[\"faber.internal/expose\"]"),
            ),
            (json!({"strategy": {"type": "Recreate"}}), Warn("strategy")),
            (json!({"paused": true}), Warn("paused")),
            // Fields of another kind are unknown here.
            (json!({"serviceName": "web"}), Error("serviceName")),
            (
                json!({"volumeClaimTemplates": []}),
                Error("volumeClaimTemplates"),
            ),
            (json!({"shiny": 1}), Error("shiny")),
            (json!({"template": null}), Error("")),
            (json!({"template": {"metadata": {}}}), Error("template")),
        ],
    );
}

#[test]
fn stateful_set_fields() {
    check_controller_rows(
        "StatefulSet",
        vec![
            (json!({"serviceName": "web"}), Ignored),
            (json!({"podManagementPolicy": "OrderedReady"}), Ignored),
            (
                json!({"podManagementPolicy": "Parallel"}),
                Warn("podManagementPolicy"),
            ),
            (
                json!({"updateStrategy": {"type": "OnDelete"}}),
                Warn("updateStrategy"),
            ),
            (
                json!({"persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}}),
                Warn("persistentVolumeClaimRetentionPolicy"),
            ),
            (json!({"ordinals": {"start": 1}}), Warn("ordinals")),
            (
                json!({"volumeClaimTemplates": [{
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "data", "labels": {"a": "b"}},
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "fast",
                        "volumeMode": "Filesystem",
                        "resources": {"requests": {"storage": "10Gi"}},
                    },
                    "status": {"phase": "Pending"},
                }]}),
                MappedWarn(
                    "/volumes/data",
                    json!({"type": "named", "name": "data", "per_replica": true}),
                    "volumeClaimTemplates[0]",
                ),
            ),
            (
                json!({"volumeClaimTemplates": [{"metadata": {"name": "data"},
                                                 "spec": {"volumeMode": "Block"}}]}),
                Error("volumeClaimTemplates[0].spec.volumeMode"),
            ),
            (
                json!({"volumeClaimTemplates": [{"metadata": {"name": "Data"}}]}),
                Error("volumeClaimTemplates[0].metadata.name"),
            ),
            (
                json!({"volumeClaimTemplates": [{"spec": {}}]}),
                Error("volumeClaimTemplates[0]"),
            ),
        ],
    );

    let claim = json!({"volumeClaimTemplates": [{"metadata": {"name": "data"}}]});
    let done = run(&[controller_doc("StatefulSet", &claim)]).unwrap();
    assert_eq!(
        messages(&done.warnings),
        [
            "m.yaml: spec.volumeClaimTemplates[0]: volume `data` is node-local, not a \
          PersistentVolume: each replica keeps its data on the node it runs on"
        ]
    );

    // A claim template and a pod volume may not share a name.
    let mut doc = controller_doc("StatefulSet", &claim);
    doc["spec"]["template"]["spec"]["volumes"] = json!([{"name": "data", "emptyDir": {}}]);
    let err = run(&[doc]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        ["m.yaml: spec.template.spec.volumes[0].name: volume `data` is defined twice"]
    );
}

fn pod_doc(spec: &Value) -> Value {
    let mut doc = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
    });
    merge(&mut doc["spec"], spec);
    doc
}

#[test]
fn a_bare_pod_is_one_replica_with_its_own_restart_policy() {
    for (spec, want) in [
        (json!({}), Mapped("/scale", json!({"replicas": 1}))),
        (json!({}), Mapped("/restart", json!("always"))),
        (
            json!({"restartPolicy": "Always"}),
            Mapped("/restart", json!("always")),
        ),
        (
            json!({"restartPolicy": "OnFailure"}),
            Mapped("/restart", json!("on_failure")),
        ),
        (
            json!({"restartPolicy": "Never"}),
            Mapped("/restart", json!("never")),
        ),
        (
            json!({"restartPolicy": "Sometimes"}),
            Error("restartPolicy"),
        ),
        (json!({"replicas": 2}), Error("replicas")),
        (
            json!({"nodeName": "node-1"}),
            Mapped("/placement/node", json!("node-1")),
        ),
    ] {
        let label = format!("Pod {spec}");
        check(&label, pod_doc(&spec), pod_doc(&json!({})), "spec", &want);
    }
}

#[test]
fn metadata_fields() {
    let with = |metadata: Value| {
        let mut doc = plain();
        merge(&mut doc["metadata"], &metadata);
        doc
    };
    let expose = |value: Value| json!({"annotations": {"faber.internal/expose": value}});
    for (metadata, want) in [
        (
            json!({
                "namespace": "shop",
                "labels": {"app": "web"},
                "annotations": {
                    "note": "x",
                    "kubectl.kubernetes.io/last-applied-configuration": "{}",
                },
                "uid": "0b1c", "resourceVersion": "42", "generation": 3,
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "managedFields": [{"manager": "kubectl"}],
                "ownerReferences": [{"kind": "x"}],
                "finalizers": ["x"],
            }),
            Ignored,
        ),
        (
            expose(json!("cluster")),
            Mapped("/expose", json!("cluster")),
        ),
        (
            expose(json!("environment")),
            Mapped("/expose", json!("environment")),
        ),
        (
            expose(json!("public")),
            Error("annotations[\"faber.internal/expose\"]"),
        ),
        (
            expose(json!(true)),
            Error("annotations[\"faber.internal/expose\"]"),
        ),
        (json!({"name": "Web_1"}), Error("name")),
        (json!({"name": "-web"}), Error("name")),
        (json!({"name": 7}), Error("name")),
        (
            json!({"name": null, "generateName": "web-"}),
            Error("generateName"),
        ),
        (json!({"name": null}), Error("")),
        (json!({"selfLink": "/x"}), Error("selfLink")),
    ] {
        let label = metadata.to_string();
        check(&label, with(metadata), plain(), "metadata", &want);
    }

    // `status` is the cluster's, and never read.
    let mut doc = plain();
    doc["status"] = json!({"readyReplicas": 1, "conditions": [{"type": "Available"}]});
    check("status", doc, plain(), "", &Ignored);
    let mut doc = plain();
    doc["shiny"] = json!(1);
    check("unknown top-level key", doc, plain(), "", &Error("shiny"));
    let mut doc = plain();
    doc["spec"] = json!(null);
    check("no spec", doc, plain(), "", &Error(""));
}

#[test]
fn a_name_that_is_no_dns_label_is_not_renamed() {
    let mut doc = plain();
    doc["metadata"]["name"] = json!("Web_1");
    let err = run(&[doc]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml: metadata.name: workload name `Web_1` is not a DNS label: it may only \
          contain a-z, 0-9 and hyphens. Rename it in the manifest"
        ]
    );
}

fn object(kind: &str, name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": kind, "metadata": {"name": name},
           "spec": {"anything": {"at": ["all"]}}, "data": {"k": "v"}})
}

#[test]
fn kinds_that_are_skipped_with_a_warning() {
    for kind in SKIPPED_KINDS {
        let done = run(&[object(kind, "thing"), plain()]).unwrap();
        assert_eq!(
            messages(&done.warnings),
            [format!(
                "m.yaml#1: {kind} `thing` is skipped: faber has nothing like it"
            )]
        );
        assert_eq!(done.workloads.len(), 1, "{kind}");
    }
    // As many as the contract lists.
    assert_eq!(SKIPPED_KINDS.len(), 21);
}

#[test]
fn a_secret_is_skipped_and_its_data_is_never_shown() {
    let secret = json!({"kind": "Secret", "metadata": {"name": "tls"},
                        "data": {"key": "aHVudGVyMg=="}, "stringData": {"pw": "hunter2"}});
    let done = run(&[secret, plain()]).unwrap();
    assert_eq!(
        messages(&done.warnings),
        [
            "m.yaml#1: Secret `tls` is skipped and its data was not read: \
          faber has no secret store yet"
        ]
    );
    assert!(!format!("{done:?}").contains("hunter2"));
}

#[test]
fn kinds_that_are_refused() {
    for (kind, part) in [
        ("Job", "Job `thing` is not supported"),
        ("CronJob", "CronJob `thing` is not supported"),
        ("ReplicationController", "write it as a Deployment"),
        ("Rollout", "Rollout `thing` is of an unknown kind"),
        ("KafkaTopic", "KafkaTopic `thing` is of an unknown kind"),
        ("DeploymentList", "is of an unknown kind"),
    ] {
        let err = run(&[plain(), object(kind, "thing")]).unwrap_err();
        assert_eq!(err.errors.len(), 1, "{kind}: {:?}", messages(&err.errors));
        assert_eq!(err.errors[0].source, "m.yaml#2");
        assert_eq!(err.errors[0].path, "");
        assert!(err.errors[0].message.contains(part), "{}", err.errors[0]);
    }
    // Not an object at all.
    for (doc, part) in [
        (json!({"metadata": {"name": "x"}}), "has no `kind`"),
        (json!(["a"]), "expected a Kubernetes object, found a list"),
        (
            json!("text"),
            "expected a Kubernetes object, found a string",
        ),
    ] {
        let err = run(&[plain(), doc]).unwrap_err();
        assert!(err.errors[0].message.contains(part), "{}", err.errors[0]);
    }
}

#[test]
fn a_list_is_flattened() {
    let mut api = deployment(&json!({}), &json!({"tty": true}));
    api["metadata"]["name"] = json!("api");
    let list = json!({
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": [
            plain(),
            object("Service", "web"),
            {"kind": "List", "items": [api]},
        ],
    });
    let err = run(std::slice::from_ref(&list)).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml: items[2].items[0].spec.template.spec.containers[0].tty: \
          a TTY is not supported"
        ]
    );
    assert_eq!(
        messages(&err.warnings),
        ["m.yaml: items[1]: Service `web` is skipped: faber has nothing like it"]
    );

    let mut list = list;
    list["items"][2]["items"][0]["spec"]["template"]["spec"]["containers"][0]["tty"] = json!(false);
    let done = run(&[list]).unwrap();
    assert_eq!(done.workloads.keys().collect::<Vec<_>>(), ["api", "web"]);
    // An empty list holds no workload.
    let err = run(&[json!({"kind": "List", "items": []})]).unwrap_err();
    assert!(err.errors[0].message.starts_with("no workload found"));
}

#[test]
fn input_without_a_workload_is_an_error() {
    for docs in [
        vec![object("Service", "web")],
        vec![object("Service", "web"), config_maps().remove(0)],
    ] {
        let err = run(&docs).unwrap_err();
        assert_eq!(err.errors.len(), 1);
        assert_eq!(
            err.errors[0].message,
            "no workload found: the input needs a Deployment, StatefulSet, DaemonSet, \
             ReplicaSet or Pod"
        );
        assert_eq!(err.errors[0].source, "m.yaml");
        // The warning for the Service is still there.
        assert_eq!(err.warnings.len(), 1);
    }
    assert_eq!(run(&[]).unwrap_err().errors.len(), 1);
    assert_eq!(convert(&[]).unwrap_err().errors[0].source, "input");
    // Another error says enough by itself.
    let err = run(&[object("Job", "batch")]).unwrap_err();
    assert_eq!(err.errors.len(), 1);
}

#[test]
fn two_workloads_of_one_name_are_an_error_that_names_both_sources() {
    let mut other = pod_doc(&json!({}));
    other["metadata"]["namespace"] = json!("elsewhere");
    let err = convert(&[
        stream("a.yaml", &[plain()]),
        stream("b.yaml", &[object("Service", "web"), other]),
    ])
    .unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "b.yaml#2: metadata.name: workload `web` is already defined in a.yaml; \
          a deploy holds each workload name once, whatever the namespace"
        ]
    );
}

#[test]
fn config_maps_are_resolved_by_namespace_and_never_stored() {
    let map = |namespace: Option<&str>, value: &str| {
        let mut doc = json!({"kind": "ConfigMap", "metadata": {"name": "settings"},
                             "data": {"MODE": value}, "immutable": true});
        if let Some(namespace) = namespace {
            doc["metadata"]["namespace"] = json!(namespace);
        }
        doc
    };
    let from = json!({"envFrom": [{"configMapRef": {"name": "settings"}}]});
    let mut shop = deployment(&json!({}), &from);
    shop["metadata"]["namespace"] = json!("shop");
    let mut bare = deployment(&json!({}), &from);
    bare["metadata"]["name"] = json!("bare");

    // The maps come after the workloads that use them.
    let done = run(&[
        shop.clone(),
        bare,
        map(Some("shop"), "shop-mode"),
        map(None, "default-mode"),
        map(Some("unused"), "x"),
    ])
    .unwrap();
    assert_eq!(done.workloads.keys().collect::<Vec<_>>(), ["bare", "web"]);
    assert_eq!(done.workloads["web"].containers[0].env["MODE"], "shop-mode");
    assert_eq!(
        done.workloads["bare"].containers[0].env["MODE"],
        "default-mode"
    );
    assert!(done.warnings.is_empty());

    // A map of another namespace does not count.
    let err = run(&[shop.clone(), map(None, "x")]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml#1: spec.template.spec.containers[0].envFrom[0].configMapRef: \
          ConfigMap `settings` is not in the input for namespace `shop`; \
          add its manifest to the deploy"
        ]
    );

    // Twice in one namespace, a value that is no string, an unknown key.
    let mut bad = map(Some("shop"), "x");
    bad["data"]["PORT"] = json!(8080);
    bad["shiny"] = json!(1);
    let err = run(&[shop, map(Some("shop"), "x"), bad]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml#3: data.PORT: expected a string, found a number: quote the value",
            "m.yaml#3: metadata.name: ConfigMap `settings` is defined twice in namespace `shop`",
            "m.yaml#3: shiny: unsupported key",
        ]
    );
}

/// Real YAML, with the scalars as people write them.
#[test]
fn yaml_scalars_come_through_as_written() {
    let done = run_yaml(concat!(
        "kind: Pod\n",
        "metadata: {name: web}\n",
        "spec:\n",
        "  containers:\n",
        "    - name: app\n",
        "      image: nginx\n",
        "      env:\n",
        "        - {name: PORT, value: \"8080\"}\n",
        "        - {name: ANSWER, value: yes}\n",
        "        - {name: NOTHING, value: }\n",
        "      resources:\n",
        "        limits: {cpu: 0.5, memory: 1e9}\n",
        "        requests: {cpu: 250m, memory: 129e6}\n",
    ))
    .unwrap();
    let app = &done.workloads["web"].containers[0];
    assert_eq!(app.env["PORT"], "8080");
    assert_eq!(app.env["ANSWER"], "yes");
    assert_eq!(app.env["NOTHING"], "");
    assert_eq!(app.resources.limits.cpu_millis, Some(500));
    assert_eq!(app.resources.limits.memory_bytes, Some(1_000_000_000));
    assert_eq!(app.resources.requests.cpu_millis, Some(250));
    assert_eq!(app.resources.requests.memory_bytes, Some(129_000_000));

    // An unquoted number as an env value has lost what was written.
    let err = run_yaml(concat!(
        "kind: Pod\n",
        "metadata: {name: web}\n",
        "spec:\n",
        "  containers:\n",
        "    - name: app\n",
        "      image: nginx\n",
        "      env:\n",
        "        - {name: PORT, value: 8080}\n",
        "        - {name: MODE, value: 0755}\n",
        "        - {name: DEBUG, value: true}\n",
    ))
    .unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml: spec.containers[0].env[0].value: expected a string, found a number: \
             quote the value",
            "m.yaml: spec.containers[0].env[1].value: expected a string, found a number: \
             quote the value",
            "m.yaml: spec.containers[0].env[2].value: expected a string, found a boolean: \
             quote the value",
        ]
    );
}

#[test]
fn a_stream_that_is_not_yaml_is_an_error_next_to_the_others() {
    let web = deployment(&json!({}), &json!({"tty": true}));
    let err = convert(&[
        SourceDocument {
            source: "broken.yaml".into(),
            text: "kind: [Pod\n".into(),
        },
        stream("web.yaml", &[web]),
    ])
    .unwrap_err();
    assert_eq!(err.errors.len(), 2, "{:?}", messages(&err.errors));
    assert_eq!(err.errors[0].source, "broken.yaml");
    assert!(err.errors[0].message.starts_with("not valid YAML: "));
    assert_eq!(err.errors[1].source, "web.yaml");
}

/// Every error of every object is reported, in input order.
#[test]
fn errors_are_collected_across_objects() {
    let mut api = deployment(
        &json!({"hostNetwork": true}),
        &json!({"securityContext": {"privileged": true}, "shiny": 1}),
    );
    api["metadata"]["name"] = json!("api");
    let err = run(&[
        deployment(&json!({}), &json!({"tty": true})),
        object("Job", "batch"),
        api,
    ])
    .unwrap_err();
    assert_eq!(
        paths(&err.errors),
        [
            "spec.template.spec.containers[0].tty",
            "",
            "spec.template.spec.hostNetwork",
            "spec.template.spec.containers[0].securityContext.privileged",
            "spec.template.spec.containers[0].shiny",
        ]
    );
    let sources: Vec<&str> = err.errors.iter().map(|d| d.source.as_str()).collect();
    assert_eq!(
        sources,
        ["m.yaml#1", "m.yaml#2", "m.yaml#3", "m.yaml#3", "m.yaml#3"]
    );
}

/// The model's own rules run last, for the documents that converted.
#[test]
fn the_model_rules_are_checked_at_the_end() {
    let mut api = deployment(&json!({}), &json!({"tty": true}));
    api["metadata"]["name"] = json!("api");
    let broken = deployment(
        &json!({"volumes": [{"name": "data", "emptyDir": {}}]}),
        &json!({
            "volumeMounts": [{"name": "data", "mountPath": "data"}],
            "env": [{"name": "1BAD", "value": "x"}],
        }),
    );
    let err = run(&[api, broken]).unwrap_err();
    assert_eq!(
        messages(&err.errors),
        [
            "m.yaml#1: spec.template.spec.containers[0].tty: a TTY is not supported",
            "m.yaml#2: web.containers[0].env.1BAD: name must match [A-Za-z_][A-Za-z0-9_.-]*",
            "m.yaml#2: web.containers[0].mounts[0].path: must be an absolute path",
        ]
    );
}
