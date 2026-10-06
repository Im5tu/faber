//! The Kubernetes converter: manifests in, one workload per workload kind out.
//!
//! Every object is read through a [`Walk`], so a field this file does not
//! name is an error with its path. The fields it does name fall into four
//! classes: mapped into the spec, ignored because faber already behaves
//! that way, skipped with a warning because they do not apply, or refused
//! because dropping them would change how the container runs.
//!
//! A field at its no-op value (`false`, `null`, `[]`, `{}`, `""` or the
//! documented default) is ignored whatever its class.

use std::collections::{BTreeMap, BTreeSet};

use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::{
    ALLOWED_CAPABILITIES, CAPABILITIES, ContainerSpec, Exposure, HttpScheme, MountSpec, Placement,
    PortSpec, Probe, ProbeAction, Protocol, ResourceAmounts, Resources, RestartPolicy, Scale,
    VolumeSource, WorkloadSpec, validate_workloads,
};
use serde_json::Value;

use crate::units::{UnitError, k8s_cpu_millis, k8s_memory_bytes};
use crate::walk::{Cursor, Walk};
use crate::{Conversion, ConvertError, Diagnostic, Diagnostics, SourceDocument, yaml};

/// The annotation on a workload object that sets [`WorkloadSpec::expose`].
pub const EXPOSE_ANNOTATION: &str = "faber.internal/expose";

/// The one `nodeSelector` key that is mapped: it pins the workload to a node.
const HOSTNAME_LABEL: &str = "kubernetes.io/hostname";

/// Kinds that are skipped with one warning each. Their content is not read.
const SKIPPED_KINDS: &[&str] = &[
    "Service",
    "Ingress",
    "IngressClass",
    "Namespace",
    "ServiceAccount",
    "Role",
    "RoleBinding",
    "ClusterRole",
    "ClusterRoleBinding",
    "HorizontalPodAutoscaler",
    "PodDisruptionBudget",
    "NetworkPolicy",
    "PersistentVolumeClaim",
    "PersistentVolume",
    "StorageClass",
    "Endpoints",
    "EndpointSlice",
    "CustomResourceDefinition",
    "PriorityClass",
    "LimitRange",
    "ResourceQuota",
];

/// What the API server writes into `metadata`, and `labels`. None of it
/// reaches the spec.
const IGNORED_METADATA: &[&str] = &[
    "labels",
    "uid",
    "resourceVersion",
    "creationTimestamp",
    "generation",
    "managedFields",
    "ownerReferences",
    "finalizers",
];

/// Longest string a `$(VAR)` expansion may produce: 64 KiB.
const MAX_EXPANDED_BYTES: usize = 64 * 1024;

const NOT_ENFORCED: &str = "not enforced";
const NO_SECRET_STORE: &str = "secrets are not supported: faber has no secret store yet";

/// Converts every document of every stream in `documents`, in order.
///
/// `Deployment`, `ReplicaSet`, `StatefulSet`, `DaemonSet` and `Pod` each
/// become a workload named after `metadata.name`. A `List` is flattened.
/// A `ConfigMap` is used to resolve references from the workloads of the
/// same input and is never stored. Input without a workload is an error,
/// and so are two workloads with one name.
///
/// `docs/contract.md` lists, per kind and field, whether it is mapped,
/// ignored, skipped with a warning or refused.
pub fn convert(documents: &[SourceDocument]) -> Result<Conversion, ConvertError> {
    // One walk per YAML document. A stream that does not parse keeps its
    // place in the order with a walk that holds the error.
    let mut parsed: Vec<(Walk, Option<Value>)> = Vec::new();
    for document in documents {
        match yaml::parse_documents(&document.text) {
            Ok(values) => {
                let several = values.len() > 1;
                for (i, value) in values.into_iter().enumerate() {
                    let source = if several {
                        format!("{}#{}", document.source, i + 1)
                    } else {
                        document.source.clone()
                    };
                    parsed.push((Walk::new(source), Some(value)));
                }
            }
            Err(e) => {
                let walk = Walk::new(document.source.clone());
                walk.error("", format!("not valid YAML: {}", e.message));
                parsed.push((walk, None));
            }
        }
    }

    let mut workloads: BTreeMap<String, WorkloadSpec> = BTreeMap::new();
    // The document each workload name was first seen in.
    let mut origin: BTreeMap<String, usize> = BTreeMap::new();
    let mut saw_workload = false;
    {
        let mut objects = Vec::new();
        for (doc, (walk, value)) in parsed.iter().enumerate() {
            if let Some(value) = value {
                flatten(doc, walk.root(value), &mut objects);
            }
        }

        // ConfigMaps first: a workload may refer to one that comes later.
        let mut config_maps = ConfigMaps::default();
        for object in &objects {
            if object.kind == "ConfigMap" {
                config_maps.read(&object.at);
            }
        }

        for object in &objects {
            let at = &object.at;
            let Some(kind) = WorkloadKind::of(object.kind) else {
                other_kind(object);
                continue;
            };
            saw_workload = true;
            let (name, spec) = workload(at, kind, &config_maps);
            let Some(name) = name else { continue };
            let Some(text) = name.as_str() else { continue };
            if let Some(first) = origin.get(text) {
                name.error(format!(
                    "workload `{text}` is already defined in {}; a deploy holds each \
                     workload name once, whatever the namespace",
                    parsed[*first].0.source()
                ));
                continue;
            }
            origin.insert(text.to_string(), object.doc);
            if let Some(spec) = spec {
                workloads.insert(text.to_string(), spec);
            }
        }
    }

    let sources: Vec<String> = parsed
        .iter()
        .map(|(walk, _)| walk.source().to_string())
        .collect();
    let mut found = Diagnostics::default();
    let mut clean = Vec::new();
    for (walk, _) in parsed {
        let diagnostics = walk.finish();
        clean.push(diagnostics.errors.is_empty());
        found.extend(diagnostics);
    }

    // The label of the whole input, for findings about no one document.
    let input = match documents {
        [one] => one.source.clone(),
        _ => "input".to_string(),
    };
    if !saw_workload && found.errors.is_empty() {
        found.errors.push(Diagnostic::new(
            input.clone(),
            "",
            "no workload found: the input needs a Deployment, StatefulSet, DaemonSet, \
             ReplicaSet or Pod",
        ));
    }

    // The model's own rules, for the workloads of documents that converted
    // without an error. A document with errors would only repeat them.
    let checked: BTreeMap<String, WorkloadSpec> = workloads
        .iter()
        .filter(|(name, _)| origin.get(*name).is_some_and(|doc| clean[*doc]))
        .map(|(name, spec)| (name.clone(), spec.clone()))
        .collect();
    if let Err(violations) = validate_workloads(&checked) {
        for violation in violations {
            // `<workload>.<path in the spec>`, or empty for the whole map.
            let name = violation.path.split('.').next().unwrap_or_default();
            let source = match origin.get(name) {
                Some(doc) => sources[*doc].clone(),
                None => input.clone(),
            };
            found
                .errors
                .push(Diagnostic::new(source, violation.path, violation.message));
        }
    }

    found.finish(workloads)
}

/// One Kubernetes object of the input.
struct Object<'a> {
    /// Index of its document.
    doc: usize,
    at: Cursor<'a>,
    kind: &'a str,
}

/// Collects the objects below `at`: the object itself, or the items of a
/// `List`, however deep.
fn flatten<'a>(doc: usize, at: Cursor<'a>, objects: &mut Vec<Object<'a>>) {
    if !at.is_map() {
        at.error(format!("expected a Kubernetes object, found {}", at.kind()));
        return;
    }
    // Read without the walk: a skipped kind must stay unopened.
    match at.value().get("kind").and_then(Value::as_str) {
        Some("List") => {
            at.ignore("apiVersion");
            at.ignore("kind");
            at.ignore("metadata");
            if let Some(items) = at.get("items") {
                for item in items.items() {
                    flatten(doc, item, objects);
                }
            }
        }
        Some(kind) => objects.push(Object { doc, at, kind }),
        None => at.error("has no `kind`, so it is not a Kubernetes object"),
    }
}

/// `Service "web"`, or the kind alone for an object without a name.
fn describe(object: &Object<'_>) -> String {
    let name = object
        .at
        .value()
        .pointer("/metadata/name")
        .and_then(Value::as_str);
    match name {
        Some(name) => format!("{} `{name}`", object.kind),
        None => object.kind.to_string(),
    }
}

/// Every kind that is neither a workload nor a ConfigMap: one warning or
/// one error, and the content stays unread.
fn other_kind(object: &Object<'_>) {
    let at = &object.at;
    let what = describe(object);
    match object.kind {
        "ConfigMap" => {}
        "Secret" => at.warn(format!(
            "{what} is skipped and its data was not read: faber has no secret store yet"
        )),
        "Job" | "CronJob" => at.error(format!(
            "{what} is not supported: faber runs workloads that stay up, not ones that \
             run to completion"
        )),
        "ReplicationController" => {
            at.error(format!("{what} is not supported: write it as a Deployment"))
        }
        kind if SKIPPED_KINDS.contains(&kind) => {
            at.warn(format!("{what} is skipped: faber has nothing like it"));
        }
        _ => at.error(format!(
            "{what} is of an unknown kind; if it runs containers, write it as a Deployment"
        )),
    }
}

/// The ConfigMaps of the input by namespace and name.
#[derive(Default)]
struct ConfigMaps {
    maps: BTreeMap<(String, String), ConfigMap>,
}

#[derive(Default)]
struct ConfigMap {
    data: BTreeMap<String, String>,
    /// Keys of `binaryData`. The values are never read.
    binary: BTreeSet<String>,
}

impl ConfigMaps {
    fn read(&mut self, at: &Cursor<'_>) {
        at.ignore("apiVersion");
        at.ignore("kind");
        at.ignore("immutable");
        let meta = metadata(at, false);
        let mut map = ConfigMap::default();
        if let Some(data) = at.get("data") {
            for (key, value) in data.entries() {
                if let Some(value) = string_value(&value) {
                    map.data.insert(key.to_string(), value.to_string());
                }
            }
        }
        if let Some(binary) = at.get("binaryData") {
            map.binary = binary.keys().into_iter().map(str::to_string).collect();
        }
        let Some(name) = meta.name else { return };
        let Some(text) = name.as_str() else { return };
        let key = (meta.namespace.to_string(), text.to_string());
        if self.maps.contains_key(&key) {
            name.error(format!(
                "ConfigMap `{text}` is defined twice in namespace `{}`",
                meta.namespace
            ));
            return;
        }
        self.maps.insert(key, map);
    }

    fn get(&self, namespace: &str, name: &str) -> Option<&ConfigMap> {
        self.maps.get(&(namespace.to_string(), name.to_string()))
    }
}

/// What `metadata` of an object says.
struct Meta<'a> {
    /// At `metadata.name`, when it is there and a string.
    name: Option<Cursor<'a>>,
    namespace: &'a str,
    expose: Exposure,
}

/// Reads `metadata`: the name, the namespace and, when `annotated`, the
/// [`EXPOSE_ANNOTATION`]. A missing name is an error.
fn metadata<'a>(object: &Cursor<'a>, annotated: bool) -> Meta<'a> {
    let mut out = Meta {
        name: None,
        namespace: "default",
        expose: Exposure::default(),
    };
    let Some(meta) = object.get("metadata") else {
        object.error("has no `metadata.name`");
        return out;
    };
    for key in IGNORED_METADATA {
        meta.ignore(key);
    }
    if let Some(namespace) = text(&meta, "namespace") {
        out.namespace = namespace;
    }
    let generated = meta.get("generateName");
    match (meta.get("name"), generated) {
        (Some(name), _) => {
            if name.expect_str().is_some() {
                out.name = Some(name);
            }
        }
        (None, Some(generated)) => generated
            .error("a generated name needs a cluster to pick it; set `metadata.name` instead"),
        (None, None) => meta.error("has no `name`"),
    }
    if !annotated {
        meta.ignore("annotations");
    } else if let Some(annotations) = meta.get("annotations") {
        if let Some(expose) = annotations.get(EXPOSE_ANNOTATION) {
            match expose.as_str() {
                Some("cluster") => out.expose = Exposure::Cluster,
                Some("environment") => out.expose = Exposure::Environment,
                _ => expose.error("must be `cluster` or `environment`"),
            }
        }
        for key in annotations.keys() {
            annotations.ignore(key);
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadKind {
    Deployment,
    ReplicaSet,
    StatefulSet,
    DaemonSet,
    Pod,
}

impl WorkloadKind {
    fn of(kind: &str) -> Option<Self> {
        Some(match kind {
            "Deployment" => Self::Deployment,
            "ReplicaSet" => Self::ReplicaSet,
            "StatefulSet" => Self::StatefulSet,
            "DaemonSet" => Self::DaemonSet,
            "Pod" => Self::Pod,
            _ => return None,
        })
    }
}

/// Converts one workload object. The name comes back even when the spec
/// does not, so that a second workload of that name is still noticed.
fn workload<'a>(
    object: &Cursor<'a>,
    kind: WorkloadKind,
    config_maps: &ConfigMaps,
) -> (Option<Cursor<'a>>, Option<WorkloadSpec>) {
    object.ignore("apiVersion");
    object.ignore("kind");
    object.ignore("status");
    let meta = metadata(object, true);
    if let Some(name) = &meta.name {
        check_label(name, "workload name");
    }
    let Some(spec) = object.get("spec") else {
        object.error("has no `spec`");
        return (meta.name, None);
    };

    let mut volumes = Volumes::new();
    let (scale, pod) = if kind == WorkloadKind::Pod {
        (Scale::Replicas(1), Some(spec))
    } else {
        controller(&spec, kind, &mut volumes)
    };
    let Some(pod) = pod else {
        return (meta.name, None);
    };
    let context = Context {
        config_maps,
        namespace: meta.namespace,
        controlled: kind != WorkloadKind::Pod,
    };
    let pod = pod_spec(&pod, &context, volumes);
    let spec = WorkloadSpec {
        scale,
        restart: pod.restart,
        expose: meta.expose,
        placement: Placement { node: pod.node },
        stop_grace_secs: pod.stop_grace_secs,
        volumes: pod.volumes,
        containers: pod.containers,
    };
    (meta.name, Some(spec))
}

/// The fields of a controller around its pod template. Returns the scale
/// and the cursor at the pod spec.
fn controller<'a>(
    spec: &Cursor<'a>,
    kind: WorkloadKind,
    volumes: &mut Volumes<'a>,
) -> (Scale, Option<Cursor<'a>>) {
    for key in [
        "selector",
        "revisionHistoryLimit",
        "progressDeadlineSeconds",
        "minReadySeconds",
    ] {
        spec.ignore(key);
    }
    let scale = if kind == WorkloadKind::DaemonSet {
        Scale::PerNode
    } else {
        Scale::Replicas(whole(spec, "replicas").unwrap_or(1))
    };

    skip(spec, "strategy", "rollout settings are not used");
    skip(spec, "updateStrategy", "rollout settings are not used");
    skip(
        spec,
        "paused",
        "not used: the workload is deployed as written",
    );
    if kind == WorkloadKind::StatefulSet {
        spec.ignore("serviceName");
        if let Some(policy) = spec.get("podManagementPolicy")
            && !matches!(policy.as_str(), Some("" | "OrderedReady"))
        {
            policy.warn("not used: faber starts replicas in no fixed order");
        }
        skip(
            spec,
            "persistentVolumeClaimRetentionPolicy",
            "not used: faber keeps a named volume until it is removed by hand",
        );
        skip(spec, "ordinals", "not used: replicas are not numbered");
        if let Some(claims) = spec.get("volumeClaimTemplates") {
            for claim in claims.items() {
                claim_template(&claim, volumes);
            }
        }
    }

    let Some(template) = spec.get("template") else {
        spec.error("has no `template`");
        return (scale, None);
    };
    template_metadata(&template);
    let pod = template.get("spec");
    if pod.is_none() {
        template.error("has no `spec`");
    }
    (scale, pod)
}

/// The pod template's `metadata`. None of it is read, but the
/// [`EXPOSE_ANNOTATION`] there would be dropped and leave the workload
/// unexposed without a word, so it is refused.
fn template_metadata(template: &Cursor<'_>) {
    let Some(meta) = template.get("metadata") else {
        return;
    };
    // Looked at without the walk first: the rest of the map stays unread.
    let misplaced = meta
        .value()
        .pointer("/annotations")
        .is_some_and(|annotations| annotations.get(EXPOSE_ANNOTATION).is_some());
    if !misplaced {
        return;
    }
    for key in meta.keys() {
        if key != "annotations" {
            meta.ignore(key);
        }
    }
    let Some(annotations) = meta.get("annotations") else {
        return;
    };
    for key in annotations.keys() {
        annotations.ignore(key);
    }
    if let Some(expose) = annotations.get(EXPOSE_ANNOTATION) {
        expose.error(format!(
            "has no effect on the pod template: put `{EXPOSE_ANNOTATION}` in the \
             `metadata.annotations` of the workload itself"
        ));
    }
}

/// One entry of a StatefulSet's `volumeClaimTemplates`: a named volume
/// that every replica gets for itself.
fn claim_template<'a>(claim: &Cursor<'a>, volumes: &mut Volumes<'a>) {
    claim.ignore("apiVersion");
    claim.ignore("kind");
    claim.ignore("status");
    let meta = metadata(claim, false);
    if let Some(spec) = claim.get("spec") {
        spec.ignore("resources");
        spec.ignore("storageClassName");
        spec.ignore("accessModes");
        if let Some(mode) = spec.get("volumeMode")
            && !matches!(mode.as_str(), Some("" | "Filesystem"))
        {
            mode.error("only `Filesystem` volumes are supported");
        }
    }
    let Some(name) = meta.name else { return };
    let Some(text) = name.as_str() else { return };
    if !check_label(&name, "volume name") {
        return;
    }
    claim.warn(format!(
        "volume `{text}` is node-local, not a PersistentVolume: each replica keeps its \
         data on the node it runs on"
    ));
    let source = VolumeSource::Named {
        name: text.to_string(),
        per_replica: true,
    };
    add_volume(
        volumes,
        &name,
        text,
        VolumeKind::Source(source, false),
        claim,
    );
}

/// What a pod spec needs from around it.
struct Context<'c> {
    config_maps: &'c ConfigMaps,
    /// The namespace ConfigMap references are resolved in.
    namespace: &'c str,
    /// Under a controller, which restarts its pods whatever they say.
    controlled: bool,
}

/// The volumes of a pod by name, with what the containers did with them.
type Volumes<'a> = BTreeMap<String, Volume<'a>>;

struct Volume<'a> {
    /// At the source, for a finding about it.
    at: Cursor<'a>,
    kind: VolumeKind,
    /// Whether a container mounts it.
    mounted: bool,
}

enum VolumeKind {
    /// A source the model has, and whether every mount of it is read-only.
    Source(VolumeSource, bool),
    /// A source it does not have, by its key: `configMap`, `nfs`.
    Unsupported(String),
}

fn add_volume<'a>(
    volumes: &mut Volumes<'a>,
    name: &Cursor<'a>,
    text: &str,
    kind: VolumeKind,
    at: &Cursor<'a>,
) {
    if volumes.contains_key(text) {
        name.error(format!("volume `{text}` is defined twice"));
        return;
    }
    let volume = Volume {
        at: at.clone(),
        kind,
        mounted: false,
    };
    volumes.insert(text.to_string(), volume);
}

struct Pod {
    restart: RestartPolicy,
    stop_grace_secs: Option<u32>,
    node: Option<String>,
    volumes: BTreeMap<String, VolumeSource>,
    containers: Vec<ContainerSpec>,
}

/// The user a pod's `securityContext` gives the containers that set none.
#[derive(Default)]
struct PodUser {
    uid: Option<u64>,
    gid: Option<u64>,
}

fn pod_spec<'a>(pod: &Cursor<'a>, context: &Context<'_>, mut volumes: Volumes<'a>) -> Pod {
    let restart = match pod.get("restartPolicy") {
        None => RestartPolicy::Always,
        Some(policy) => match policy.expect_str() {
            Some("" | "Always") | None => RestartPolicy::Always,
            Some("OnFailure" | "Never") if context.controlled => {
                policy.error("must be `Always` under a controller");
                RestartPolicy::Always
            }
            Some("OnFailure") => RestartPolicy::OnFailure,
            Some("Never") => RestartPolicy::Never,
            Some(_) => {
                policy.error("must be `Always`, `OnFailure` or `Never`");
                RestartPolicy::Always
            }
        },
    };
    let stop_grace_secs = whole(pod, "terminationGracePeriodSeconds");

    let node = placement(pod);
    scheduling(pod);
    let user = pod_security(pod);
    pod_refused(pod);
    for key in [
        "automountServiceAccountToken",
        "enableServiceLinks",
        "overhead",
    ] {
        pod.ignore(key);
    }
    for (key, message) in [
        (
            "imagePullSecrets",
            "not used: faber has no registry credentials yet",
        ),
        (
            "serviceAccountName",
            "not used: faber has no service accounts",
        ),
        ("serviceAccount", "not used: faber has no service accounts"),
        ("hostname", "not used: faber names the pod"),
        ("subdomain", "not used: faber names the pod"),
        ("setHostnameAsFQDN", "not used: faber names the pod"),
    ] {
        skip(pod, key, message);
    }

    refuse(
        pod,
        "initContainers",
        "init containers are not supported: faber starts every container of a workload together",
    );
    refuse(
        pod,
        "ephemeralContainers",
        "ephemeral containers are not supported",
    );

    if let Some(list) = pod.get("volumes") {
        for entry in list.items() {
            pod_volume(&entry, &mut volumes);
        }
    }

    let mut containers = Vec::new();
    match pod.get("containers") {
        Some(list) if !is_noop(&list) => {
            for item in list.items() {
                if let Some(spec) = container(&item, context, &user, &mut volumes) {
                    containers.push(spec);
                }
            }
        }
        _ => pod.error("has no `containers`"),
    }

    let mut sources = BTreeMap::new();
    for (name, volume) in volumes {
        match volume.kind {
            VolumeKind::Source(source, _) => {
                sources.insert(name, source);
            }
            VolumeKind::Unsupported(key) if volume.mounted => {
                let hint = match key.as_str() {
                    "configMap" => {
                        "; pass the values as environment variables with `configMapKeyRef` \
                         or `envFrom`"
                    }
                    "secret" => "; faber has no secret store yet",
                    _ => "",
                };
                volume
                    .at
                    .error(format!("`{key}` volumes are not supported{hint}"));
            }
            VolumeKind::Unsupported(key) => volume.at.warn(format!(
                "`{key}` volumes are not supported; skipped because no container mounts it"
            )),
        }
    }

    Pod {
        restart,
        stop_grace_secs,
        node,
        volumes: sources,
        containers,
    }
}

/// `nodeName` and `nodeSelector`: the node the workload is pinned to.
fn placement(pod: &Cursor<'_>) -> Option<String> {
    let mut node = text(pod, "nodeName").map(str::to_string);
    let Some(selector) = pod.get("nodeSelector") else {
        return node;
    };
    for (key, value) in selector.entries() {
        if key != HOSTNAME_LABEL {
            value.error(format!(
                "only `{HOSTNAME_LABEL}` is supported: faber nodes have no labels"
            ));
            continue;
        }
        let Some(host) = value.expect_str() else {
            continue;
        };
        match &node {
            Some(named) if named != host => {
                value.error(format!("conflicts with `nodeName: {named}`"));
            }
            _ => node = Some(host.to_string()),
        }
    }
    node
}

/// Scheduling fields. Only a required node affinity is refused: it limits
/// where the pod may run, and faber cannot keep that promise.
fn scheduling(pod: &Cursor<'_>) {
    const NOT_USED: &str = "not used: faber places workloads itself";
    if let Some(affinity) = pod.get("affinity").filter(|c| !is_noop(c)) {
        let mut preferences = false;
        if let Some(node) = affinity.get("nodeAffinity") {
            refuse(
                &node,
                "requiredDuringSchedulingIgnoredDuringExecution",
                "required node affinity is not supported: pin the workload with `nodeName`, \
                 or remove the rule",
            );
            preferences |=
                present(&node, "preferredDuringSchedulingIgnoredDuringExecution").is_some();
        }
        preferences |= present(&affinity, "podAffinity").is_some();
        preferences |= present(&affinity, "podAntiAffinity").is_some();
        if preferences {
            affinity.warn(NOT_USED);
        }
    }
    for key in [
        "tolerations",
        "topologySpreadConstraints",
        "priorityClassName",
        "priority",
        "readinessGates",
        "schedulingGates",
    ] {
        skip(pod, key, NOT_USED);
    }
    for (key, default) in [
        ("preemptionPolicy", "PreemptLowerPriority"),
        ("schedulerName", "default-scheduler"),
    ] {
        if let Some(at) = present(pod, key)
            && at.as_str() != Some(default)
        {
            at.warn(NOT_USED);
        }
    }
}

/// The pod's `securityContext`. Returns the user it sets for containers.
fn pod_security(pod: &Cursor<'_>) -> PodUser {
    let mut user = PodUser::default();
    let Some(context) = pod.get("securityContext") else {
        return user;
    };
    user.uid = context.get("runAsUser").and_then(|c| c.expect_u64());
    if let Some(group) = context.get("runAsGroup") {
        match (user.uid, group.expect_u64()) {
            (Some(_), gid) => user.gid = gid,
            (None, Some(_)) => group.error("a group needs a user: set `runAsUser` next to it"),
            (None, None) => {}
        }
    }
    skip(&context, "runAsNonRoot", NOT_ENFORCED);
    skip(
        &context,
        "fsGroup",
        "not enforced: volume ownership is not changed",
    );
    skip(
        &context,
        "fsGroupChangePolicy",
        "not enforced: volume ownership is not changed",
    );
    skip(&context, "seLinuxOptions", NOT_ENFORCED);
    skip(&context, "appArmorProfile", NOT_ENFORCED);
    seccomp(&context);
    refuse(&context, "sysctls", "sysctls are not supported");
    refuse(
        &context,
        "supplementalGroups",
        "supplemental groups are not supported",
    );
    refuse(
        &context,
        "windowsOptions",
        "Windows containers are not supported",
    );
    user
}

/// `seccompProfile`: the runtime's default is what every container gets.
fn seccomp(context: &Cursor<'_>) {
    let Some(profile) = context.get("seccompProfile") else {
        return;
    };
    profile.ignore("localhostProfile");
    if let Some(kind) = profile.get("type")
        && !matches!(kind.as_str(), Some("" | "RuntimeDefault"))
    {
        kind.error(
            "only `RuntimeDefault` is supported: containers run under the runtime's profile",
        );
    }
}

/// Pod fields that change how the containers run and have no place in the
/// model.
fn pod_refused(pod: &Cursor<'_>) {
    for (key, message) in [
        ("hostNetwork", "the host network is not supported"),
        ("hostPID", "the host PID namespace is not supported"),
        ("hostIPC", "the host IPC namespace is not supported"),
        (
            "shareProcessNamespace",
            "a shared process namespace is not supported",
        ),
        ("hostAliases", "host aliases are not supported"),
        (
            "dnsConfig",
            "DNS settings are not supported: faber configures DNS",
        ),
        ("runtimeClassName", "runtime classes are not supported"),
        ("resourceClaims", "resource claims are not supported"),
        (
            "resources",
            "pod-level resources are not supported: set them on each container",
        ),
        (
            "activeDeadlineSeconds",
            "a deadline is not supported: faber runs workloads that stay up",
        ),
    ] {
        refuse(pod, key, message);
    }
    if let Some(users) = pod.get("hostUsers")
        && users.expect_bool() == Some(false)
    {
        users.error("user namespaces are not supported");
    }
    if let Some(policy) = pod.get("dnsPolicy")
        && !matches!(policy.as_str(), Some("" | "ClusterFirst" | "Default"))
    {
        policy.error("only `ClusterFirst` and `Default` are supported: faber configures DNS");
    }
    if let Some(os) = pod.get("os")
        && let Some(name) = os.get("name")
        && !matches!(name.as_str(), Some("" | "linux"))
    {
        name.error("only Linux containers are supported");
    }
}

/// One entry of the pod's `volumes`.
fn pod_volume<'a>(entry: &Cursor<'a>, volumes: &mut Volumes<'a>) {
    // Take every key. A source with a null value is as good as absent.
    let sources: Vec<(&str, Cursor<'a>)> = entry
        .keys()
        .into_iter()
        .filter(|key| *key != "name")
        .filter_map(|key| entry.get(key).map(|at| (key, at)))
        .collect();
    let Some(name) = entry.get("name") else {
        entry.error("has no `name`");
        return;
    };
    let Some(volume_name) = name.expect_str() else {
        return;
    };
    if !check_label(&name, "volume name") {
        return;
    }
    let (key, at) = match sources.as_slice() {
        [one] => one,
        [] => {
            entry
                .error("has no source: expected `hostPath`, `emptyDir` or `persistentVolumeClaim`");
            return;
        }
        _ => {
            entry.error("has more than one source");
            return;
        }
    };
    let kind = match *key {
        "hostPath" => {
            let create = match at.get("type") {
                None => Some(false),
                Some(kind) => match kind.expect_str() {
                    Some("" | "Directory" | "File") => Some(false),
                    Some("DirectoryOrCreate") => Some(true),
                    Some(other) => {
                        kind.error(format!(
                            "host path type `{other}` is not supported: use `Directory`, \
                             `DirectoryOrCreate` or `File`"
                        ));
                        None
                    }
                    None => None,
                },
            };
            let path = text(at, "path").map(clean_path);
            if path.is_none() {
                at.error("has no `path`");
            }
            let (Some(path), Some(create)) = (path, create) else {
                return;
            };
            VolumeKind::Source(VolumeSource::HostPath { path, create }, false)
        }
        "emptyDir" => {
            let memory = match at.get("medium") {
                None => false,
                Some(medium) => match medium.expect_str() {
                    Some("Memory") => true,
                    Some("") | None => false,
                    Some(other) => {
                        medium.error(format!(
                            "medium `{other}` is not supported: use `Memory` or leave it out"
                        ));
                        false
                    }
                },
            };
            let size_bytes = at
                .get("sizeLimit")
                .and_then(|limit| quantity(&limit, k8s_memory_bytes));
            VolumeKind::Source(VolumeSource::Ephemeral { memory, size_bytes }, false)
        }
        "persistentVolumeClaim" => {
            let read_only = flag(at, "readOnly");
            let Some(claim) = at.get("claimName") else {
                at.error("has no `claimName`");
                return;
            };
            let Some(claim_name) = claim.expect_str() else {
                return;
            };
            if !check_label(&claim, "claim name") {
                return;
            }
            at.warn(format!(
                "volume `{claim_name}` is node-local, not a PersistentVolume: the data stays \
                 on the node the pod runs on"
            ));
            VolumeKind::Source(
                VolumeSource::Named {
                    name: claim_name.to_string(),
                    per_replica: false,
                },
                read_only,
            )
        }
        other => VolumeKind::Unsupported(other.to_string()),
    };
    add_volume(volumes, &name, volume_name, kind, at);
}

fn container<'a>(
    at: &Cursor<'a>,
    context: &Context<'_>,
    pod_user: &PodUser,
    volumes: &mut Volumes<'a>,
) -> Option<ContainerSpec> {
    let name = match at.get("name") {
        Some(name) => {
            check_label(&name, "container name");
            name.as_str()
        }
        None => {
            at.error("has no `name`");
            None
        }
    };
    let image = match at.get("image") {
        Some(image) => image.expect_str(),
        None => {
            at.error("has no `image`");
            None
        }
    };

    let mut spec = ContainerSpec::new(name.unwrap_or_default(), image.unwrap_or_default());
    spec.working_dir = text(at, "workingDir").map(clean_path);
    spec.env = environment(at, context);
    spec.command = expanded_strings(at, "command", &spec.env);
    spec.args = expanded_strings(at, "args", &spec.env);
    spec.ports = ports(at);
    spec.resources = resources(at);
    spec.mounts = mounts(at, volumes);
    spec.liveness = probe(at, "livenessProbe", &spec.ports);
    spec.readiness = probe(at, "readinessProbe", &spec.ports);
    spec.startup = probe(at, "startupProbe", &spec.ports);
    container_security(at, pod_user, &mut spec);

    for (key, message) in [
        ("lifecycle", "lifecycle hooks are not supported"),
        ("volumeDevices", "block devices are not supported"),
        (
            "restartPolicy",
            "a restart policy per container is not supported: set it on the pod",
        ),
        ("stdin", "an open stdin is not supported"),
        ("stdinOnce", "an open stdin is not supported"),
        ("tty", "a TTY is not supported"),
    ] {
        refuse(at, key, message);
    }
    if let Some(policy) = at.get("imagePullPolicy")
        && !matches!(policy.as_str(), Some("" | "IfNotPresent"))
    {
        policy.warn("not used: faber pulls an image when the node does not have it");
    }
    for key in [
        "terminationMessagePath",
        "terminationMessagePolicy",
        "resizePolicy",
    ] {
        at.ignore(key);
    }

    // Without either there is an error already, and nothing worth keeping.
    (name.is_some() && image.is_some()).then_some(spec)
}

/// The container's environment as the kubelet builds it: `envFrom` in
/// order, then `env` in order, each value expanded against what is set
/// before it.
fn environment(at: &Cursor<'_>, context: &Context<'_>) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    if let Some(list) = at.get("envFrom") {
        for from in list.items() {
            let prefix = text(&from, "prefix").unwrap_or_default();
            refuse(&from, "secretRef", NO_SECRET_STORE);
            let Some(reference) = from.get("configMapRef") else {
                continue;
            };
            let optional = flag(&reference, "optional");
            let Some(name) = text(&reference, "name") else {
                reference.error("has no `name`");
                continue;
            };
            let Some(map) = config_map(&reference, name, optional, context) else {
                continue;
            };
            for (key, value) in &map.data {
                let variable = format!("{prefix}{key}");
                if is_env_name(&variable) {
                    env.insert(variable, value.clone());
                } else {
                    reference.warn(format!(
                        "key `{key}` of ConfigMap `{name}` is skipped: `{variable}` is not \
                         a valid environment variable name"
                    ));
                }
            }
        }
    }

    let Some(list) = at.get("env") else {
        return env;
    };
    let mut declared = BTreeSet::new();
    for item in list.items() {
        let name = item.get("name");
        let value = item.get("value").filter(|v| v.as_str() != Some(""));
        let from = item.get("valueFrom").filter(|f| !is_noop(f));
        let resolved = match (value, from) {
            (Some(_), Some(from)) => {
                from.error("cannot be set next to `value`");
                None
            }
            (Some(value), None) => {
                string_value(&value).and_then(|text| expand_at(&value, text, &env))
            }
            (None, Some(from)) => value_from(&from, context),
            (None, None) => Some(String::new()),
        };
        let Some(name) = name else {
            item.error("has no `name`");
            continue;
        };
        let Some(variable) = name.expect_str() else {
            continue;
        };
        if !declared.insert(variable) {
            name.warn(format!(
                "`{variable}` is set more than once; the last value is used"
            ));
        }
        if let Some(resolved) = resolved {
            env.insert(variable.to_string(), resolved);
        }
    }
    env
}

/// The ConfigMap `name` of the pod's namespace. A missing one is a warning
/// when the reference is optional, else an error.
fn config_map<'c>(
    reference: &Cursor<'_>,
    name: &str,
    optional: bool,
    context: &Context<'c>,
) -> Option<&'c ConfigMap> {
    let found = context.config_maps.get(context.namespace, name);
    if found.is_none() {
        let missing = format!(
            "ConfigMap `{name}` is not in the input for namespace `{}`",
            context.namespace
        );
        if optional {
            reference.warn(format!("{missing}; skipped because it is optional"));
        } else {
            reference.error(format!("{missing}; add its manifest to the deploy"));
        }
    }
    found
}

/// `env[].valueFrom`: only a ConfigMap key can be resolved without a
/// cluster.
fn value_from(from: &Cursor<'_>, context: &Context<'_>) -> Option<String> {
    refuse(from, "secretKeyRef", NO_SECRET_STORE);
    for key in ["fieldRef", "resourceFieldRef"] {
        refuse(
            from,
            key,
            "pod fields are not known at deploy time: write the value out",
        );
    }
    let reference = from.get("configMapKeyRef")?;
    let optional = flag(&reference, "optional");
    let name = text(&reference, "name");
    let key = text(&reference, "key");
    if name.is_none() {
        reference.error("has no `name`");
    }
    if key.is_none() {
        reference.error("has no `key`");
    }
    let (name, key) = (name?, key?);
    let map = config_map(&reference, name, optional, context)?;
    if let Some(value) = map.data.get(key) {
        return Some(value.clone());
    }
    if map.binary.contains(key) {
        reference.error(format!(
            "key `{key}` of ConfigMap `{name}` is binary data, which cannot be an \
             environment variable"
        ));
    } else if optional {
        reference.warn(format!(
            "ConfigMap `{name}` has no key `{key}`; skipped because it is optional"
        ));
    } else {
        reference.error(format!("ConfigMap `{name}` has no key `{key}`"));
    }
    None
}

/// `[A-Za-z_][A-Za-z0-9_.-]*`, the model's rule for a variable name.
fn is_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// The list of strings at `key` with `$(NAME)` replaced from `env`.
fn expanded_strings(parent: &Cursor<'_>, key: &str, env: &BTreeMap<String, String>) -> Vec<String> {
    let Some(list) = parent.get(key) else {
        return Vec::new();
    };
    list.items()
        .iter()
        .filter_map(|item| expand_at(item, string_value(item)?, env))
        .collect()
}

/// [`expand`], with an error at `at` for a result over the limit.
fn expand_at(at: &Cursor<'_>, text: &str, env: &BTreeMap<String, String>) -> Option<String> {
    let expanded = expand(text, env);
    if expanded.is_none() {
        at.error("is longer than 64 KiB once `$(VAR)` references are replaced");
    }
    expanded
}

/// Replaces `$(NAME)` by the variable of that name, as the kubelet does.
/// A reference to a variable that is not set stays as written, `$$` is a
/// literal `$` (so `$$(NAME)` gives `$(NAME)`), and a `$(` that is never
/// closed stays too.
///
/// `None` when the result is over [`MAX_EXPANDED_BYTES`]. References can
/// repeat a long value, so the result is bounded while it is built.
fn expand(input: &str, env: &BTreeMap<String, String>) -> Option<String> {
    let mut out = String::with_capacity(input.len().min(MAX_EXPANDED_BYTES));
    let mut rest = input;
    // Once no `)` is left, none will be found later either. Without this
    // every unclosed `$(` would search the rest of the string again.
    let mut closers_left = true;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        if out.len() > MAX_EXPANDED_BYTES {
            return None;
        }
        let after = &rest[dollar + 1..];
        let reference = after
            .strip_prefix('(')
            .filter(|_| closers_left)
            .and_then(|tail| {
                let close = tail.find(')');
                closers_left = close.is_some();
                close.map(|close| (tail, close))
            });
        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
        } else if let Some((tail, close)) = reference {
            let name = &tail[..close];
            match env.get(name) {
                Some(value) => out.push_str(value),
                None => {
                    out.push_str("$(");
                    out.push_str(name);
                    out.push(')');
                }
            }
            rest = &tail[close + 1..];
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    (out.len() <= MAX_EXPANDED_BYTES).then_some(out)
}

fn ports(at: &Cursor<'_>) -> Vec<PortSpec> {
    let mut out = Vec::new();
    let Some(list) = at.get("ports") else {
        return out;
    };
    for item in list.items() {
        skip(
            &item,
            "hostPort",
            "not published: faber opens no port on the node",
        );
        skip(
            &item,
            "hostIP",
            "not published: faber opens no port on the node",
        );
        let name = text(&item, "name").map(str::to_string);
        let protocol = match item.get("protocol") {
            None => Some(Protocol::Tcp),
            Some(protocol) => match protocol.expect_str() {
                Some("" | "TCP") => Some(Protocol::Tcp),
                Some("UDP") => Some(Protocol::Udp),
                Some("SCTP") => {
                    protocol.error("SCTP is not supported: use `TCP` or `UDP`");
                    None
                }
                Some(_) => {
                    protocol.error("must be `TCP` or `UDP`");
                    None
                }
                None => None,
            },
        };
        let port = match item.get("containerPort") {
            Some(port) => port_number(&port),
            None => {
                item.error("has no `containerPort`");
                None
            }
        };
        if let (Some(port), Some(protocol)) = (port, protocol) {
            out.push(PortSpec {
                name,
                port,
                protocol,
            });
        }
    }
    out
}

/// A port from 1 to 65535.
fn port_number(at: &Cursor<'_>) -> Option<u16> {
    let port = at.as_u64().and_then(|n| u16::try_from(n).ok());
    if !port.is_some_and(|p| p > 0) {
        at.error("expected a port from 1 to 65535");
        return None;
    }
    port
}

fn resources(at: &Cursor<'_>) -> Resources {
    let Some(resources) = at.get("resources") else {
        return Resources::default();
    };
    refuse(&resources, "claims", "resource claims are not supported");
    Resources {
        limits: amounts(&resources, "limits"),
        requests: amounts(&resources, "requests"),
    }
}

fn amounts(resources: &Cursor<'_>, key: &str) -> ResourceAmounts {
    let mut out = ResourceAmounts::default();
    let Some(map) = resources.get(key) else {
        return out;
    };
    for (name, amount) in map.entries() {
        if amount.is_null() {
            continue;
        }
        match name {
            "cpu" => out.cpu_millis = quantity(&amount, k8s_cpu_millis),
            "memory" => out.memory_bytes = quantity(&amount, k8s_memory_bytes),
            "ephemeral-storage" => amount.warn("not enforced: faber does not limit disk use"),
            _ => amount.error(format!(
                "resource `{name}` is not supported: only `cpu` and `memory` are"
            )),
        }
    }
    out
}

/// A quantity as it was written. YAML has already turned an unquoted
/// `1` or `0.5` into a number, and the number's own text is what the
/// parser reads then.
fn quantity(at: &Cursor<'_>, parse: fn(&str) -> Result<u64, UnitError>) -> Option<u64> {
    let text = match at.value() {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => {
            at.error(format!("expected a quantity, found {}", at.kind()));
            return None;
        }
    };
    // Zero means "not set" in Kubernetes, and the model has no zero
    // amount, so it is left out. The unit must still be a real one, which
    // the parser tells from the same text with a one in front. `m` is a
    // real unit for every quantity.
    if let Some(unit) = zero_unit(&text) {
        if unit != "m" && parse(&format!("1{unit}")).is_err() {
            at.error(format!("`{text}` is not a quantity"));
        }
        return None;
    }
    match parse(&text) {
        Ok(amount) => Some(amount),
        Err(e) => {
            at.error(e.0);
            None
        }
    }
}

/// What follows the number of a quantity that is zero (`0`, `0.0`, `+0`):
/// the suffix or exponent, which may be empty. `None` for any other text.
fn zero_unit(text: &str) -> Option<&str> {
    let number = text.strip_prefix(['+', '-']).unwrap_or(text);
    let len = number
        .bytes()
        .take_while(|b| b.is_ascii_digit() || *b == b'.')
        .count();
    let (digits, unit) = number.split_at(len);
    let zero = digits.contains('0')
        && digits.bytes().all(|b| b == b'0' || b == b'.')
        && digits.matches('.').count() <= 1;
    zero.then_some(unit)
}

fn mounts<'a>(at: &Cursor<'a>, volumes: &mut Volumes<'a>) -> Vec<MountSpec> {
    let mut out = Vec::new();
    let Some(list) = at.get("volumeMounts") else {
        return out;
    };
    for item in list.items() {
        refuse(
            &item,
            "subPathExpr",
            "expanded sub-paths are not supported: use `subPath`",
        );
        if let Some(propagation) = item.get("mountPropagation")
            && !matches!(propagation.as_str(), Some("" | "None"))
        {
            propagation.error("mount propagation is not supported");
        }
        if let Some(recursive) = item.get("recursiveReadOnly")
            && !matches!(recursive.as_str(), Some("" | "Disabled"))
        {
            recursive.warn(NOT_ENFORCED);
        }
        let read_only = flag(&item, "readOnly");
        let sub_path = text(&item, "subPath").map(str::to_string);
        let path = text(&item, "mountPath").map(clean_path);
        if path.is_none() {
            item.error("has no `mountPath`");
        }
        let Some(name) = item.get("name") else {
            item.error("has no `name`");
            continue;
        };
        let Some(volume_name) = name.expect_str() else {
            continue;
        };
        let Some(volume) = volumes.get_mut(volume_name) else {
            name.error(format!(
                "no volume named `{volume_name}` in the pod's `volumes`"
            ));
            continue;
        };
        volume.mounted = true;
        if let (VolumeKind::Source(_, volume_read_only), Some(path)) = (&volume.kind, path) {
            out.push(MountSpec {
                volume: volume_name.to_string(),
                path,
                read_only: read_only || *volume_read_only,
                sub_path,
            });
        }
    }
    out
}

fn probe(container: &Cursor<'_>, key: &str, ports: &[PortSpec]) -> Option<Probe> {
    let at = present(container, key)?;
    let mut actions = Vec::new();
    let mut refused = false;

    if let Some(exec) = at.get("exec") {
        let command = strings(&exec, "command");
        if command.is_empty() {
            exec.error("has no `command`");
            refused = true;
        } else {
            actions.push(ProbeAction::Exec { command });
        }
    }
    if let Some(http) = at.get("httpGet") {
        let scheme = match http.get("scheme") {
            None => Some(HttpScheme::Http),
            Some(scheme) => match scheme.expect_str() {
                Some("" | "HTTP") => Some(HttpScheme::Http),
                Some("HTTPS") => Some(HttpScheme::Https),
                Some(_) => {
                    scheme.error("must be `HTTP` or `HTTPS`");
                    None
                }
                None => None,
            },
        };
        let mut headers = BTreeMap::new();
        let mut host = None;
        if let Some(list) = http.get("httpHeaders") {
            for header in list.items() {
                let value = header
                    .get("value")
                    .and_then(|v| string_value(&v))
                    .unwrap_or_default();
                let Some(name) = header.get("name") else {
                    header.error("has no `name`");
                    continue;
                };
                let Some(text) = name.expect_str() else {
                    continue;
                };
                // The `Host` header has its own field in the model.
                let again = if text.eq_ignore_ascii_case("host") {
                    host.replace(value.to_string()).is_some()
                } else {
                    headers
                        .insert(text.to_string(), value.to_string())
                        .is_some()
                };
                if again {
                    name.error(format!("header `{text}` is set more than once"));
                }
            }
        }
        refuse(
            &http,
            "host",
            "probes always connect to the pod; to set the Host header use httpHeaders",
        );
        let path = text(&http, "path").unwrap_or("/").to_string();
        match (probe_port(&http, ports), scheme) {
            (Some(port), Some(scheme)) => actions.push(ProbeAction::Http {
                port,
                path,
                scheme,
                host,
                headers,
            }),
            _ => refused = true,
        }
    }
    if let Some(tcp) = at.get("tcpSocket") {
        refuse(
            &tcp,
            "host",
            "a probe of another host is not supported: the probe goes to the pod",
        );
        match probe_port(&tcp, ports) {
            Some(port) => actions.push(ProbeAction::Tcp { port }),
            None => refused = true,
        }
    }
    if let Some(grpc) = at.get("grpc") {
        grpc.error("gRPC probes are not supported: use `httpGet`, `tcpSocket` or `exec`");
        refused = true;
    }
    skip(
        &at,
        "terminationGracePeriodSeconds",
        "not used: the pod's `terminationGracePeriodSeconds` applies",
    );

    // Read before the action is checked, so the keys are taken either way.
    let timings = [
        "initialDelaySeconds",
        "periodSeconds",
        "timeoutSeconds",
        "successThreshold",
        "failureThreshold",
    ]
    .map(|key| whole(&at, key));

    let action = match (actions.pop(), actions.is_empty()) {
        (Some(action), true) if !refused => action,
        _ => {
            if !refused {
                at.error("needs exactly one of `exec`, `httpGet` and `tcpSocket`");
            }
            return None;
        }
    };
    let mut probe = Probe::new(action);
    let fields = [
        &mut probe.initial_delay_secs,
        &mut probe.period_secs,
        &mut probe.timeout_secs,
        &mut probe.success_threshold,
        &mut probe.failure_threshold,
    ];
    for (field, value) in fields.into_iter().zip(timings) {
        if let Some(value) = value {
            *field = value;
        }
    }
    Some(probe)
}

/// The `port` of a probe: a number, or the name of one of the container's
/// ports.
fn probe_port(handler: &Cursor<'_>, ports: &[PortSpec]) -> Option<u16> {
    let Some(port) = handler.get("port") else {
        handler.error("has no `port`");
        return None;
    };
    let Some(name) = port.as_str() else {
        return port_number(&port);
    };
    let named = ports.iter().find(|p| p.name.as_deref() == Some(name));
    // The kubelet reads a name that no port has as a number.
    let found = named
        .map(|p| p.port)
        .or_else(|| name.parse::<u16>().ok().filter(|p| *p > 0));
    if found.is_none() {
        port.error(format!(
            "no port named `{name}` in this container's `ports`"
        ));
    }
    found
}

/// The container's `securityContext`, on top of the pod's user.
fn container_security(at: &Cursor<'_>, pod_user: &PodUser, spec: &mut ContainerSpec) {
    let context = at.get("securityContext");
    let mut uid = pod_user.uid;
    let mut gid = pod_user.gid;
    if let Some(context) = &context {
        if let Some(user) = context.get("runAsUser").and_then(|c| c.expect_u64()) {
            uid = Some(user);
        }
        if let Some(group) = context.get("runAsGroup") {
            match (uid, group.expect_u64()) {
                (Some(_), Some(id)) => gid = Some(id),
                (None, Some(_)) => {
                    group.error("a group needs a user: set `runAsUser` next to it");
                }
                (_, None) => {}
            }
        }
    }
    spec.user = match (uid, gid) {
        (Some(uid), Some(gid)) => Some(format!("{uid}:{gid}")),
        (Some(uid), None) => Some(uid.to_string()),
        (None, _) => None,
    };
    let Some(context) = context else { return };

    if let Some(capabilities) = context.get("capabilities") {
        capabilities.ignore("drop");
        if let Some(add) = capabilities.get("add") {
            for item in add.items() {
                let Some(text) = item.expect_str() else {
                    continue;
                };
                let upper = text.to_ascii_uppercase();
                let name = upper.strip_prefix("CAP_").unwrap_or(&upper);
                if name == "ALL" {
                    item.error("`ALL` is refused: list the capabilities the container needs");
                } else if ALLOWED_CAPABILITIES.contains(&name) {
                    // Named twice is granted once.
                    if !spec.capabilities.iter().any(|have| have == name) {
                        spec.capabilities.push(name.to_string());
                    }
                } else if CAPABILITIES.contains(&name) {
                    item.error(format!(
                        "capability `{name}` is not allowed: it gives control of the \
                         node. Allowed: {}",
                        ALLOWED_CAPABILITIES.join(", ")
                    ));
                } else {
                    item.error(format!("`{text}` is not a Linux capability"));
                }
            }
        }
    }
    spec.read_only_root_fs = flag(&context, "readOnlyRootFilesystem");
    refuse(
        &context,
        "privileged",
        "privileged containers are not supported: list the capabilities the container \
         needs under `capabilities.add`",
    );
    refuse(
        &context,
        "allowPrivilegeEscalation",
        "privilege escalation is not supported: every container runs with \
         no-new-privileges",
    );
    if let Some(mount) = context.get("procMount")
        && !matches!(mount.as_str(), Some("" | "Default"))
    {
        mount.error("only the default `/proc` mount is supported");
    }
    refuse(
        &context,
        "windowsOptions",
        "Windows containers are not supported",
    );
    seccomp(&context);
    skip(&context, "runAsNonRoot", NOT_ENFORCED);
    skip(&context, "seLinuxOptions", NOT_ENFORCED);
    skip(&context, "appArmorProfile", NOT_ENFORCED);
}

/// Whether the value is one that sets nothing: `false`, `null`, `[]`,
/// `{}` or `""`.
fn is_noop(at: &Cursor<'_>) -> bool {
    match at.value() {
        Value::Null => true,
        Value::Bool(set) => !set,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
        Value::Number(_) => false,
    }
}

/// Takes `key` and returns its cursor unless the value sets nothing.
/// Whatever is below the key is taken with it.
fn present<'a>(parent: &Cursor<'a>, key: &str) -> Option<Cursor<'a>> {
    parent.get(key).filter(|at| !is_noop(at))
}

/// An error at `key` when it sets something.
fn refuse(parent: &Cursor<'_>, key: &str, message: &str) {
    if let Some(at) = present(parent, key) {
        at.error(message);
    }
}

/// A warning at `key` when it sets something.
fn skip(parent: &Cursor<'_>, key: &str, message: &str) {
    if let Some(at) = present(parent, key) {
        at.warn(message);
    }
}

/// The string at `key`. An empty one counts as absent.
fn text<'a>(parent: &Cursor<'a>, key: &str) -> Option<&'a str> {
    parent.get(key)?.expect_str().filter(|s| !s.is_empty())
}

/// The boolean at `key`, false when absent.
fn flag(parent: &Cursor<'_>, key: &str) -> bool {
    parent
        .get(key)
        .and_then(|at| at.expect_bool())
        .unwrap_or(false)
}

/// The whole number at `key`.
fn whole(parent: &Cursor<'_>, key: &str) -> Option<u32> {
    let at = parent.get(key)?;
    let number = at.expect_u64()?;
    let fits = u32::try_from(number).ok();
    if fits.is_none() {
        at.error("is too large");
    }
    fits
}

/// The list of strings at `key`, empty when absent.
fn strings(parent: &Cursor<'_>, key: &str) -> Vec<String> {
    let Some(list) = parent.get(key) else {
        return Vec::new();
    };
    list.items()
        .iter()
        .filter_map(|item| string_value(item))
        .map(str::to_string)
        .collect()
}

/// A value that must be a string. YAML turns an unquoted `8080` or `true`
/// into a number or a boolean, and Kubernetes refuses those here, so the
/// error names the fix.
fn string_value<'a>(at: &Cursor<'a>) -> Option<&'a str> {
    match at.value() {
        Value::Number(_) | Value::Bool(_) => {
            at.error(format!(
                "expected a string, found {}: quote the value",
                at.kind()
            ));
            None
        }
        _ => at.expect_str(),
    }
}

/// A path in the one spelling the model takes: no `//`, no `.` component
/// and no `/` at the end. Kubernetes accepts all three and means the same
/// directory. A `..` stays, for the model to refuse, and a path that is
/// not absolute stays relative.
fn clean_path(path: &str) -> String {
    let parts: Vec<&str> = path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    let root = if path.starts_with('/') { "/" } else { "" };
    format!("{root}{}", parts.join("/"))
}

/// Checks that the string at `at` is a DNS label. Nothing is renamed, so
/// anything else is an error.
fn check_label(at: &Cursor<'_>, what: &str) -> bool {
    let Some(name) = at.expect_str() else {
        return false;
    };
    match validate_dns_label(name) {
        Ok(()) => true,
        Err(rule) => {
            let rule = rule.strip_prefix("name ").unwrap_or(&rule);
            at.error(format!(
                "{what} `{name}` is not a DNS label: it {rule}. Rename it in the manifest"
            ));
            false
        }
    }
}

#[cfg(test)]
mod tests;
