//! A cursor over a parsed document that records which keys a converter took.
//!
//! A converter makes one [`Walk`] per document, reads the document through
//! [`Cursor`]s, and calls [`Walk::finish`]. Every key of every map the
//! converter looked into must have been taken by then, with
//! [`Cursor::get`], [`Cursor::ignore`] or [`Cursor::entries`]. Each key
//! that was not is an error with its path.
//!
//! ```
//! use faber_workload_import::walk::Walk;
//! use serde_json::json;
//!
//! let doc = json!({"services": {"web": {"image": "nginx", "privileged": true}}});
//! let walk = Walk::new("compose.yaml");
//! let root = walk.root(&doc);
//! if let Some(services) = root.get("services") {
//!     for (_name, service) in services.entries() {
//!         let image = service.get("image").and_then(|c| c.expect_str());
//!         assert_eq!(image, Some("nginx"));
//!     }
//! }
//! let found = walk.finish();
//! assert_eq!(
//!     found.errors[0].to_string(),
//!     "compose.yaml: services.web.privileged: unsupported key"
//! );
//! ```
//!
//! The rules:
//!
//! - A map counts as looked into once `get`, `ignore` or `entries` was
//!   called on it. A map that was only taken as a key of its parent, and
//!   never looked into, is taken whole: nothing below it is checked. That
//!   is what `ignore("labels")` relies on.
//! - `get` treats a null value like an absent key and still takes it, so
//!   `ports:` with nothing after it is the same as no `ports`.
//! - Reading a value as the wrong shape is an error at its path, not a
//!   silent miss: `get` on a string, `items` on a map, `expect_str` on a
//!   number.
//! - Paths are `a.b[0].c`. A key that is not made of letters, digits, `_`
//!   and `-` is written `a["kubernetes.io/hostname"]`.
//! - Diagnostics come out in the order they were recorded. The errors for
//!   keys nobody took follow them, ordered by the path of their map and
//!   then by key.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{Diagnostic, Diagnostics};

/// The message of the error [`Walk::finish`] records for a key nobody took.
pub const UNSUPPORTED_KEY: &str = "unsupported key";

/// One document's walk: who took which key, and what was found.
#[derive(Debug)]
pub struct Walk {
    source: String,
    /// Every map that was looked into, by path.
    opened: RefCell<BTreeMap<String, Opened>>,
    /// Paths that already have a wrong-shape error, so that asking twice
    /// reports once.
    misshapen: RefCell<BTreeSet<String>>,
    found: RefCell<Diagnostics>,
}

#[derive(Debug)]
struct Opened {
    /// The keys of the map, sorted.
    keys: Vec<String>,
    taken: BTreeSet<String>,
}

impl Walk {
    /// A walk over one document. `source` is what its diagnostics name:
    /// the file, or the file and the document number.
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            opened: RefCell::new(BTreeMap::new()),
            misshapen: RefCell::new(BTreeSet::new()),
            found: RefCell::new(Diagnostics::default()),
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// A cursor at the top of `document`, with an empty path. Use one
    /// walk for one document: paths are how the walk tells maps apart.
    pub fn root<'a>(&'a self, document: &'a Value) -> Cursor<'a> {
        Cursor {
            walk: self,
            value: document,
            path: String::new(),
        }
    }

    /// Records an error at `path`. [`Cursor::error`] is the usual way.
    pub fn error(&self, path: impl Into<String>, message: impl Into<String>) {
        let d = Diagnostic::new(self.source.clone(), path, message);
        self.found.borrow_mut().errors.push(d);
    }

    /// Records a warning at `path`. [`Cursor::warn`] is the usual way.
    pub fn warn(&self, path: impl Into<String>, message: impl Into<String>) {
        let d = Diagnostic::new(self.source.clone(), path, message);
        self.found.borrow_mut().warnings.push(d);
    }

    /// Whether an error was recorded so far. Keys nobody took do not
    /// count before [`Self::finish`].
    pub fn has_errors(&self) -> bool {
        !self.found.borrow().errors.is_empty()
    }

    /// Ends the walk: everything recorded, plus one [`UNSUPPORTED_KEY`]
    /// error for each key of a looked-into map that nobody took.
    pub fn finish(self) -> Diagnostics {
        let mut found = self.found.into_inner();
        for (path, opened) in self.opened.into_inner() {
            for key in opened.keys {
                if !opened.taken.contains(&key) {
                    found.errors.push(Diagnostic::new(
                        self.source.clone(),
                        child_path(&path, &key),
                        UNSUPPORTED_KEY,
                    ));
                }
            }
        }
        found
    }

    /// Marks `key` of the map at `path` as taken, registering the map as
    /// looked into on first use.
    fn take(&self, path: &str, map: &serde_json::Map<String, Value>, key: Option<&str>) {
        let mut opened = self.opened.borrow_mut();
        let entry = opened.entry(path.to_string()).or_insert_with(|| {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            Opened {
                keys,
                taken: BTreeSet::new(),
            }
        });
        match key {
            Some(key) => {
                entry.taken.insert(key.to_string());
            }
            None => entry.taken.extend(entry.keys.iter().cloned()),
        }
    }

    /// Records "expected X, found Y" at `path`, once per path.
    fn misshapen(&self, path: &str, expected: &str, value: &Value) {
        if self.misshapen.borrow_mut().insert(path.to_string()) {
            self.error(path, format!("expected {expected}, found {}", kind(value)));
        }
    }
}

/// A position in a document: a value and the path that leads to it.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    walk: &'a Walk,
    value: &'a Value,
    path: String,
}

impl<'a> Cursor<'a> {
    /// The path from the top of the document: `services.web.ports[0]`.
    /// Empty at the top.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The value itself. Reading below it this way is not recorded, so
    /// use it for leaves and for values that are passed on whole.
    pub fn value(&self) -> &'a Value {
        self.value
    }

    /// What the value is, for a message: `a map`, `a list`, `a string`,
    /// `a number`, `a boolean`, `null`.
    pub fn kind(&self) -> &'static str {
        kind(self.value)
    }

    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }

    /// Whether the value is a map with `key`, null or not. Takes nothing.
    pub fn has(&self, key: &str) -> bool {
        self.value.as_object().is_some_and(|m| m.contains_key(key))
    }

    /// The keys of a map, sorted. Empty for anything else. Takes nothing.
    pub fn keys(&self) -> Vec<&'a str> {
        let mut keys: Vec<&'a str> = match self.value.as_object() {
            Some(map) => map.keys().map(String::as_str).collect(),
            None => Vec::new(),
        };
        keys.sort_unstable();
        keys
    }

    /// Takes `key` and returns a cursor at its value. `None` when the key
    /// is absent or its value is null.
    ///
    /// On a value that is neither a map nor null this records the error
    /// `expected a map, found ...` and returns `None`.
    pub fn get(&self, key: &str) -> Option<Cursor<'a>> {
        let map = self.map()?;
        self.walk.take(&self.path, map, Some(key));
        let value = map.get(key)?;
        if value.is_null() {
            return None;
        }
        Some(self.child(child_path(&self.path, key), value))
    }

    /// Takes `key` without reading it, and whatever is below it. Returns
    /// whether the key is there. For keys that are ignored on purpose.
    pub fn ignore(&self, key: &str) -> bool {
        let Some(map) = self.map() else {
            return false;
        };
        self.walk.take(&self.path, map, Some(key));
        map.contains_key(key)
    }

    /// Takes every key and returns each with a cursor at its value, sorted
    /// by key. Null values are included. For maps whose keys are names:
    /// `services`, `environment`, `volumes`.
    ///
    /// Empty for null. On any other value that is not a map this records
    /// the error `expected a map, found ...` and returns nothing.
    pub fn entries(&self) -> Vec<(&'a str, Cursor<'a>)> {
        let Some(map) = self.map() else {
            return Vec::new();
        };
        self.walk.take(&self.path, map, None);
        let mut entries: Vec<(&'a str, Cursor<'a>)> = map
            .iter()
            .map(|(key, value)| (key.as_str(), self.child(child_path(&self.path, key), value)))
            .collect();
        entries.sort_unstable_by_key(|(key, _)| *key);
        entries
    }

    /// A cursor at each element of a list, with the paths `p[0]`, `p[1]`.
    ///
    /// Empty for null. On any other value that is not a list this records
    /// the error `expected a list, found ...` and returns nothing.
    pub fn items(&self) -> Vec<Cursor<'a>> {
        match self.value {
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, value)| self.child(format!("{}[{i}]", self.path), value))
                .collect(),
            Value::Null => Vec::new(),
            other => {
                self.walk.misshapen(&self.path, "a list", other);
                Vec::new()
            }
        }
    }

    /// Whether the value is a map. Records nothing.
    pub fn is_map(&self) -> bool {
        self.value.is_object()
    }

    /// Whether the value is a list. Records nothing.
    pub fn is_list(&self) -> bool {
        self.value.is_array()
    }

    /// The value as a string, when it is one. Records nothing.
    pub fn as_str(&self) -> Option<&'a str> {
        self.value.as_str()
    }

    /// The value as a boolean, when it is one. Records nothing.
    pub fn as_bool(&self) -> Option<bool> {
        self.value.as_bool()
    }

    /// The value as a whole number from 0 up, when it is one. Records
    /// nothing.
    pub fn as_u64(&self) -> Option<u64> {
        self.value.as_u64()
    }

    /// The value as a whole number, when it is one. Records nothing.
    pub fn as_i64(&self) -> Option<i64> {
        self.value.as_i64()
    }

    /// The value as a number, whole or not. Records nothing.
    pub fn as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    /// The value as the text it was written as: a string as it is, a
    /// whole number in decimal, a boolean as `true` or `false`. `None` for
    /// everything else, including a number with a fraction or an
    /// exponent, whose spelling the YAML parser has already lost. Records
    /// nothing.
    pub fn as_text(&self) -> Option<String> {
        match self.value {
            Value::String(s) => Some(s.clone()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Number(n) if n.is_u64() || n.is_i64() => Some(n.to_string()),
            _ => None,
        }
    }

    /// [`Self::as_str`], recording `expected a string, found ...` when the
    /// value is anything else.
    pub fn expect_str(&self) -> Option<&'a str> {
        self.expect(self.value.as_str(), "a string")
    }

    /// [`Self::as_bool`], recording an error when the value is anything
    /// else.
    pub fn expect_bool(&self) -> Option<bool> {
        self.expect(self.value.as_bool(), "a boolean")
    }

    /// [`Self::as_u64`], recording an error when the value is anything
    /// else.
    pub fn expect_u64(&self) -> Option<u64> {
        self.expect(self.value.as_u64(), "a whole number from 0 up")
    }

    /// [`Self::as_text`], recording an error when the value is anything
    /// else. For a number that is not whole the error says to quote it.
    pub fn expect_text(&self) -> Option<String> {
        let text = self.as_text();
        if text.is_none() {
            if self.value.is_number() {
                if self.walk.misshapen.borrow_mut().insert(self.path.clone()) {
                    self.error(
                        "expected a string, found a number that is not whole; \
                         quote the value to keep it as written",
                    );
                }
            } else {
                self.walk.misshapen(&self.path, "a string", self.value);
            }
        }
        text
    }

    /// Records an error at this path.
    pub fn error(&self, message: impl Into<String>) {
        self.walk.error(self.path.clone(), message);
    }

    /// Records a warning at this path.
    pub fn warn(&self, message: impl Into<String>) {
        self.walk.warn(self.path.clone(), message);
    }

    fn child(&self, path: String, value: &'a Value) -> Cursor<'a> {
        Cursor {
            walk: self.walk,
            value,
            path,
        }
    }

    /// The value as a map. Null is no map and no error; anything else
    /// records the wrong-shape error.
    fn map(&self) -> Option<&'a serde_json::Map<String, Value>> {
        match self.value {
            Value::Object(map) => Some(map),
            Value::Null => None,
            other => {
                self.walk.misshapen(&self.path, "a map", other);
                None
            }
        }
    }

    fn expect<T>(&self, got: Option<T>, expected: &str) -> Option<T> {
        if got.is_none() {
            self.walk.misshapen(&self.path, expected, self.value);
        }
        got
    }
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "a map",
    }
}

/// The path of `key` in the map at `parent`: `parent.key`, or
/// `parent["key"]` when the key holds anything but letters, digits, `_`
/// and `-`.
pub(crate) fn child_path(parent: &str, key: &str) -> String {
    let plain = !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if !plain {
        let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
        return format!("{parent}[\"{escaped}\"]");
    }
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn errors(found: &Diagnostics) -> Vec<String> {
        found.errors.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_walk_that_takes_every_key_finds_nothing() {
        let doc = json!({"image": "nginx", "replicas": 2, "ro": true});
        let walk = Walk::new("f");
        let root = walk.root(&doc);
        assert_eq!(root.path(), "");
        assert_eq!(root.get("image").unwrap().expect_str(), Some("nginx"));
        assert_eq!(root.get("replicas").unwrap().expect_u64(), Some(2));
        assert_eq!(root.get("ro").unwrap().expect_bool(), Some(true));
        assert!(root.get("absent").is_none());
        assert!(!walk.has_errors());
        assert_eq!(walk.finish(), Diagnostics::default());
    }

    #[test]
    fn keys_nobody_took_are_errors_with_their_paths() {
        let doc = json!({
            "services": {
                "web": {"image": "nginx", "privileged": true, "deploy": {"mode": "x", "zz": 1}},
                "db": {"image": "pg", "tty": true},
            },
            "version": "3",
            "networks": {"front": {}},
        });
        let walk = Walk::new("compose.yaml");
        let root = walk.root(&doc);
        assert!(root.ignore("version"));
        assert!(!root.ignore("name"));
        for (_, service) in root.get("services").unwrap().entries() {
            service.get("image");
            if let Some(deploy) = service.get("deploy") {
                deploy.get("mode");
            }
        }
        let found = walk.finish();
        assert_eq!(
            errors(&found),
            [
                // Ordered by the path of the map, then by key. `networks`
                // is reported once, with nothing below it.
                "compose.yaml: networks: unsupported key",
                "compose.yaml: services.db.tty: unsupported key",
                "compose.yaml: services.web.privileged: unsupported key",
                "compose.yaml: services.web.deploy.zz: unsupported key",
            ]
        );
        assert!(found.warnings.is_empty());
    }

    #[test]
    fn a_map_that_was_never_looked_into_is_taken_whole() {
        let doc = json!({"labels": {"a": {"deep": 1}}, "x-meta": {"k": 1}, "raw": {"k": 1}});
        let walk = Walk::new("f");
        let root = walk.root(&doc);
        root.ignore("labels");
        // `keys` and `has` take nothing; the converter picks from them.
        assert_eq!(root.keys(), ["labels", "raw", "x-meta"]);
        assert!(root.has("x-meta") && !root.has("nope"));
        for key in root.keys() {
            if key.starts_with("x-") {
                root.ignore(key);
            }
        }
        // Taken, then read through `value()` without the walk.
        assert_eq!(root.get("raw").unwrap().value(), &json!({"k": 1}));
        assert_eq!(walk.finish(), Diagnostics::default());
    }

    #[test]
    fn looking_into_a_map_without_taking_anything_reports_all_its_keys() {
        let doc = json!({"build": {"context": ".", "args": {}}});
        let walk = Walk::new("f");
        let build = walk.root(&doc).get("build").unwrap();
        assert!(build.get("dockerfile").is_none());
        assert_eq!(
            errors(&walk.finish()),
            [
                "f: build.args: unsupported key",
                "f: build.context: unsupported key"
            ]
        );
    }

    #[test]
    fn null_is_like_absent_for_get_and_still_taken() {
        let doc = json!({"ports": null, "environment": {"A": null, "B": "1"}});
        let walk = Walk::new("f");
        let root = walk.root(&doc);
        assert!(root.has("ports"));
        assert!(root.get("ports").is_none());
        // `entries` keeps nulls: in an environment map they mean something.
        let env = root.get("environment").unwrap().entries();
        assert_eq!(env.len(), 2);
        assert_eq!(env[0].0, "A");
        assert!(env[0].1.is_null());
        assert_eq!(env[0].1.path(), "environment.A");
        assert_eq!(env[1].1.as_str(), Some("1"));
        // Reading through a null cursor is empty, not an error.
        assert!(env[0].1.get("x").is_none());
        assert!(env[0].1.entries().is_empty());
        assert!(env[0].1.items().is_empty());
        assert_eq!(walk.finish(), Diagnostics::default());
    }

    #[test]
    fn lists_give_indexed_paths_and_are_checked_per_element() {
        let doc = json!({"containers": [
            {"name": "a", "image": "x"},
            {"name": "b", "image": "y", "tty": true},
        ]});
        let walk = Walk::new("pod.yaml#1");
        let containers = walk.root(&doc).get("containers").unwrap();
        assert!(containers.is_list() && !containers.is_map());
        let items = containers.items();
        assert_eq!(items[1].path(), "containers[1]");
        for item in &items {
            item.get("name");
            item.get("image");
        }
        assert_eq!(
            errors(&walk.finish()),
            ["pod.yaml#1: containers[1].tty: unsupported key"]
        );
    }

    #[test]
    fn a_wrong_shape_is_an_error_once_and_never_a_silent_miss() {
        let doc = json!({"deploy": "fast", "ports": {"a": 1}, "image": 7, "n": -1, "b": "yes"});
        let walk = Walk::new("f");
        let root = walk.root(&doc);
        let deploy = root.get("deploy").unwrap();
        assert!(deploy.get("replicas").is_none());
        assert!(deploy.get("mode").is_none());
        assert!(!deploy.ignore("x"));
        assert!(deploy.entries().is_empty());
        assert!(root.get("ports").unwrap().items().is_empty());
        assert_eq!(root.get("image").unwrap().expect_str(), None);
        assert_eq!(root.get("n").unwrap().expect_u64(), None);
        assert_eq!(root.get("b").unwrap().expect_bool(), None);
        // The silent readers record nothing.
        assert_eq!(root.get("b").unwrap().as_bool(), None);
        assert!(walk.has_errors());
        assert_eq!(
            errors(&walk.finish()),
            [
                "f: deploy: expected a map, found a string",
                "f: ports: expected a list, found a map",
                "f: image: expected a string, found a number",
                "f: n: expected a whole number from 0 up, found a number",
                "f: b: expected a boolean, found a string",
            ]
        );
    }

    #[test]
    fn text_keeps_what_was_written_and_refuses_what_was_lost() {
        let doc = json!({"s": "1.20", "i": 8080, "neg": -1, "t": true, "f": 1.2, "l": []});
        let walk = Walk::new("f");
        let root = walk.root(&doc);
        assert_eq!(
            root.get("s").unwrap().expect_text().as_deref(),
            Some("1.20")
        );
        assert_eq!(
            root.get("i").unwrap().expect_text().as_deref(),
            Some("8080")
        );
        assert_eq!(
            root.get("neg").unwrap().expect_text().as_deref(),
            Some("-1")
        );
        assert_eq!(
            root.get("t").unwrap().expect_text().as_deref(),
            Some("true")
        );
        assert_eq!(root.get("f").unwrap().as_text(), None);
        assert_eq!(root.get("f").unwrap().as_f64(), Some(1.2));
        assert_eq!(root.get("i").unwrap().as_i64(), Some(8080));
        assert_eq!(root.get("f").unwrap().expect_text(), None);
        assert_eq!(root.get("l").unwrap().expect_text(), None);
        assert_eq!(
            errors(&walk.finish()),
            [
                "f: f: expected a string, found a number that is not whole; \
                 quote the value to keep it as written",
                "f: l: expected a string, found a list",
            ]
        );
    }

    #[test]
    fn keys_that_are_not_plain_words_are_quoted_in_paths() {
        let doc = json!({"nodeSelector": {"kubernetes.io/hostname": "n1", "a\"b": 1, "": 2}});
        let walk = Walk::new("f");
        let selector = walk.root(&doc).get("nodeSelector").unwrap();
        let host = selector.get("kubernetes.io/hostname").unwrap();
        assert_eq!(host.path(), "nodeSelector[\"kubernetes.io/hostname\"]");
        assert_eq!(
            errors(&walk.finish()),
            [
                "f: nodeSelector[\"\"]: unsupported key",
                "f: nodeSelector[\"a\\\"b\"]: unsupported key",
            ]
        );
    }

    #[test]
    fn errors_and_warnings_keep_their_order_and_source() {
        let doc = json!({"build": ".", "depends_on": ["db"], "extra": 1});
        let walk = Walk::new("compose.yaml");
        assert_eq!(walk.source(), "compose.yaml");
        let root = walk.root(&doc);
        root.get("depends_on")
            .unwrap()
            .warn("start order is not kept");
        root.get("build").unwrap().error("images are not built");
        walk.warn("", "an override file is not merged");
        let found = walk.finish();
        assert_eq!(
            errors(&found),
            [
                "compose.yaml: build: images are not built",
                // Keys nobody took come after what the converter recorded.
                "compose.yaml: extra: unsupported key",
            ]
        );
        let warnings: Vec<String> = found.warnings.iter().map(ToString::to_string).collect();
        assert_eq!(
            warnings,
            [
                "compose.yaml: depends_on: start order is not kept",
                "compose.yaml: an override file is not merged",
            ]
        );
    }
}
