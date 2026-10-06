//! The one place that knows the YAML library: text in, JSON values out.
//!
//! [`parse_documents`] is the whole interface, so swapping the library
//! (`serde-saphyr` now, `serde_norway` as the fallback) changes this file
//! and nothing else. The tests pin what the converters rely on.
//!
//! How plain scalars come out, since the converters must not guess:
//!
//! | YAML | JSON |
//! |---|---|
//! | `true`, `True`, `TRUE`, `false` | boolean |
//! | `yes`, `no`, `on`, `off`, `y`, `n` | the string, as written |
//! | `42`, `-7`, `+1`, `0x1F`, `0o17` | integer |
//! | `1.5`, `1e3`, `1.20` | float: `1.5`, `1000.0`, `1.2` |
//! | `0755` | float: `755.0`. The leading zero is lost |
//! | `~`, `null`, nothing | null |
//! | `"quoted"`, `'0755'` | string |
//! | `8080:80`, `12:30:45`, `2001-01-01` | string |
//!
//! So a value that must be a string (an env value, a port mapping, a file
//! mode, a version) and arrives as a float was written unquoted, and the
//! number has already lost what was written. A converter refuses it and
//! tells the user to quote it.

use serde_json::Value;

/// Largest input [`parse_documents`] reads: 8 MiB.
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
/// Most documents in one stream.
pub const MAX_DOCUMENTS: usize = 1024;
/// Deepest nesting of maps and lists.
pub const MAX_DEPTH: usize = 64;
/// Most maps, lists and scalars in one stream, and most that aliases may
/// repeat. The second is what stops an alias bomb.
pub const MAX_NODES: usize = 100_000;
/// Most aliases (`*name`) in one stream.
pub const MAX_ALIASES: usize = 10_000;

/// Why a YAML stream was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct YamlError {
    /// What is wrong. It names the position when the library knows it.
    pub message: String,
    /// 1-based line of the problem, when known.
    pub line: Option<u64>,
    /// 1-based column of the problem, when known.
    pub column: Option<u64>,
}

/// Parses a YAML stream into one JSON value per document.
///
/// - Documents are separated by `---`. An empty document is skipped, so an
///   empty stream gives no values.
/// - Anchors, aliases and merge keys (`<<: *base`, `<<: [*a, *b]`) are
///   expanded. A key next to the merge key wins over the merged one.
/// - A map with the same key twice is an error.
/// - Every map key becomes a string: `1: a` has the key `"1"`.
/// - Input over a budget is an error: [`MAX_INPUT_BYTES`],
///   [`MAX_DOCUMENTS`], [`MAX_DEPTH`], [`MAX_NODES`], [`MAX_ALIASES`].
/// - `.nan` and `.inf` are errors, because JSON has no such number.
///
/// The module documentation says how plain scalars are typed.
pub fn parse_documents(text: &str) -> Result<Vec<Value>, YamlError> {
    if text.len() > MAX_INPUT_BYTES {
        return Err(YamlError {
            message: format!("input is larger than {MAX_INPUT_BYTES} bytes"),
            line: None,
            column: None,
        });
    }
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_documents: MAX_DOCUMENTS,
            max_depth: MAX_DEPTH,
            max_nodes: MAX_NODES,
            max_aliases: MAX_ALIASES,
        },
        alias_limits: serde_saphyr::alias_limits! {
            max_total_replayed_events: MAX_NODES,
        },
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        merge_keys: serde_saphyr::MergeKeyPolicy::Merge,
        // `yes` and `on` stay strings, as in compose and YAML 1.2.
        strict_booleans: true,
        legacy_octal_numbers: false,
        // No source snippet in the message: the text may hold env values.
        with_snippet: false,
    };
    serde_saphyr::from_multiple_with_options(text, options).map_err(|e| {
        let location = e.location();
        YamlError {
            message: e.to_string(),
            line: location.map(|l| l.line()),
            column: location.map(|l| l.column()),
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn one(text: &str) -> Value {
        let mut docs = parse_documents(text).unwrap();
        assert_eq!(docs.len(), 1, "{docs:?}");
        docs.remove(0)
    }

    #[test]
    fn a_stream_gives_one_value_per_document() {
        assert_eq!(
            parse_documents("a: 1\n---\nb: 2\n").unwrap(),
            vec![json!({"a": 1}), json!({"b": 2})]
        );
        // A leading marker, an end marker and an empty document in between.
        assert_eq!(
            parse_documents("---\na: 1\n...\n---\n---\nb: 2\n").unwrap(),
            vec![json!({"a": 1}), json!({"b": 2})]
        );
        assert_eq!(parse_documents("").unwrap(), Vec::<Value>::new());
        assert_eq!(parse_documents("# nothing\n").unwrap(), Vec::<Value>::new());
        // A document need not be a map.
        assert_eq!(one("- a\n- b\n"), json!(["a", "b"]));
    }

    #[test]
    fn anchors_and_merge_keys_are_expanded() {
        let doc = one(concat!(
            "base: &base\n",
            "  image: nginx\n",
            "  restart: always\n",
            "web:\n",
            "  <<: *base\n",
            "  restart: never\n",
        ));
        assert_eq!(
            doc,
            json!({
                "base": {"image": "nginx", "restart": "always"},
                // The key next to `<<` wins, and `<<` itself is gone.
                "web": {"image": "nginx", "restart": "never"},
            })
        );
        // A list of maps to merge, and a plain alias as a value.
        let doc = one("a: &a {x: 1}\nb: &b {y: 2}\nc:\n  <<: [*a, *b]\n  z: 3\nd: *a\n");
        assert_eq!(doc["c"], json!({"x": 1, "y": 2, "z": 3}));
        assert_eq!(doc["d"], json!({"x": 1}));
    }

    #[test]
    fn a_duplicate_key_is_refused() {
        let err = parse_documents("a: 1\na: 2\n").unwrap_err();
        assert!(err.message.contains("duplicate"), "{err}");
        assert_eq!((err.line, err.column), (Some(2), Some(1)));
        // Also below the top level.
        let err = parse_documents("s:\n  a: 1\n  a: 2\n").unwrap_err();
        assert!(err.message.contains("duplicate"), "{err}");
        assert_eq!(err.line, Some(3));
    }

    /// "Billion laughs": nine levels of nine aliases each would expand to
    /// 9^9 scalars. The limit on replayed aliases stops it long before that.
    #[test]
    fn an_alias_bomb_is_refused() {
        let mut text = String::from("a: &a [x,x,x,x,x,x,x,x,x]\n");
        for (name, prev) in "bcdefghi".chars().zip("abcdefgh".chars()) {
            let aliases = vec![format!("*{prev}"); 9].join(",");
            text.push_str(&format!("{name}: &{name} [{aliases}]\n"));
        }
        assert!(text.len() < 512);
        let started = std::time::Instant::now();
        let err = parse_documents(&text).unwrap_err();
        assert!(err.message.contains("alias replay limit"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }

    #[test]
    fn the_other_budgets_hold_too() {
        // Depth.
        let deep = format!("{}1{}", "[".repeat(200), "]".repeat(200));
        assert!(parse_documents(&deep).is_err());
        // Aliases, even when each one is small.
        let many = format!("a: &a 1\nb: [{}]\n", vec!["*a"; MAX_ALIASES + 1].join(","));
        assert!(parse_documents(&many).is_err());
        // Size, before anything is parsed.
        let big = format!("a: \"{}\"\n", "x".repeat(MAX_INPUT_BYTES));
        let err = parse_documents(&big).unwrap_err();
        assert!(err.message.contains("larger than"), "{err}");
        // An ordinary use of anchors is far below every budget.
        let ok = format!("a: &a {{x: 1}}\nb: [{}]\n", vec!["*a"; 50].join(","));
        assert_eq!(one(&ok)["b"].as_array().unwrap().len(), 50);
    }

    /// How plain scalars are typed. The converters depend on every row.
    #[test]
    fn plain_scalars_have_pinned_types() {
        for (yaml, want) in [
            // Only the YAML 1.2 spellings are booleans.
            ("true", json!(true)),
            ("True", json!(true)),
            ("TRUE", json!(true)),
            ("false", json!(false)),
            ("yes", json!("yes")),
            ("no", json!("no")),
            ("on", json!("on")),
            ("off", json!("off")),
            ("On", json!("On")),
            ("y", json!("y")),
            ("n", json!("n")),
            // Integers.
            ("42", json!(42)),
            ("-7", json!(-7)),
            ("+1", json!(1)),
            ("0x1F", json!(31)),
            ("0o17", json!(15)),
            (
                "18446744073709551615",
                json!(18_446_744_073_709_551_615_u64),
            ),
            // Floats. What was written is not recoverable.
            ("1.5", json!(1.5)),
            ("1e3", json!(1000.0)),
            ("1.20", json!(1.2)),
            // A leading zero does not mean octal, and it does not survive.
            ("0755", json!(755.0)),
            // Null.
            ("~", json!(null)),
            ("null", json!(null)),
            ("", json!(null)),
            // Strings.
            ("\"quoted\"", json!("quoted")),
            ("'0755'", json!("0755")),
            ("\"yes\"", json!("yes")),
            ("\"123\"", json!("123")),
            ("8080:80", json!("8080:80")),
            ("12:30:45", json!("12:30:45")),
            ("2001-01-01", json!("2001-01-01")),
            ("nginx:1.27", json!("nginx:1.27")),
        ] {
            assert_eq!(one(&format!("k: {yaml}\n"))["k"], want, "{yaml:?} in a map");
            if !yaml.is_empty() {
                assert_eq!(one(&format!("- {yaml}\n"))[0], want, "{yaml:?} in a list");
            }
        }
        assert!(one("k: 1e3\n")["k"].is_f64());
        assert!(one("k: 0755\n")["k"].is_f64());
        assert!(one("k: 42\n")["k"].is_u64());
    }

    #[test]
    fn map_keys_become_strings() {
        assert_eq!(one("1: a\ntrue: b\n"), json!({"1": "a", "true": "b"}));
    }

    #[test]
    fn tags_do_not_leak_into_the_value() {
        assert_eq!(one("a: !!str 123\n"), json!({"a": "123"}));
        assert_eq!(one("a: !custom x\n"), json!({"a": "x"}));
    }

    #[test]
    fn what_json_cannot_hold_is_refused() {
        for bad in ["a: .nan\n", "a: .inf\n", "a: -.inf\n"] {
            assert!(parse_documents(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_syntax_error_carries_its_position_and_no_source_text() {
        let err = parse_documents("ok: 1\nsecret: [hunter2, 2\n").unwrap_err();
        assert_eq!(err.line, Some(2));
        assert!(err.column.is_some());
        assert!(!err.message.contains("hunter2"), "{err}");
        // A tab as indentation is an error, not a guess.
        assert!(parse_documents("a:\n\tb: 1\n").is_err());
    }
}
