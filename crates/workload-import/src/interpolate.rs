//! Compose variable interpolation: `$VAR`, `${VAR}`, `${VAR:-default}`, `${VAR:?message}`, `$$`.

use std::collections::{BTreeMap, BTreeSet};

/// Largest text one interpolation may produce: 64 KiB. Past it the text
/// is refused, so a few short references to a long value cannot grow
/// without bound.
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// Most levels of `${A:-${B:-...}}` followed before the text is refused.
pub const MAX_DEPTH: usize = 16;

/// A text after interpolation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interpolated {
    pub text: String,
    /// Variables that were not set and had no default. Each was replaced
    /// by an empty string, as compose does; the converter warns about them.
    pub unset: Vec<String>,
    /// Variables whose value went into `text`, sorted.
    pub used: Vec<String>,
}

/// Why a text could not be interpolated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InterpolateError {
    /// `${VAR:?message}` or `${VAR?message}` with `VAR` unset or empty.
    /// `message` is what the file says, with no variable replaced, so it
    /// cannot carry a value.
    #[error("required variable {name} is not set: {message}")]
    Required { name: String, message: String },
    /// Not a form compose knows, such as `${` without `}` or `${VAR:x}`.
    #[error("invalid interpolation: {0}")]
    Invalid(String),
    /// Nested deeper than [`MAX_DEPTH`].
    #[error("interpolation is nested more than {MAX_DEPTH} levels deep")]
    TooDeep,
}

/// Replaces the variable references in `text` from `variables`.
///
/// Forms: `$VAR`, `${VAR}`, `${VAR:-default}` and `${VAR-default}`,
/// `${VAR:?message}` and `${VAR?message}`, `${VAR:+replacement}` and
/// `${VAR+replacement}`, and `$$` for a literal `$`. The forms with a
/// colon treat an empty value like an unset one. A default, message or
/// replacement may hold references itself. The message of `:?` is taken
/// as written.
///
/// The value of a variable is inserted as it is and never read again, so
/// a `$` in a value stays a `$`. Error messages name the variable and
/// never repeat the text, which may hold a secret.
pub fn interpolate(
    text: &str,
    variables: &BTreeMap<String, String>,
) -> Result<Interpolated, InterpolateError> {
    let mut seen = Seen::default();
    let text = expand(text, variables, 0, &mut seen)?;
    Ok(Interpolated {
        text,
        unset: seen.unset,
        used: seen.used.into_iter().collect(),
    })
}

/// The variables one interpolation came across.
#[derive(Default)]
struct Seen {
    /// Not set, in the order they were met.
    unset: Vec<String>,
    /// The same names, to tell a repeat without a search.
    unset_names: BTreeSet<String>,
    /// Set, and their value was inserted.
    used: BTreeSet<String>,
}

fn too_large() -> InterpolateError {
    InterpolateError::Invalid(format!(
        "the result is larger than {} KiB",
        MAX_OUTPUT_BYTES / 1024
    ))
}

/// Expands one text. `depth` is how many defaults deep it sits.
fn expand(
    text: &str,
    variables: &BTreeMap<String, String>,
    depth: usize,
    seen: &mut Seen,
) -> Result<String, InterpolateError> {
    if depth > MAX_DEPTH {
        return Err(InterpolateError::TooDeep);
    }
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        // Checked after every piece, so no step holds more than the limit
        // and one value.
        if out.len() > MAX_OUTPUT_BYTES {
            return Err(too_large());
        }
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
        } else if let Some(braced) = after.strip_prefix('{') {
            let end = closing_brace(braced)
                .ok_or_else(|| InterpolateError::Invalid("`${` has no closing `}`".to_string()))?;
            out.push_str(&braced_value(&braced[..end], variables, depth, seen)?);
            rest = &braced[end + 1..];
        } else {
            let len = name_len(after);
            if len == 0 {
                return Err(InterpolateError::Invalid(
                    "`$` is not followed by a variable name; write `$$` for a literal `$`"
                        .to_string(),
                ));
            }
            out.push_str(lookup(&after[..len], variables, seen));
            rest = &after[len..];
        }
    }
    out.push_str(rest);
    if out.len() > MAX_OUTPUT_BYTES {
        return Err(too_large());
    }
    Ok(out)
}

/// The value of what stands between `${` and `}`.
fn braced_value(
    body: &str,
    variables: &BTreeMap<String, String>,
    depth: usize,
    seen: &mut Seen,
) -> Result<String, InterpolateError> {
    let len = name_len(body);
    if len == 0 {
        return Err(InterpolateError::Invalid(
            "`${` is not followed by a variable name".to_string(),
        ));
    }
    let (name, tail) = body.split_at(len);
    if tail.is_empty() {
        return Ok(lookup(name, variables, seen).to_string());
    }
    // With a colon, an empty value counts as unset.
    let (or_empty, tail) = match tail.strip_prefix(':') {
        Some(tail) => (true, tail),
        None => (false, tail),
    };
    let mut chars = tail.chars();
    let (Some(operator @ ('-' | '?' | '+')), argument) = (chars.next(), chars.as_str()) else {
        return Err(InterpolateError::Invalid(format!(
            "`${{{name}` must be followed by `}}`, `:-`, `-`, `:?`, `?`, `:+` or `+`"
        )));
    };
    let value = variables
        .get(name)
        .filter(|value| !(or_empty && value.is_empty()));
    let used = match (operator, value) {
        ('-', None) | ('+', Some(_)) => true,
        ('?', None) => {
            // As written: an interpolated message could print a value.
            return Err(InterpolateError::Required {
                name: name.to_string(),
                message: argument.to_string(),
            });
        }
        _ => false,
    };
    if used {
        return expand(argument, variables, depth + 1, seen);
    }
    // The argument is not used, but a malformed one is still refused. A
    // variable it requires or misses does not matter.
    // The message of `?` is never read as more than text.
    if operator != '?' {
        match expand(argument, variables, depth + 1, &mut Seen::default()) {
            Ok(_) | Err(InterpolateError::Required { .. }) => {}
            Err(other) => return Err(other),
        }
    }
    Ok(match (operator, value) {
        ('+', _) | (_, None) => String::new(),
        (_, Some(value)) => {
            seen.used.insert(name.to_string());
            value.clone()
        }
    })
}

/// The value of `name`, or an empty string and a note in `unset`.
fn lookup<'a>(name: &str, variables: &'a BTreeMap<String, String>, seen: &mut Seen) -> &'a str {
    match variables.get(name) {
        Some(value) => {
            seen.used.insert(name.to_string());
            value
        }
        None => {
            if seen.unset_names.insert(name.to_string()) {
                seen.unset.push(name.to_string());
            }
            ""
        }
    }
}

/// How many bytes at the start of `text` are a variable name: a letter or
/// `_`, then letters, digits and `_`.
fn name_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    if !bytes
        .first()
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
    {
        return 0;
    }
    bytes
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
        .count()
}

/// The position of the `}` that closes a `${` whose content starts at the
/// start of `text`, skipping over nested `${...}` and `$$`.
fn closing_brace(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut open = 1usize;
    let mut i = 0;
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1)) {
            (b'$', Some(b'$')) => i += 1,
            (b'$', Some(b'{')) => {
                open += 1;
                i += 1;
            }
            (b'}', _) => {
                open -= 1;
                if open == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variables() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("TAG".to_string(), "1.27".to_string()),
            ("EMPTY".to_string(), String::new()),
            ("_under9".to_string(), "u".to_string()),
            ("DOLLAR".to_string(), "${TAG}".to_string()),
        ])
    }

    fn text(input: &str) -> String {
        let got = interpolate(input, &variables()).unwrap_or_else(|e| panic!("{input:?}: {e}"));
        assert_eq!(got.unset, Vec::<String>::new(), "{input:?}");
        got.text
    }

    #[test]
    fn every_form_is_replaced() {
        for (input, want) in [
            ("plain", "plain"),
            ("", ""),
            ("nginx:$TAG", "nginx:1.27"),
            ("nginx:${TAG}", "nginx:1.27"),
            ("$TAG-$TAG", "1.27-1.27"),
            ("${_under9}$_under9", "uu"),
            // A name ends at the first character that cannot be in one.
            ("$TAG.x", "1.27.x"),
            ("a$$b", "a$b"),
            ("$$TAG", "$TAG"),
            ("$${TAG}", "${TAG}"),
            ("$$$TAG", "$1.27"),
            // Set and empty is a value for the forms without a colon.
            ("[${EMPTY}]", "[]"),
            ("[$EMPTY]", "[]"),
            // Default.
            ("${TAG:-d}", "1.27"),
            ("${TAG-d}", "1.27"),
            ("${NOPE:-d}", "d"),
            ("${NOPE-d}", "d"),
            ("${EMPTY:-d}", "d"),
            ("${EMPTY-d}", ""),
            ("${NOPE:-}", ""),
            ("${NOPE:-a b:c/d}", "a b:c/d"),
            // Required, when it is there.
            ("${TAG:?need a tag}", "1.27"),
            ("${TAG?need a tag}", "1.27"),
            ("${EMPTY?need it}", ""),
            // Replacement.
            ("${TAG:+r}", "r"),
            ("${TAG+r}", "r"),
            ("${NOPE:+r}", ""),
            ("${NOPE+r}", ""),
            ("${EMPTY:+r}", ""),
            ("${EMPTY+r}", "r"),
            // Nesting.
            ("${NOPE:-${TAG}}", "1.27"),
            ("${NOPE:-${ALSO_NOPE:-deep}}", "deep"),
            ("${NOPE:-v$TAG}", "v1.27"),
            ("${TAG:+is-${TAG}}", "is-1.27"),
            ("${NOPE:-a$$b}", "a$b"),
            // An unused argument may require or miss anything.
            ("${TAG:-${NOPE:?never asked}}", "1.27"),
            ("${TAG:-$NOPE}", "1.27"),
            // A value is inserted as it is.
            ("$DOLLAR", "${TAG}"),
            // A `}` closes the innermost `${`.
            ("${NOPE:-{}}", "{}"),
            ("é${TAG}é", "é1.27é"),
        ] {
            assert_eq!(text(input), want, "{input:?}");
        }
    }

    #[test]
    fn an_unset_variable_without_a_default_is_empty_and_noted_once() {
        let got = interpolate("a${NOPE}b$OTHER$NOPE", &variables()).unwrap();
        assert_eq!(got.text, "ab");
        assert_eq!(got.unset, ["NOPE", "OTHER"]);
        // Also from inside a default that is used.
        let got = interpolate("${NOPE:-$OTHER}", &variables()).unwrap();
        assert_eq!(
            (got.text.as_str(), got.unset),
            ("", vec!["OTHER".to_string()])
        );
    }

    #[test]
    fn a_required_variable_that_is_missing_is_an_error_with_its_message() {
        for (input, name, message) in [
            ("${NOPE:?set NOPE first}", "NOPE", "set NOPE first"),
            ("${NOPE?set NOPE first}", "NOPE", "set NOPE first"),
            ("${EMPTY:?must not be empty}", "EMPTY", "must not be empty"),
            ("x${NOPE:?}", "NOPE", ""),
            ("${ALSO_NOPE:-${NOPE:?inner}}", "NOPE", "inner"),
            // The message is not interpolated, so it cannot print a value.
            ("${NOPE:?tag was $TAG}", "NOPE", "tag was $TAG"),
            ("${NOPE?$TAG}", "NOPE", "$TAG"),
            ("${NOPE:?$}", "NOPE", "$"),
        ] {
            assert_eq!(
                interpolate(input, &variables()),
                Err(InterpolateError::Required {
                    name: name.to_string(),
                    message: message.to_string(),
                }),
                "{input:?}"
            );
        }
        assert_eq!(
            interpolate("${NOPE:?set it}", &variables())
                .unwrap_err()
                .to_string(),
            "required variable NOPE is not set: set it"
        );
    }

    #[test]
    fn a_malformed_expression_is_an_error_that_does_not_repeat_the_text() {
        for input in [
            "hunter2$",
            "hunter2$ x",
            "hunter2$1",
            "hunter2${",
            "hunter2${TAG",
            "hunter2${}",
            "hunter2${1A}",
            "hunter2${ TAG }",
            "hunter2${TAG:x}",
            "hunter2${TAG:}",
            "hunter2${TAG!x}",
            "hunter2${TAG/a/b}",
            "hunter2${NOPE:-${}",
            // Also inside an argument that is not used.
            "hunter2${TAG:-${}}",
            "hunter2${TAG:-$}",
        ] {
            match interpolate(input, &variables()) {
                Err(e @ InterpolateError::Invalid(_)) => {
                    assert!(!e.to_string().contains("hunter2"), "{input:?}: {e}");
                }
                other => panic!("{input:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn the_variables_whose_value_was_inserted_are_reported() {
        let used = |input: &str| interpolate(input, &variables()).unwrap().used;
        assert_eq!(used("$TAG ${_under9} $TAG $NOPE"), ["TAG", "_under9"]);
        assert_eq!(used("${NOPE:-$TAG}"), ["TAG"]);
        assert_eq!(used("${TAG:-$_under9}"), ["TAG"]);
        // Only that it is set was read, not what it holds.
        assert_eq!(used("${TAG:+yes}"), Vec::<String>::new());
        assert_eq!(used("${EMPTY:-d} $$TAG"), Vec::<String>::new());
    }

    #[test]
    fn the_result_has_a_size_limit() {
        let mut variables = variables();
        variables.insert("BIG".to_string(), "x".repeat(1024));
        let fits = "$BIG".repeat(64);
        assert_eq!(
            interpolate(&fits, &variables).unwrap().text.len(),
            MAX_OUTPUT_BYTES
        );
        for input in [
            format!("{fits}!"),
            "$BIG".repeat(10_000),
            format!("${{NOPE:-{}}}", "$BIG".repeat(65)),
            "y".repeat(MAX_OUTPUT_BYTES + 1),
        ] {
            let err = interpolate(&input, &variables).unwrap_err();
            assert_eq!(
                err.to_string(),
                "invalid interpolation: the result is larger than 64 KiB"
            );
        }
        // Many unset names cost no more than their number.
        let many: String = (0..5_000).map(|i| format!("$V{i}")).collect();
        assert_eq!(interpolate(&many, &variables).unwrap().unset.len(), 5_000);
    }

    #[test]
    fn nesting_stops_at_the_depth_limit() {
        let nested = |levels: usize| {
            let mut text = String::from("end");
            for i in 0..levels {
                text = format!("${{N{i}:-{text}}}");
            }
            text
        };
        assert_eq!(text(&nested(MAX_DEPTH)), "end");
        assert_eq!(
            interpolate(&nested(MAX_DEPTH + 1), &variables()),
            Err(InterpolateError::TooDeep)
        );
        // Deep and unused is refused all the same.
        let unused = format!("${{TAG:-{}}}", nested(MAX_DEPTH));
        assert_eq!(
            interpolate(&unused, &variables()),
            Err(InterpolateError::TooDeep)
        );
    }
}
