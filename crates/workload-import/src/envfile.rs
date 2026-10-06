//! `.env` files: `KEY=value` lines as compose reads them.

/// Why an env file was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("line {line}: {message}")]
pub struct EnvFileError {
    /// 1-based line of the problem.
    pub line: usize,
    pub message: String,
}

/// One line of an env file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: String,
    /// `None` for a bare `KEY`.
    pub value: Option<String>,
    /// The value stood in single quotes, so compose replaces no variable
    /// in it.
    pub literal: bool,
}

/// Parses the text of an env file into its entries, in file order.
///
/// A line is `KEY=value`, `KEY="value"`, `KEY='value'`, `export KEY=value`
/// or a bare `KEY`. Blank lines and `#` comments are skipped. Double
/// quotes take the escapes `\n`, `\t`, `\"` and `\\`; single quotes take
/// none. A bare `KEY` has no value (`None`): compose takes it from the
/// caller's environment, and the converter decides what that means here.
/// A key that appears twice is given twice; the last one wins.
///
/// Values are returned as written: no variable is interpolated here.
pub fn parse(text: &str) -> Result<Vec<(String, Option<String>)>, EnvFileError> {
    let entries = parse_entries(text)?;
    Ok(entries.into_iter().map(|e| (e.key, e.value)).collect())
}

/// [`parse`], and for each entry whether its value was single-quoted.
///
/// The rest of the rules: whitespace around the key and the value is
/// dropped, an unquoted value ends at a `#` that follows whitespace,
/// double quotes also take `\r`, and any other backslash stays as written.
/// A quoted value ends on its own line. Error messages never repeat the
/// line, which may hold a secret.
pub fn parse_entries(text: &str) -> Result<Vec<Entry>, EnvFileError> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let fail = |message: &str| EnvFileError {
            line: index + 1,
            message: message.to_string(),
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.strip_prefix("export") {
            Some(rest) if rest.starts_with(char::is_whitespace) => rest.trim_start(),
            _ => line,
        };
        let (key, value, literal) = match line.split_once('=') {
            None => (without_comment(line), None, false),
            Some((key, raw)) => {
                let (value, literal) = value(raw).map_err(fail)?;
                (key.trim_end(), Some(value), literal)
            }
        };
        let is_name = !key.is_empty()
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
        if !is_name {
            return Err(fail(
                "expected KEY=VALUE or KEY; a name holds letters, digits, `_`, `.` and `-`",
            ));
        }
        entries.push(Entry {
            key: key.to_string(),
            value,
            literal,
        });
    }
    Ok(entries)
}

/// The value written after `=`, and whether it was single-quoted.
fn value(raw: &str) -> Result<(String, bool), &'static str> {
    let trimmed = raw.trim_start();
    if let Some(quoted) = trimmed.strip_prefix('\'') {
        let (value, rest) = quoted
            .split_once('\'')
            .ok_or("the single quote is not closed on this line")?;
        after_quote(rest)?;
        return Ok((value.to_string(), true));
    }
    if let Some(quoted) = trimmed.strip_prefix('"') {
        let mut value = String::new();
        let mut chars = quoted.chars();
        loop {
            match chars.next() {
                None => return Err("the double quote is not closed on this line"),
                Some('"') => break,
                Some('\\') => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some('r') => value.push('\r'),
                    Some('t') => value.push('\t'),
                    Some('\\') => value.push('\\'),
                    Some('"') => value.push('"'),
                    Some(other) => {
                        value.push('\\');
                        value.push(other);
                    }
                    None => return Err("the double quote is not closed on this line"),
                },
                Some(other) => value.push(other),
            }
        }
        after_quote(chars.as_str())?;
        return Ok((value, false));
    }
    Ok((without_comment(raw).to_string(), false))
}

/// Only whitespace or a comment may follow a closing quote.
fn after_quote(rest: &str) -> Result<(), &'static str> {
    let rest = rest.trim_start();
    if rest.is_empty() || rest.starts_with('#') {
        Ok(())
    } else {
        Err("unexpected text after the closing quote")
    }
}

/// `text` up to a `#` that follows whitespace, trimmed.
fn without_comment(text: &str) -> &str {
    let mut after_space = false;
    for (at, c) in text.char_indices() {
        if c == '#' && after_space {
            return text[..at].trim();
        }
        after_space = c.is_whitespace();
    }
    text.trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(line: &str) -> (String, Option<String>) {
        let mut entries = parse(line).unwrap_or_else(|e| panic!("{line:?}: {e}"));
        assert_eq!(entries.len(), 1, "{line:?}");
        entries.remove(0)
    }

    #[test]
    fn each_kind_of_line_is_read_as_compose_reads_it() {
        for (line, key, value) in [
            ("KEY=value", "KEY", Some("value")),
            ("KEY=", "KEY", Some("")),
            ("KEY", "KEY", None),
            ("  KEY = value  ", "KEY", Some("value")),
            ("export KEY=value", "KEY", Some("value")),
            ("export   KEY", "KEY", None),
            ("export=1", "export", Some("1")),
            ("a.b-c_9=1", "a.b-c_9", Some("1")),
            // Only the first `=` splits.
            (
                "URL=postgres://u:p@db/x?a=b",
                "URL",
                Some("postgres://u:p@db/x?a=b"),
            ),
            // Single quotes are literal.
            ("KEY='a $B \\n # c'", "KEY", Some("a $B \\n # c")),
            ("KEY=''", "KEY", Some("")),
            ("KEY='a' # comment", "KEY", Some("a")),
            // Double quotes take the escapes.
            ("KEY=\"a b\"", "KEY", Some("a b")),
            ("KEY=\"a\\nb\\tc\\rd\"", "KEY", Some("a\nb\tc\rd")),
            (
                "KEY=\"say \\\"hi\\\" \\\\ \"",
                "KEY",
                Some("say \"hi\" \\ "),
            ),
            (
                "KEY=\"a # not a comment\" # comment",
                "KEY",
                Some("a # not a comment"),
            ),
            ("KEY=\"\\x\\$\"", "KEY", Some("\\x\\$")),
            // Unquoted: a comment needs whitespace before it.
            ("KEY=value # comment", "KEY", Some("value")),
            ("KEY=value\t# comment", "KEY", Some("value")),
            ("KEY=a#b", "KEY", Some("a#b")),
            ("KEY=#b", "KEY", Some("#b")),
            ("KEY= # only a comment", "KEY", Some("")),
            ("KEY=a b  c", "KEY", Some("a b  c")),
            ("KEY=it's", "KEY", Some("it's")),
            ("KEY # comment", "KEY", None),
        ] {
            assert_eq!(
                one(line),
                (key.to_string(), value.map(str::to_string)),
                "{line:?}"
            );
        }
    }

    #[test]
    fn comments_blank_lines_and_line_endings_are_skipped() {
        let text = "\u{feff}# top\r\n\r\nA=1\r\n   # indented\nB=2\n\nA=3";
        assert_eq!(
            parse(text).unwrap(),
            [
                ("A".to_string(), Some("1".to_string())),
                ("B".to_string(), Some("2".to_string())),
                // A key that appears twice is given twice.
                ("A".to_string(), Some("3".to_string())),
            ]
        );
        assert_eq!(parse("").unwrap(), []);
    }

    #[test]
    fn single_quoted_values_are_marked_literal() {
        let entries = parse_entries("A='$X'\nB=\"$X\"\nC=$X\nD").unwrap();
        let literal: Vec<bool> = entries.iter().map(|e| e.literal).collect();
        assert_eq!(literal, [true, false, false, false]);
    }

    #[test]
    fn a_bad_line_is_an_error_with_its_number_and_without_its_text() {
        for (text, line) in [
            ("A=1\n=hunter2", 2),
            ("A B=hunter2", 1),
            ("hunter2 hunter2", 1),
            ("A=1\n\nB='hunter2", 3),
            ("B=\"hunter2", 1),
            ("B=\"hunter2\\", 1),
            ("B=\"a\"hunter2", 1),
            ("B='a'hunter2", 1),
            ("KEY: hunter2", 1),
        ] {
            let err = parse(text).unwrap_err();
            assert_eq!(err.line, line, "{text:?}");
            assert!(!err.to_string().contains("hunter2"), "{text:?}: {err}");
        }
        assert_eq!(
            parse("A='x").unwrap_err().to_string(),
            "line 1: the single quote is not closed on this line"
        );
    }
}
