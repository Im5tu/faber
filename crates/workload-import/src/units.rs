//! Quantities and durations as Docker and Kubernetes write them.

/// Why a quantity or duration was refused. The message names what was
/// expected and does not repeat the path, which the converter adds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UnitError(pub String);

/// A Docker byte size in bytes: `1024`, `512k`, `256m`, `1g`, `1gb`,
/// `1.5g`. The units are powers of 1024 and case does not matter.
///
/// The units are `b`, `k`, `m`, `g`, `t` and `p`, each also as `kb` or
/// `kib`, with one space allowed before the unit. A number without a unit
/// is bytes. A fraction of a byte is dropped.
pub fn docker_bytes(text: &str) -> Result<u64, UnitError> {
    let fail = || {
        UnitError(format!(
            "`{text}` is not a size; write bytes (`1048576`) or a number with a unit \
             (`512k`, `256m`, `1g`), where each unit is 1024 of the one before"
        ))
    };
    let (number, unit) = docker_split(text.trim());
    let unit = unit.strip_prefix(' ').unwrap_or(unit).to_ascii_lowercase();
    let (scale, rest) = match unit.as_bytes().first() {
        Some(b'k') => (1u32, &unit[1..]),
        Some(b'm') => (2, &unit[1..]),
        Some(b'g') => (3, &unit[1..]),
        Some(b't') => (4, &unit[1..]),
        Some(b'p') => (5, &unit[1..]),
        _ => (0, unit.as_str()),
    };
    let unit_is_known = match scale {
        0 => matches!(rest, "" | "b"),
        _ => matches!(rest, "" | "b" | "ib" | "i"),
    };
    let (billionths, _) = docker_decimal(number).ok_or_else(fail)?;
    if !unit_is_known {
        return Err(fail());
    }
    billionths
        .checked_mul(1024u128.pow(scale))
        .and_then(|scaled| u64::try_from(scaled / DOCKER_BILLION).ok())
        .ok_or_else(|| UnitError(format!("`{text}` is too large for a size")))
}

/// A Docker CPU count in thousandths of a CPU: `0.5` is 500, `2` is 2000.
///
/// A fraction of a thousandth rounds up, so `0.0001` is 1.
pub fn docker_cpu_millis(text: &str) -> Result<u64, UnitError> {
    let (billionths, more) = docker_decimal(text.trim()).ok_or_else(|| {
        UnitError(format!(
            "`{text}` is not a number of CPUs; write a decimal such as `0.5` or `2`"
        ))
    })?;
    let millis = billionths.div_ceil(1_000_000) + u128::from(more && billionths % 1_000_000 == 0);
    u64::try_from(millis).map_err(|_| UnitError(format!("`{text}` is too many CPUs")))
}

/// A Docker duration in whole seconds: `30s`, `1m30s`, `1h`, `500ms`. A
/// fraction of a second rounds up, so `500ms` is 1.
///
/// The form is Go's: one or more numbers, each with a unit out of `h`,
/// `m`, `s`, `ms`, `us` and `ns`. `0` needs no unit; any other number
/// without one is an error.
pub fn docker_duration_secs(text: &str) -> Result<u32, UnitError> {
    let fail = || {
        UnitError(format!(
            "`{text}` is not a duration; write numbers with units, such as `30s`, `1m30s` or `500ms`"
        ))
    };
    let mut rest = text.trim();
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(fail());
    }
    let mut total = 0u128;
    let mut inexact = false;
    while !rest.is_empty() {
        let (number, tail) = docker_split(rest);
        let unit_len = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let nanos_per_unit: u128 = match &tail[..unit_len] {
            "h" => 3_600_000_000_000,
            "m" => 60_000_000_000,
            "s" => 1_000_000_000,
            "ms" => 1_000_000,
            "us" | "µs" | "μs" => 1_000,
            "ns" => 1,
            "" if docker_decimal(number).is_some() => {
                return Err(UnitError(format!(
                    "`{text}` has a number without a unit; write `{number}s` for seconds"
                )));
            }
            _ => return Err(fail()),
        };
        let (billionths, more) = docker_decimal(number).ok_or_else(fail)?;
        let scaled = billionths.checked_mul(nanos_per_unit).ok_or_else(fail)?;
        inexact |= more || scaled % DOCKER_BILLION != 0;
        total = total
            .checked_add(scaled / DOCKER_BILLION)
            .ok_or_else(fail)?;
        rest = &tail[unit_len..];
    }
    let secs = total.div_ceil(DOCKER_BILLION)
        + u128::from(inexact && total.is_multiple_of(DOCKER_BILLION));
    u32::try_from(secs).map_err(|_| UnitError(format!("`{text}` is too long for a duration")))
}

const DOCKER_BILLION: u128 = 1_000_000_000;

/// Splits `text` after its leading digits and dots.
fn docker_split(text: &str) -> (&str, &str) {
    let len = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    text.split_at(len)
}

/// A decimal without a sign or an exponent (`2`, `0.5`, `.5`, `1.`) in
/// billionths, and whether digits beyond the ninth place were dropped.
/// `None` for anything else, and for a number over 10^18.
fn docker_decimal(text: &str) -> Option<(u128, bool)> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.len() + fraction.len() == 0 || !digits(whole) || !digits(fraction) {
        return None;
    }
    let whole = whole.trim_start_matches('0');
    if whole.len() > 18 {
        return None;
    }
    let mut value: u128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let fraction = fraction.as_bytes();
    for place in 0..9 {
        let digit = fraction.get(place).map_or(0, |digit| digit - b'0');
        value = value * 10 + u128::from(digit);
    }
    let more = fraction.iter().skip(9).any(|digit| *digit != b'0');
    Some((value, more))
}

/// A Kubernetes CPU quantity in thousandths of a CPU: `500m` is 500,
/// `0.5` is 500, `2` is 2000.
///
/// The suffixes are `m` (thousandths), `u` (millionths) and `n`
/// (billionths). A fraction of a thousandth rounds up. Zero and negative
/// amounts are errors.
pub fn k8s_cpu_millis(text: &str) -> Result<u64, UnitError> {
    let quantity = k8s_quantity(text, "a CPU quantity such as `500m`, `0.5` or `2`")?;
    let pow10 = match quantity.suffix {
        "" => 0,
        "m" => -3,
        "u" => -6,
        "n" => -9,
        other => {
            return Err(UnitError(format!(
                "`{text}` has the unknown CPU suffix `{other}`; \
                 write a number of CPUs (`0.5`) or thousandths (`500m`)"
            )));
        }
    };
    // Thousandths of a CPU, so three more decimal places.
    k8s_scale(&quantity, 0, pow10 + 3)
        .ok_or_else(|| UnitError(format!("`{text}` is too large for a CPU quantity")))
}

/// A Kubernetes memory quantity in bytes: `128Mi`, `1Gi`, `500M`,
/// `1e9`, `1024`. The suffix `m` (thousandths of a byte) is an error: it
/// is almost always a typo for `M` or `Mi`.
///
/// The suffixes are `Ki Mi Gi Ti Pi Ei` (powers of 1024) and `k M G T P E`
/// (powers of 1000). A fraction of a byte rounds up. Zero, negative
/// amounts and amounts over `u64::MAX` are errors.
pub fn k8s_memory_bytes(text: &str) -> Result<u64, UnitError> {
    let quantity = k8s_quantity(text, "a memory quantity such as `128Mi`, `1Gi` or `500M`")?;
    let (pow2, pow10) = match quantity.suffix {
        "" => (0, 0),
        "Ki" => (10, 0),
        "Mi" => (20, 0),
        "Gi" => (30, 0),
        "Ti" => (40, 0),
        "Pi" => (50, 0),
        "Ei" => (60, 0),
        "k" => (0, 3),
        "M" => (0, 6),
        "G" => (0, 9),
        "T" => (0, 12),
        "P" => (0, 15),
        "E" => (0, 18),
        "m" => {
            let number = &text[..text.len() - 1];
            return Err(UnitError(format!(
                "`{text}` means {number} thousandths of a byte; did you mean `{number}Mi`?"
            )));
        }
        other => {
            return Err(UnitError(format!(
                "`{text}` has the unknown memory suffix `{other}`; \
                 use Ki, Mi, Gi, Ti, Pi, Ei or k, M, G, T, P, E"
            )));
        }
    };
    k8s_scale(&quantity, pow2, pow10)
        .ok_or_else(|| UnitError(format!("`{text}` is too large for a memory quantity")))
}

/// A Kubernetes quantity taken apart: `digits * 10^pow10`, then the suffix.
struct K8sQuantity<'a> {
    /// Greater than zero and below 10^20.
    digits: u128,
    pow10: i64,
    suffix: &'a str,
}

/// Most significant digits a quantity may have. Twenty digits already hold
/// every `u64`.
const K8S_MAX_DIGITS: usize = 20;

/// Splits a quantity into number and suffix: a sign, digits with an
/// optional fraction, then either an exponent (`e3`, `E-2`) or a suffix.
/// `expected` names the right form in the error for anything else.
fn k8s_quantity<'a>(text: &'a str, expected: &str) -> Result<K8sQuantity<'a>, UnitError> {
    let malformed = || UnitError(format!("`{text}` is not {expected}"));
    let positive = || UnitError(format!("`{text}` must be greater than zero"));

    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let whole_len = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (whole, rest) = rest.split_at(whole_len);
    let (fraction, rest) = match rest.strip_prefix('.') {
        Some(after) => after.split_at(after.bytes().take_while(u8::is_ascii_digit).count()),
        None => ("", rest),
    };
    if whole.is_empty() && fraction.is_empty() {
        return Err(malformed());
    }

    // `1E3` is an exponent, `1E` is one exa.
    let exponent = rest
        .strip_prefix(['e', 'E'])
        .filter(|e| {
            let digits = e.strip_prefix(['+', '-']).unwrap_or(e);
            !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
        })
        .map(|e| e.parse::<i32>().map_err(|_| malformed()))
        .transpose()?;
    let suffix = if exponent.is_some() { "" } else { rest };

    // Zeros at the end of the fraction and at the start of the number
    // carry nothing.
    let fraction = fraction.trim_end_matches('0');
    let significant = format!("{whole}{fraction}");
    let significant = significant.trim_start_matches('0');
    if negative || significant.is_empty() {
        return Err(positive());
    }
    if significant.len() > K8S_MAX_DIGITS {
        return Err(UnitError(format!(
            "`{text}` has more than {K8S_MAX_DIGITS} significant digits"
        )));
    }
    let digits = significant.parse::<u128>().map_err(|_| malformed())?;
    Ok(K8sQuantity {
        digits,
        pow10: i64::from(exponent.unwrap_or(0)) - fraction.len() as i64,
        suffix,
    })
}

/// `digits * 2^pow2 * 10^(quantity.pow10 + pow10)`, rounded up to a whole
/// number. `None` when that is over `u64::MAX`.
fn k8s_scale(quantity: &K8sQuantity<'_>, pow2: u32, pow10: i64) -> Option<u64> {
    // Below 10^20 * 2^60, which is below 2^127.
    let mut value = quantity.digits << pow2;
    let pow10 = quantity.pow10 + pow10;
    if pow10 >= 0 {
        for _ in 0..pow10 {
            value = value.checked_mul(10)?;
            if value > u128::from(u64::MAX) {
                return None;
            }
        }
    } else if pow10 < -38 {
        // Less than one, and not zero.
        value = 1;
    } else {
        value = value.div_ceil(10u128.pow(pow10.unsigned_abs() as u32));
    }
    u64::try_from(value).ok()
}

#[cfg(test)]
mod k8s_tests {
    use super::*;

    #[test]
    fn cpu_quantities_become_millis_rounded_up() {
        for (text, want) in [
            ("1", 1000),
            ("2", 2000),
            ("0.5", 500),
            (".5", 500),
            ("1.", 1000),
            ("+1", 1000),
            ("500m", 500),
            ("1m", 1),
            ("1500m", 1500),
            ("0.1", 100),
            ("0.001", 1),
            // Below a thousandth: up to one.
            ("0.0001", 1),
            ("0.5m", 1),
            ("1.5m", 2),
            ("100u", 1),
            ("250000u", 250),
            ("250001u", 251),
            ("1n", 1),
            ("500000000n", 500),
            // What a YAML float turns into.
            ("1000.0", 1_000_000),
            ("1e3", 1_000_000),
            ("5e-1", 500),
            ("1E-7", 1),
            ("0.50000000000000000000000000", 500),
            ("18446744073709551", 18_446_744_073_709_551_000),
        ] {
            assert_eq!(k8s_cpu_millis(text), Ok(want), "{text}");
        }
    }

    #[test]
    fn cpu_quantities_that_are_refused() {
        for (text, part) in [
            ("0", "greater than zero"),
            ("0.0", "greater than zero"),
            ("0m", "greater than zero"),
            ("-1", "greater than zero"),
            ("-500m", "greater than zero"),
            ("", "is not a CPU quantity"),
            ("m", "is not a CPU quantity"),
            (".", "is not a CPU quantity"),
            ("abc", "is not a CPU quantity"),
            (" 1", "is not a CPU quantity"),
            ("1 ", "unknown CPU suffix ` `"),
            ("1k", "unknown CPU suffix `k`"),
            ("1Mi", "unknown CPU suffix `Mi`"),
            ("1mm", "unknown CPU suffix `mm`"),
            ("1e", "unknown CPU suffix `e`"),
            ("1e3m", "unknown CPU suffix `e3m`"),
            ("1.2.3", "unknown CPU suffix `.3`"),
            ("18446744073709552", "too large"),
            ("1e30", "too large"),
            ("1e99999999999", "is not a CPU quantity"),
            ("123456789012345678901", "more than 20 significant digits"),
        ] {
            let err = k8s_cpu_millis(text).unwrap_err();
            assert!(err.0.contains(part), "{text:?}: {err}");
        }
    }

    #[test]
    fn memory_quantities_become_bytes_rounded_up() {
        for (text, want) in [
            ("1024", 1024),
            ("1", 1),
            ("1Ki", 1 << 10),
            ("128Mi", 128 << 20),
            ("1Gi", 1 << 30),
            ("2Ti", 2 << 40),
            ("1Pi", 1 << 50),
            ("1Ei", 1 << 60),
            ("15Ei", 15 << 60),
            ("1k", 1_000),
            ("500M", 500_000_000),
            ("1G", 1_000_000_000),
            ("1T", 1_000_000_000_000),
            ("1P", 1_000_000_000_000_000),
            ("1E", 1_000_000_000_000_000_000),
            ("18E", 18_000_000_000_000_000_000),
            ("1.5Gi", 1_610_612_736),
            ("0.5Mi", 524_288),
            ("1e3", 1_000),
            ("1E3", 1_000),
            ("1e9", 1_000_000_000),
            ("1.5e3", 1_500),
            ("129e6", 129_000_000),
            ("1000000000.0", 1_000_000_000),
            // A fraction of a byte rounds up.
            ("0.5", 1),
            ("1.5", 2),
            ("0.1Ki", 103),
            ("1e-3", 1),
            ("1e-100", 1),
            ("18446744073709551615", u64::MAX),
        ] {
            assert_eq!(k8s_memory_bytes(text), Ok(want), "{text}");
        }
    }

    #[test]
    fn memory_quantities_that_are_refused() {
        for (text, part) in [
            ("0", "greater than zero"),
            ("0Mi", "greater than zero"),
            ("-1Gi", "greater than zero"),
            ("", "is not a memory quantity"),
            ("Mi", "is not a memory quantity"),
            ("1MB", "unknown memory suffix `MB`"),
            ("1K", "unknown memory suffix `K`"),
            ("1mi", "unknown memory suffix `mi`"),
            ("1gb", "unknown memory suffix `gb`"),
            ("1e3Ki", "unknown memory suffix `e3Ki`"),
            ("16Ei", "too large"),
            ("19E", "too large"),
            ("18446744073709551616", "too large"),
            ("1e20", "too large"),
        ] {
            let err = k8s_memory_bytes(text).unwrap_err();
            assert!(err.0.contains(part), "{text:?}: {err}");
        }
    }

    #[test]
    fn the_memory_suffix_m_is_refused_with_a_hint() {
        assert_eq!(
            k8s_memory_bytes("128m").unwrap_err().0,
            "`128m` means 128 thousandths of a byte; did you mean `128Mi`?"
        );
    }
}

#[cfg(test)]
mod docker_tests {
    use super::*;

    #[test]
    fn docker_sizes_are_powers_of_1024() {
        for (text, want) in [
            ("0", 0),
            ("1024", 1024),
            ("1b", 1),
            ("512k", 512 * 1024),
            ("512kb", 512 * 1024),
            ("512K", 512 * 1024),
            ("256m", 256 * 1024 * 1024),
            ("256M", 256 * 1024 * 1024),
            ("256mb", 256 * 1024 * 1024),
            ("256MiB", 256 * 1024 * 1024),
            ("1g", 1 << 30),
            ("1gb", 1 << 30),
            ("1G", 1 << 30),
            ("2t", 2 << 40),
            ("1p", 1 << 50),
            ("1.5g", 3 << 29),
            ("0.5k", 512),
            (".5k", 512),
            ("1 gb", 1 << 30),
            (" 64m ", 64 << 20),
            ("536870912", 536_870_912),
            // A fraction of a byte is dropped.
            ("1.9", 1),
            ("0.0001k", 0),
        ] {
            assert_eq!(docker_bytes(text), Ok(want), "{text:?}");
        }
    }

    #[test]
    fn docker_sizes_refuse_everything_else() {
        for text in [
            "",
            " ",
            "m",
            "-1",
            "+1",
            "1x",
            "1kk",
            "1bb",
            "1ib",
            "1e3",
            "1,5g",
            "1.2.3",
            "1  g",
            "0x10",
            "1g1",
            "99999999999999999999",
        ] {
            let err = docker_bytes(text).unwrap_err();
            assert!(err.0.contains("is not a size"), "{text:?}: {err}");
        }
        let err = docker_bytes("99999999p").unwrap_err();
        assert_eq!(err.0, "`99999999p` is too large for a size");
    }

    #[test]
    fn docker_cpus_are_decimals_rounded_up_to_thousandths() {
        for (text, want) in [
            ("0", 0),
            ("0.0", 0),
            ("2", 2000),
            ("0.5", 500),
            (".5", 500),
            ("1.", 1000),
            ("1.25", 1250),
            ("0.001", 1),
            ("0.0001", 1),
            ("0.2501", 251),
            ("0.25000000001", 251),
            ("0.2500", 250),
            (" 1.5 ", 1500),
            ("64", 64_000),
        ] {
            assert_eq!(docker_cpu_millis(text), Ok(want), "{text:?}");
        }
        for text in ["", ".", "-1", "1e3", "500m", "half", "1.2.3", "1 000"] {
            let err = docker_cpu_millis(text).unwrap_err();
            assert!(err.0.contains("is not a number of CPUs"), "{text:?}: {err}");
        }
    }

    #[test]
    fn docker_durations_are_go_durations_rounded_up_to_seconds() {
        for (text, want) in [
            ("0", 0),
            ("0s", 0),
            ("30s", 30),
            ("1m30s", 90),
            ("1h", 3600),
            ("1h2m3s", 3723),
            ("1.5m", 90),
            ("1.5s", 2),
            ("500ms", 1),
            ("1500ms", 2),
            ("2000ms", 2),
            ("1s1ns", 2),
            ("1us", 1),
            ("1µs", 1),
            ("0.0000000001ns", 1),
            ("90m", 5400),
            (" 10s ", 10),
        ] {
            assert_eq!(docker_duration_secs(text), Ok(want), "{text:?}");
        }
    }

    #[test]
    fn docker_durations_refuse_everything_else() {
        for text in [
            "", "s", "-5s", "+5s", "5 s", "5x", "5sec", "1d", "1m 30s", "1..5s",
        ] {
            let err = docker_duration_secs(text).unwrap_err();
            assert!(err.0.contains("is not a duration"), "{text:?}: {err}");
        }
        // A bare number is the likely slip, so the message names the fix.
        assert_eq!(
            docker_duration_secs("30").unwrap_err().0,
            "`30` has a number without a unit; write `30s` for seconds"
        );
        assert_eq!(
            docker_duration_secs("1m30").unwrap_err().0,
            "`1m30` has a number without a unit; write `30s` for seconds"
        );
        assert_eq!(
            docker_duration_secs("9999999999h").unwrap_err().0,
            "`9999999999h` is too long for a duration"
        );
    }
}
