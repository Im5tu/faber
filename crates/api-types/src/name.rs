//! The name rule shared by nodes, projects, environments, workloads,
//! containers and volumes: a DNS label.

/// Longest name accepted: the length limit of a DNS label.
pub const MAX_DNS_LABEL_LEN: usize = 63;

/// Checks that `name` is a DNS label: 1 to 63 characters of `a-z`, `0-9`
/// and `-`, with no hyphen at either end. The error is a message that
/// starts with `name`.
///
/// A name that passes holds no `/`, `.` or `%`, so it is safe as one
/// segment of a URL path or of a storage key.
pub fn validate_dns_label(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_DNS_LABEL_LEN {
        return Err("name must be between 1 and 63 characters".to_string());
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err("name may only contain a-z, 0-9 and hyphens".to_string());
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("name must not start or end with a hyphen".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_dns_labels() {
        let longest = "a".repeat(MAX_DNS_LABEL_LEN);
        for good in ["a", "0", "node-1", "web-eu-2", "9lives", "a--b", &longest] {
            assert_eq!(validate_dns_label(good), Ok(()), "{good}");
        }
    }

    #[test]
    fn rejects_everything_else_with_a_message_per_rule() {
        let too_long = "a".repeat(MAX_DNS_LABEL_LEN + 1);
        for bad in ["", &too_long] {
            assert_eq!(
                validate_dns_label(bad).unwrap_err(),
                "name must be between 1 and 63 characters",
                "{bad:?}"
            );
        }
        for bad in [
            "Node-1", "node_1", "node.1", "node 1", " node", "node\n", "a/b", "..", "a%2f", "é",
        ] {
            assert_eq!(
                validate_dns_label(bad).unwrap_err(),
                "name may only contain a-z, 0-9 and hyphens",
                "{bad:?}"
            );
        }
        for bad in ["-a", "a-", "-"] {
            assert_eq!(
                validate_dns_label(bad).unwrap_err(),
                "name must not start or end with a hyphen",
                "{bad:?}"
            );
        }
    }

    /// The length is counted in bytes and checked before the other rules.
    #[test]
    fn the_length_rule_comes_first_and_counts_bytes() {
        for bad in [
            "A".repeat(MAX_DNS_LABEL_LEN + 1),
            "-".repeat(MAX_DNS_LABEL_LEN + 1),
            "é".repeat(MAX_DNS_LABEL_LEN / 2 + 1),
        ] {
            assert_eq!(
                validate_dns_label(&bad).unwrap_err(),
                "name must be between 1 and 63 characters",
                "{bad:?}"
            );
        }
        // At the limit the other rules still apply.
        let hyphen_last = format!("{}-", "a".repeat(MAX_DNS_LABEL_LEN - 1));
        assert_eq!(
            validate_dns_label(&hyphen_last).unwrap_err(),
            "name must not start or end with a hyphen"
        );
        let hyphens_inside = format!("a{}a", "-".repeat(MAX_DNS_LABEL_LEN - 2));
        assert_eq!(validate_dns_label(&hyphens_inside), Ok(()));
        assert_eq!(
            validate_dns_label(&"A".repeat(MAX_DNS_LABEL_LEN)).unwrap_err(),
            "name may only contain a-z, 0-9 and hyphens"
        );
    }
}
