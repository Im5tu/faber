//! The CNI network config `<cni_conf_dir>/10-faber.conflist`.
//!
//! One `bridge` plugin with `host-local` IPAM on this node's container
//! subnet. containerd watches the directory, so a new or removed file is
//! picked up without a restart.

use serde::Serialize;

use crate::PodNetworkSpec;

/// File name of the network config inside the CNI conf dir.
pub const CONFLIST_FILE: &str = "10-faber.conflist";

/// CNI spec version of the file. containerd 1.6 rejects 1.1.0.
const CNI_VERSION: &str = "1.0.0";
/// Name of the network. host-local keeps its leases in a directory of this
/// name under [`IPAM_DATA_DIR`].
const NETWORK_NAME: &str = "faber";
/// Where host-local keeps its leases. Under `/run`, so they do not survive a
/// reboot.
const IPAM_DATA_DIR: &str = "/run/cni/networks";

// Fields are serialised in declaration order, which keeps the output stable.

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfList<'a> {
    cni_version: &'static str,
    name: &'static str,
    plugins: [Bridge<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Bridge<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    bridge: &'a str,
    /// The bridge takes the first address of the subnet.
    is_gateway: bool,
    /// Containers get a default route through the bridge.
    is_default_gateway: bool,
    /// Replace the bridge address when the subnet changes.
    force_address: bool,
    /// No NAT: container egress is off, and iptables is not installed.
    ip_masq: bool,
    /// A container can reach itself through the bridge.
    hairpin_mode: bool,
    mtu: u16,
    ipam: HostLocal,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HostLocal {
    #[serde(rename = "type")]
    kind: &'static str,
    ranges: [[Range; 1]; 1],
    data_dir: &'static str,
}

#[derive(Serialize)]
struct Range {
    subnet: String,
}

/// The conflist for `network`: pretty-printed JSON with a trailing newline.
/// Deterministic for equal input.
pub(crate) fn render(network: &PodNetworkSpec, bridge: &str) -> String {
    let conf = ConfList {
        cni_version: CNI_VERSION,
        name: NETWORK_NAME,
        plugins: [Bridge {
            kind: "bridge",
            bridge,
            is_gateway: true,
            is_default_gateway: true,
            force_address: true,
            ip_masq: false,
            hairpin_mode: true,
            mtu: network.mtu,
            ipam: HostLocal {
                kind: "host-local",
                ranges: [[Range {
                    subnet: network.subnet.to_string(),
                }]],
                data_dir: IPAM_DATA_DIR,
            },
        }],
    };
    let mut out = serde_json::to_string_pretty(&conf)
        .expect("a struct of strings, bools and integers serialises");
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network() -> PodNetworkSpec {
        PodNetworkSpec {
            subnet: "10.200.3.0/24".parse().unwrap(),
            mtu: 1420,
        }
    }

    #[test]
    fn render_golden() {
        let expected = r#"{
  "cniVersion": "1.0.0",
  "name": "faber",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "faberbr0",
      "isGateway": true,
      "isDefaultGateway": true,
      "forceAddress": true,
      "ipMasq": false,
      "hairpinMode": true,
      "mtu": 1420,
      "ipam": {
        "type": "host-local",
        "ranges": [
          [
            {
              "subnet": "10.200.3.0/24"
            }
          ]
        ],
        "dataDir": "/run/cni/networks"
      }
    }
  ]
}
"#;
        assert_eq!(render(&network(), "faberbr0"), expected);
    }

    #[test]
    fn render_follows_the_spec() {
        let out = render(
            &PodNetworkSpec {
                subnet: "10.200.7.0/24".parse().unwrap(),
                mtu: 1380,
            },
            "br\"x",
        );
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let plugin = &v["plugins"][0];
        assert_eq!(v["plugins"].as_array().unwrap().len(), 1);
        assert_eq!(plugin["bridge"], "br\"x");
        assert_eq!(plugin["mtu"], 1380);
        assert_eq!(plugin["ipam"]["ranges"][0][0]["subnet"], "10.200.7.0/24");
    }
}
