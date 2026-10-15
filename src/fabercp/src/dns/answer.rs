//! Decides what a DNS query gets: an answer for a service name, an error, or
//! a trip to the upstream resolvers.

use std::net::Ipv4Addr;

use faber_api_types::name::validate_dns_label;
use hickory_proto::op::{Edns, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{DNSClass, RData, Record, RecordType};

use super::ServiceIps;

/// Seconds a client may cache a service address.
const ANSWER_TTL: u32 = 30;

/// Labels of the zone every service name sits under.
const ZONE: [&[u8]; 2] = [b"faber", b"local"];

/// Labels of a service name: `<svc>.<env>.<project>` and the zone.
const SERVICE_NAME_LABELS: usize = 3 + ZONE.len();

/// Size of a DNS header, the least a packet needs to be answered at all.
const HEADER_LEN: usize = 12;

/// What to do with one query packet.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Send nothing: the packet is not a query.
    Drop,
    /// Send this packet back.
    Reply(Vec<u8>),
    /// Ask the upstream resolvers.
    Forward,
}

/// Decides the fate of `packet`. Names under `faber.local` are answered from
/// `ips`; every other name is [`Outcome::Forward`].
pub fn answer(packet: &[u8], ips: &dyn ServiceIps) -> Outcome {
    let Ok(request) = Message::from_vec(packet) else {
        // Never answer a response, however broken: two servers could
        // otherwise trade error replies forever.
        if is_response(packet) {
            return Outcome::Drop;
        }
        return error_reply(packet, ResponseCode::FormErr).map_or(Outcome::Drop, Outcome::Reply);
    };
    if request.metadata.message_type == MessageType::Response {
        return Outcome::Drop;
    }
    if request.metadata.op_code != OpCode::Query {
        return reply(&request, ResponseCode::NotImp, None);
    }
    let [query] = request.queries.as_slice() else {
        return reply(&request, ResponseCode::FormErr, None);
    };
    let labels: Vec<&[u8]> = query.name().iter().collect();
    if !in_zone(&labels) {
        return Outcome::Forward;
    }
    if query.query_class() != DNSClass::IN {
        return reply(&request, ResponseCode::Refused, None);
    }
    let Some(ip) = service_ip(&labels, ips) else {
        return reply(&request, ResponseCode::NXDomain, None);
    };
    let record = (query.query_type() == RecordType::A).then(|| a_record(query, ip));
    reply(&request, ResponseCode::NoError, record)
}

/// A response to `packet` that carries only `code`: its question when the
/// packet parses, else just its id. `None` when there is not even a header.
pub fn error_reply(packet: &[u8], code: ResponseCode) -> Option<Vec<u8>> {
    if packet.len() < HEADER_LEN {
        return None;
    }
    let message = match Message::from_vec(packet) {
        Ok(request) => {
            let mut message = response_to(&request, code);
            message.queries.clone_from(&request.queries);
            message
        }
        Err(_) => Message::error_msg(
            u16::from_be_bytes([packet[0], packet[1]]),
            OpCode::Query,
            code,
        ),
    };
    message.to_vec().ok()
}

/// Whether the header of `packet` has the QR bit set.
fn is_response(packet: &[u8]) -> bool {
    packet.get(2).is_some_and(|flags| flags & 0x80 != 0)
}

/// Size of a UDP reply when the client sent no EDNS.
const PLAIN_UDP_SIZE: usize = 512;

/// `response` as it may go to the sender of `query` over UDP: unchanged when
/// it fits the size the client accepts (its EDNS payload size, or 512 bytes
/// without EDNS), else cut to the question with the TC bit set so the client
/// retries over TCP.
pub fn fit_udp(query: &[u8], response: Vec<u8>) -> Vec<u8> {
    let limit = Message::from_vec(query).map_or(PLAIN_UDP_SIZE, |q| usize::from(q.max_payload()));
    if response.len() <= limit {
        return response;
    }
    if let Ok(message) = Message::from_vec(&response)
        && let Ok(cut) = message.truncate().to_vec()
        && cut.len() <= limit
    {
        return cut;
    }
    // Not parseable, or the question alone does not fit: a bare header.
    let mut header = response;
    header.truncate(HEADER_LEN);
    header[2] |= 0x02;
    header[4..HEADER_LEN].fill(0);
    header
}

fn in_zone(labels: &[&[u8]]) -> bool {
    labels.len() >= ZONE.len()
        && labels[labels.len() - ZONE.len()..]
            .iter()
            .zip(ZONE)
            .all(|(label, zone)| label.eq_ignore_ascii_case(zone))
}

/// The virtual IP of the service `labels` names, if it is a well-formed
/// service name that has one.
fn service_ip(labels: &[&[u8]], ips: &dyn ServiceIps) -> Option<Ipv4Addr> {
    if labels.len() != SERVICE_NAME_LABELS {
        return None;
    }
    let label = |i: usize| -> Option<String> {
        let name = std::str::from_utf8(labels[i]).ok()?.to_ascii_lowercase();
        validate_dns_label(&name).ok()?;
        Some(name)
    };
    ips.lookup(&label(2)?, &label(1)?, &label(0)?)
}

fn a_record(query: &Query, ip: Ipv4Addr) -> Record {
    Record::from_rdata(query.name().clone(), ANSWER_TTL, RData::A(A(ip)))
}

fn response_to(request: &Message, code: ResponseCode) -> Message {
    let mut message = Message::error_msg(request.metadata.id, request.metadata.op_code, code);
    message.metadata.recursion_desired = request.metadata.recursion_desired;
    message.metadata.checking_disabled = request.metadata.checking_disabled;
    if request.edns.is_some() {
        message.edns = Some(Edns::new());
    }
    message
}

fn reply(request: &Message, code: ResponseCode, answer: Option<Record>) -> Outcome {
    let mut message = response_to(request, code);
    message.metadata.authoritative = true;
    message.queries.clone_from(&request.queries);
    message.answers.extend(answer);
    message.to_vec().map_or(Outcome::Drop, Outcome::Reply)
}

#[cfg(test)]
mod tests {
    use hickory_proto::rr::Name;

    use super::*;
    use crate::dns::InMemoryServiceIps;

    const IP: Ipv4Addr = Ipv4Addr::new(10, 201, 0, 7);

    fn ips() -> InMemoryServiceIps {
        let ips = InMemoryServiceIps::default();
        ips.set("shop", "prod", "web", IP);
        ips
    }

    fn query_packet(name: &str, kind: RecordType) -> Vec<u8> {
        let mut m = Message::query();
        m.metadata.id = 7;
        m.metadata.recursion_desired = true;
        m.add_query(Query::query(Name::from_ascii(name).unwrap(), kind));
        m.to_vec().unwrap()
    }

    fn reply_of(outcome: Outcome) -> Message {
        match outcome {
            Outcome::Reply(bytes) => Message::from_vec(&bytes).unwrap(),
            other => panic!("expected a reply, got {other:?}"),
        }
    }

    #[test]
    fn service_name_gets_its_virtual_ip() {
        let m = reply_of(answer(
            &query_packet("web.prod.shop.faber.local.", RecordType::A),
            &ips(),
        ));
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert_eq!(m.metadata.id, 7);
        assert!(m.metadata.authoritative);
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.answers[0].data, RData::A(A(IP)));
        assert_eq!(m.queries.len(), 1);
    }

    #[test]
    fn matching_ignores_case() {
        let m = reply_of(answer(
            &query_packet("WeB.PROD.Shop.Faber.LOCAL.", RecordType::A),
            &ips(),
        ));
        assert_eq!(m.answers.len(), 1);
    }

    #[test]
    fn unknown_names_under_the_zone_are_nxdomain() {
        for name in [
            "api.prod.shop.faber.local.",
            "web.prod.faber.local.",
            "x.web.prod.shop.faber.local.",
            "faber.local.",
            "bad_name.prod.shop.faber.local.",
        ] {
            let m = reply_of(answer(&query_packet(name, RecordType::A), &ips()));
            assert_eq!(m.metadata.response_code, ResponseCode::NXDomain, "{name}");
            assert!(m.answers.is_empty());
        }
    }

    #[test]
    fn aaaa_for_a_service_is_empty_noerror() {
        let m = reply_of(answer(
            &query_packet("web.prod.shop.faber.local.", RecordType::AAAA),
            &ips(),
        ));
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert!(m.answers.is_empty());
    }

    #[test]
    fn other_names_are_forwarded() {
        let out = answer(&query_packet("example.com.", RecordType::A), &ips());
        assert_eq!(out, Outcome::Forward);
        // Only the exact zone suffix counts.
        let out = answer(
            &query_packet("web.prod.shop.faber.local.example.com.", RecordType::A),
            &ips(),
        );
        assert_eq!(out, Outcome::Forward);
    }

    #[test]
    fn responses_are_dropped_and_garbage_gets_formerr() {
        let mut m = Message::response(1, OpCode::Query);
        m.add_query(Query::query(
            Name::from_ascii("a.b.").unwrap(),
            RecordType::A,
        ));
        assert_eq!(answer(&m.to_vec().unwrap(), &ips()), Outcome::Drop);

        let mut garbage = vec![0, 9, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        garbage.push(0xff);
        let m = reply_of(answer(&garbage, &ips()));
        assert_eq!(m.metadata.id, 9);
        assert_eq!(m.metadata.response_code, ResponseCode::FormErr);
        assert_eq!(answer(&[1, 2, 3], &ips()), Outcome::Drop);
    }

    #[test]
    fn dns_unparseable_response_is_dropped() {
        let broken = [0, 9, 0x81, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xff];
        assert_eq!(answer(&broken, &ips()), Outcome::Drop);
    }

    fn big_response(query: &[u8]) -> Vec<u8> {
        let request = Message::from_vec(query).unwrap();
        let mut m = Message::response(request.metadata.id, OpCode::Query);
        m.queries.clone_from(&request.queries);
        for i in 0..60u8 {
            m.add_answer(Record::from_rdata(
                request.queries[0].name().clone(),
                60,
                RData::A(A(Ipv4Addr::new(1, 1, 1, i))),
            ));
        }
        m.to_vec().unwrap()
    }

    #[test]
    fn dns_udp_reply_over_the_clients_size_is_truncated() {
        let plain = query_packet("example.com.", RecordType::A);
        let big = big_response(&plain);
        assert!(big.len() > 512);
        // No EDNS: cut to 512.
        let cut = Message::from_vec(&fit_udp(&plain, big.clone())).unwrap();
        assert!(cut.metadata.truncation);
        assert!(cut.answers.is_empty());
        assert_eq!(cut.queries.len(), 1);
        assert_eq!(cut.metadata.id, 7);
        // EDNS 4096: fits as is.
        let mut edns = Message::from_vec(&plain).unwrap();
        let mut e = Edns::new();
        e.set_max_payload(4096);
        edns.set_edns(e);
        assert_eq!(fit_udp(&edns.to_vec().unwrap(), big.clone()), big);
        // Garbage that is too big still goes out cut.
        let cut = fit_udp(&plain, vec![0xaa; 600]);
        assert_eq!(cut.len(), 12);
        assert_eq!(cut[2] & 0x02, 0x02);
    }
}
