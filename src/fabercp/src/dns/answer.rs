//! Decides what a DNS query gets: an answer for a service name, an error, or
//! a trip to the upstream resolvers.

use std::net::Ipv4Addr;

use faber_api_types::name::validate_dns_label;
use faber_api_types::workload::DNS_SUFFIX;
use hickory_proto::op::{Edns, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::rdata::{A, SOA};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};

use super::{LookupFailed, ServiceIps};

/// Seconds a client may cache a service address.
const ANSWER_TTL: u32 = 30;

/// Seconds a resolver may cache a negative answer (RFC 2308). Short, because
/// services come and go.
const NEGATIVE_TTL: u32 = 5;

/// Labels of a service name before the zone: `<svc>.<env>.<project>`.
const SERVICE_PREFIX_LABELS: usize = 3;

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
    // RFC 6891 section 6.1.3: only EDNS version 0 is implemented.
    if request.edns.as_ref().is_some_and(|edns| edns.version() > 0) {
        return reply(&request, ResponseCode::BADVERS, None);
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
    let ip = match service_ip(&labels, ips) {
        Ok(Some(ip)) => ip,
        Ok(None) => return reply(&request, ResponseCode::NXDomain, None),
        // Not knowing is not the same as there being no such service.
        Err(LookupFailed) => return reply(&request, ResponseCode::ServFail, None),
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

/// Number of labels in the zone, [`DNS_SUFFIX`].
fn zone_len() -> usize {
    DNS_SUFFIX.split('.').count()
}

fn in_zone(labels: &[&[u8]]) -> bool {
    labels.len() >= zone_len()
        && labels
            .iter()
            .rev()
            .zip(DNS_SUFFIX.rsplit('.'))
            .all(|(label, zone)| label.eq_ignore_ascii_case(zone.as_bytes()))
}

/// The virtual IP of the service `labels` names, if it is a well-formed
/// service name that has one. `Ok(None)` for a name that is not a service
/// name, or names a service without an address.
fn service_ip(labels: &[&[u8]], ips: &dyn ServiceIps) -> Result<Option<Ipv4Addr>, LookupFailed> {
    if labels.len() != SERVICE_PREFIX_LABELS + zone_len() {
        return Ok(None);
    }
    let label = |i: usize| -> Option<String> {
        let name = std::str::from_utf8(labels[i]).ok()?.to_ascii_lowercase();
        validate_dns_label(&name).ok()?;
        Some(name)
    };
    let (Some(project), Some(env), Some(svc)) = (label(2), label(1), label(0)) else {
        return Ok(None);
    };
    ips.lookup(&project, &env, &svc)
}

fn a_record(query: &Query, ip: Ipv4Addr) -> Record {
    Record::from_rdata(query.name().clone(), ANSWER_TTL, RData::A(A(ip)))
}

/// The SOA of the zone, sent with negative answers so resolvers can cache
/// them (RFC 2308). Its TTL and minimum are [`NEGATIVE_TTL`]; the other
/// fields are fixed, as the zone is not transferred.
fn zone_soa() -> Record {
    let zone = Name::from_ascii(format!("{DNS_SUFFIX}.")).unwrap_or_else(|_| Name::root());
    let child = |label: &str| zone.prepend_label(label).unwrap_or_else(|_| zone.clone());
    let soa = SOA::new(
        child("ns"),
        child("hostmaster"),
        1,
        3600,
        600,
        86400,
        NEGATIVE_TTL,
    );
    Record::from_rdata(zone.clone(), NEGATIVE_TTL, RData::SOA(soa))
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
    if code == ResponseCode::NXDomain || (code == ResponseCode::NoError && answer.is_none()) {
        message.authorities.push(zone_soa());
    }
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
    fn answers_the_name_the_workload_response_carries() {
        let name = format!(
            "{}.",
            faber_api_types::workload::fqdn("web", "prod", "shop")
        );
        let m = reply_of(answer(&query_packet(&name, RecordType::A), &ips()));
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.answers[0].data, RData::A(A(IP)));
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
        // An unparseable response is dropped, not answered.
        let broken = [0, 9, 0x81, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xff];
        assert_eq!(answer(&broken, &ips()), Outcome::Drop);
    }

    #[test]
    fn dns_failed_lookup_is_servfail_not_nxdomain() {
        let ips = ips();
        ips.set_failing(true);
        let m = reply_of(answer(
            &query_packet("web.prod.shop.faber.local.", RecordType::A),
            &ips,
        ));
        assert_eq!(m.metadata.response_code, ResponseCode::ServFail);
        assert!(m.answers.is_empty());
        // A name that is no service name never reaches the store.
        let m = reply_of(answer(
            &query_packet("bad_name.prod.shop.faber.local.", RecordType::A),
            &ips,
        ));
        assert_eq!(m.metadata.response_code, ResponseCode::NXDomain);
    }

    fn soa_of(m: &Message) -> &hickory_proto::rr::rdata::SOA {
        let [soa] = m.authorities.as_slice() else {
            panic!("expected one authority record, got {:?}", m.authorities);
        };
        assert_eq!(soa.name, Name::from_ascii("faber.local.").unwrap());
        assert_eq!(soa.ttl, NEGATIVE_TTL);
        match &soa.data {
            RData::SOA(soa) => soa,
            other => panic!("expected an SOA, got {other:?}"),
        }
    }

    #[test]
    fn negative_answers_carry_the_zone_soa() {
        for (name, kind, code) in [
            (
                "api.prod.shop.faber.local.",
                RecordType::A,
                ResponseCode::NXDomain,
            ),
            ("faber.local.", RecordType::A, ResponseCode::NXDomain),
            (
                "web.prod.shop.faber.local.",
                RecordType::AAAA,
                ResponseCode::NoError,
            ),
        ] {
            let m = reply_of(answer(&query_packet(name, kind), &ips()));
            assert_eq!(m.metadata.response_code, code, "{name}");
            assert!(m.answers.is_empty());
            assert_eq!(soa_of(&m).minimum, NEGATIVE_TTL, "{name}");
        }
    }

    #[test]
    fn positive_and_failed_answers_carry_no_soa() {
        let m = reply_of(answer(
            &query_packet("web.prod.shop.faber.local.", RecordType::A),
            &ips(),
        ));
        assert!(m.authorities.is_empty());
        let failing = ips();
        failing.set_failing(true);
        let m = reply_of(answer(
            &query_packet("web.prod.shop.faber.local.", RecordType::A),
            &failing,
        ));
        assert!(m.authorities.is_empty());
    }

    fn edns_query(version: u8, name: &str) -> Vec<u8> {
        let mut m = Message::from_vec(&query_packet(name, RecordType::A)).unwrap();
        let mut e = Edns::new();
        e.set_version(version);
        m.set_edns(e);
        m.to_vec().unwrap()
    }

    #[test]
    fn edns_version_above_zero_gets_badvers_with_opt() {
        // Zone names and forwarded names alike: the version is checked first.
        for name in ["web.prod.shop.faber.local.", "example.com."] {
            let m = reply_of(answer(&edns_query(1, name), &ips()));
            let edns = m.edns.as_ref().expect("OPT record");
            assert_eq!(edns.version(), 0, "{name}");
            // BADVERS is 16: the low four bits are 0 and the rest is in the OPT.
            assert_eq!(edns.rcode_high(), 1, "{name}");
            assert_eq!(m.metadata.response_code.low(), 0, "{name}");
            assert!(m.answers.is_empty());
            assert_eq!(m.queries.len(), 1);
            assert_eq!(m.metadata.id, 7);
        }
    }

    #[test]
    fn edns_version_zero_is_answered_normally() {
        let m = reply_of(answer(&edns_query(0, "web.prod.shop.faber.local."), &ips()));
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.edns.as_ref().map(Edns::rcode_high), Some(0));
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
