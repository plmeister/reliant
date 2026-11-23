
#[test]
fn audit_roundtrip_msgpack_zstd() {
    use reliant_datamodel::Event;
    use reliant_messaging::{write_audit, read_audit, event_to_audit};
    let ev = Event::Tick { timestamp: 42 };
    let msgs = vec![event_to_audit(&ev, "ingress", 42)];
    let path = "/tmp/oc/audit_test.dat.zstd";
    write_audit(path, &msgs).unwrap();
    let back = read_audit(path).unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].header.get("topic").unwrap(), "ingress");
    assert_eq!(back[0].header.get("timestamp").unwrap(), "42");
}


#[test]
fn audit_append_accumulates() {
    use reliant_datamodel::Event;
    use reliant_messaging::{append_audit, read_audit, event_to_audit};
    let path = "/tmp/oc/audit_append.dat";
    let _ = std::fs::remove_file(path);
    let ev1 = Event::Tick { timestamp: 1 };
    append_audit(path, &[event_to_audit(&ev1, "ingress", 1)]).unwrap();
    let ev2 = Event::Tick { timestamp: 2 };
    append_audit(path, &[event_to_audit(&ev2, "ingress", 2)]).unwrap();
    let back = read_audit(path).unwrap();
    assert_eq!(back.len(), 2);
}



#[test]
fn audit_compress_roundtrip() {
    use reliant_datamodel::Event;
    use reliant_messaging::{write_audit, read_audit, compress_audit, event_to_audit};
    let dat = "/tmp/oc/audit_c.dat";
    let zst = "/tmp/oc/audit_c.dat.zstd";
    let _ = std::fs::remove_file(dat);
    let _ = std::fs::remove_file(zst);
    let ev = Event::Tick { timestamp: 7 };
    write_audit(dat, &[event_to_audit(&ev, "ingress", 7)]).unwrap();
    compress_audit(dat, zst).unwrap();
    // reader auto-detects .zstd
    let back = read_audit(zst).unwrap();
    assert_eq!(back.len(), 1);
    // .dat still readable
    let back2 = read_audit(dat).unwrap();
    assert_eq!(back2.len(), 1);
}


#[test]
fn drain_receiver_to_audit_writes_dat() {
    use reliant_datamodel::Event;
    use reliant_messaging::{drain_receiver_to_audit, read_audit};
    use std::sync::mpsc;
    let path = "/tmp/oc/drain.dat";
    let _ = std::fs::remove_file(path);
    let (tx, rx) = mpsc::channel();
    tx.send(Event::Tick { timestamp: 5 }).unwrap();
    tx.send(Event::Tick { timestamp: 6 }).unwrap();
    drop(tx);
    drain_receiver_to_audit(rx, "ingress", path).unwrap();
    let back = read_audit(path).unwrap();
    assert_eq!(back.len(), 2);
}


#[test]
fn audit_subscriber_auto_records() {
    use reliant_datamodel::Event;
    use reliant_messaging::{AuditSubscriber, read_audit};
    use std::sync::mpsc;
    let path = "/tmp/oc/sub.dat";
    let _ = std::fs::remove_file(path);
    let (tx, rx) = mpsc::channel();
    let sub = AuditSubscriber::spawn(rx, "ingress", path);
    tx.send(Event::Tick { timestamp: 9 }).unwrap();
    drop(tx);
    sub.join();
    let back = read_audit(path).unwrap();
    assert_eq!(back.len(), 1);
}

