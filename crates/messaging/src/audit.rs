use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use reliant_datamodel::Event;
use std::sync::mpsc::Receiver;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditMessage {
    pub header: HashMap<String, String>,
    pub payload: serde_json::Value,
}

pub fn event_to_audit(event: &Event, topic: &str, ts: i64) -> AuditMessage {
    let mut header = HashMap::new();
    header.insert("topic".to_string(), topic.to_string());
    header.insert("timestamp".to_string(), ts.to_string());
    AuditMessage {
        header,
        payload: serde_json::to_value(event).unwrap_or(serde_json::Value::Null),
    }
}

fn encode_frame(msg: &AuditMessage) -> Result<Vec<u8>, String> {
    let payload = rmp_serde::to_vec(msg).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_frames(bytes: &[u8]) -> Result<Vec<AuditMessage>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 > bytes.len() {
            return Err("truncated frame header".to_string());
        }
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + len > bytes.len() {
            return Err("truncated frame payload".to_string());
        }
        let msg: AuditMessage =
            rmp_serde::from_slice(&bytes[i..i + len]).map_err(|e| e.to_string())?;
        out.push(msg);
        i += len;
    }
    Ok(out)
}

pub fn write_audit(path: &str, messages: &[AuditMessage]) -> Result<(), String> {
    let mut buf = Vec::new();
    for m in messages {
        buf.extend(encode_frame(m)?);
    }
    let data = if path.ends_with(".zstd") {
        zstd::encode_all(&buf[..], 3).map_err(|e| e.to_string())?
    } else {
        buf
    };
    fs::write(path, data).map_err(|e| e.to_string())?;
    Ok(())
}

pub fn append_audit(path: &str, messages: &[AuditMessage]) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    for m in messages {
        let frame = encode_frame(m)?;
        file.write_all(&frame).map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())?;
    Ok(())
}

pub fn read_audit(path: &str) -> Result<Vec<AuditMessage>, String> {
    let raw = fs::read(path).map_err(|e| e.to_string())?;
    let bytes = if path.ends_with(".zstd") {
        zstd::decode_all(&raw[..]).map_err(|e| e.to_string())?
    } else {
        raw
    };
    decode_frames(&bytes)
}

pub fn compress_audit(src: &str, dst: &str) -> Result<(), String> {
    let raw = fs::read(src).map_err(|e| e.to_string())?;
    let compressed = zstd::encode_all(&raw[..], 3).map_err(|e| e.to_string())?;
    fs::write(dst, compressed).map_err(|e| e.to_string())?;
    Ok(())
}




pub fn drain_receiver_to_audit(rx: Receiver<reliant_datamodel::Event>, topic: &str, path: &str) -> Result<(), String> {
    loop {
        match rx.recv() {
            Ok(ev) => {
                let ts = match &ev {
                    reliant_datamodel::Event::CandleClosed { candle, .. } => candle.timestamp.timestamp(),
                    reliant_datamodel::Event::Tick { timestamp } => *timestamp,
                    _ => 0,
                };
                append_audit(path, &[event_to_audit(&ev, topic, ts)])?;
            }
            Err(_) => break,
        }
    }
    Ok(())
}

pub struct AuditSubscriber {
    pub topic: String,
    pub path: String,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl AuditSubscriber {
    pub fn spawn(rx: std::sync::mpsc::Receiver<reliant_datamodel::Event>, topic: &str, path: &str) -> Self {
        let topic = topic.to_string();
        let path = path.to_string();
        let topic2 = topic.clone();
        let path2 = path.clone();
        let handle = std::thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(ev) => {
                        let ts = match &ev {
                            reliant_datamodel::Event::CandleClosed { candle, .. } => candle.timestamp.timestamp(),
                            reliant_datamodel::Event::Tick { timestamp } => *timestamp,
                            _ => 0,
                        };
                        let _ = append_audit(&path2, &[event_to_audit(&ev, &topic2, ts)]);
                    }
                    Err(_) => break,
                }
            }
        });
        Self { topic, path, handle: Some(handle) }
    }

    pub fn join(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
