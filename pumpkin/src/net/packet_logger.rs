use std::fs::{OpenOptions};
use std::io::Write;
use std::sync::Mutex;
use once_cell::sync::Lazy;

// Global logger for outgoing packets
pub static PACKET_LOGGER: Lazy<Mutex<std::fs::File>> = Lazy::new(|| {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("outgoing_packets.log")
        .expect("Failed to open outgoing_packets.log");
    Mutex::new(file)
});

pub fn log_packet(data: &[u8], context: &str) {
    let mut logger = PACKET_LOGGER.lock().unwrap();
    let _ = writeln!(logger, "{}: {} bytes: {:02X?}", context, data.len(), data);
}
