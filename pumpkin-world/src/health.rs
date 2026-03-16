use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ServerHealth {
    pub scheduler_tick: AtomicU64,
    pub io_read_tick: AtomicU64,
    pub io_write_tick: AtomicU64,
    pub gen_tick: AtomicU64,
    pub net_out_tick: AtomicU64,
}

pub static HEALTH: ServerHealth = ServerHealth {
    scheduler_tick: AtomicU64::new(0),
    io_read_tick: AtomicU64::new(0),
    io_write_tick: AtomicU64::new(0),
    gen_tick: AtomicU64::new(0),
    net_out_tick: AtomicU64::new(0),
};

/// Updates the timestamp for a specific metric to the current Unix time
pub fn mark_tick(metric: &AtomicU64) {
    if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
        metric.store(now.as_secs(), Ordering::Relaxed);
    }
}

pub fn spawn_watchdog() {
    std::thread::Builder::new()
        .name("Watchdog".into())
        .spawn(|| {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3));
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let check = |val: &AtomicU64, name: &str| {
                    let last = val.load(Ordering::Relaxed);
                    if last != 0 && now - last > 5 {
                        eprintln!(
                            "\x1b[91m[WATCHDOG] !!! ALERT !!! {} has stopped ticking for {}s\x1b[0m",
                            name,
                            now - last
                        );
                    }
                };

                check(&HEALTH.scheduler_tick, "SCHEDULER");
                check(&HEALTH.io_read_tick, "IO_READ");
                check(&HEALTH.gen_tick, "GENERATION_WORKER");
                check(&HEALTH.net_out_tick, "NETWORK_OUT");
            }
        })
        .expect("Failed to spawn Watchdog thread");
}
