use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, Duration};

/// StatsTracker manages atomic counters for tracking network statistics.
pub struct StatsTracker {
    pub total_sent: AtomicU64,
    pub total_received: AtomicU64,
    pub interval_sent: AtomicU64,
    pub interval_received: AtomicU64,
    pub start_time: Instant,
}

impl StatsTracker {
    pub fn new() -> Self {
        Self {
            total_sent: AtomicU64::new(0),
            total_received: AtomicU64::new(0),
            interval_sent: AtomicU64::new(0),
            interval_received: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn add_sent(&self, bytes: usize) {
        let bytes = bytes as u64;
        self.total_sent.fetch_add(bytes, Ordering::Relaxed);
        self.interval_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_received(&self, bytes: usize) {
        let bytes = bytes as u64;
        self.total_received.fetch_add(bytes, Ordering::Relaxed);
        self.interval_received.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn tick_rates(&self) -> (u64, u64) {
        let sent = self.interval_sent.swap(0, Ordering::Relaxed);
        let recv = self.interval_received.swap(0, Ordering::Relaxed);
        (sent, recv)
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_rate(bytes_per_sec: u64) -> String {
    let kb_per_sec = bytes_per_sec as f64 / 1024.0;
    let mbps = (bytes_per_sec * 8) as f64 / 1_000_000.0;
    if kb_per_sec >= 1024.0 {
        format!("{:.2} MB/s ({:.2} Mbps)", kb_per_sec / 1024.0, mbps)
    } else {
        format!("{:.2} KB/s ({:.2} Mbps)", kb_per_sec, mbps)
    }
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, secs)
}

/// Helper to render the dashboard using ANSI escape codes.
pub fn draw_dashboard(
    is_first_print: &mut bool,
    role: &str,
    bind_info: &str,
    status_info: &str,
    elapsed: Duration,
    tx_rate: u64,
    tx_total: u64,
    rx_rate: u64,
    rx_total: u64,
) {
    // 12 lines will be printed
    let height = 12;

    if !*is_first_print {
        // Move cursor up by height to overwrite the previous output
        print!("\x1B[{}A", height);
    } else {
        *is_first_print = false;
        // Hide the cursor on first print
        print!("\x1B[?25l");
    }

    let cyan = "\x1B[36m";
    let green = "\x1B[32m";
    let yellow = "\x1B[33m";
    let bold = "\x1B[1m";
    let reset = "\x1B[0m";

    println!("┌────────────────────────────────────────────────────────┐");
    println!("│             {}UDP TRANSFER MONITOR{}                      │", bold, reset);
    println!("├────────────────────────────────────────────────────────┤");
    println!("│ Role: {}{:<48}{} │", cyan, role, reset);
    println!("│ Endpoint: {:<44} │", bind_info);
    println!("│ Status: {:<46} │", status_info);
    println!("│ Elapsed Time: {}{:<41}{} │", yellow, format_duration(elapsed), reset);
    println!("├────────────────────────────────────────────────────────┤");
    println!("│ [TX] Send Rate:  {}{:<38}{} │", green, format_rate(tx_rate), reset);
    println!("│ [TX] Total Sent: {:<38} │", format_bytes(tx_total));
    println!("├────────────────────────────────────────────────────────┤");
    println!("│ [RX] Recv Rate:  {}{:<38}{} │", yellow, format_rate(rx_rate), reset);
    println!("│ [RX] Total Recv: {:<38} │", format_bytes(rx_total));
    println!("└────────────────────────────────────────────────────────┘");
}

/// Restores the cursor visibility
pub fn restore_cursor() {
    print!("\x1B[?25h");
}
