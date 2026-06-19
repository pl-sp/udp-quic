use std::net::{UdpSocket, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Instant, Duration};
use rand::{Rng, RngCore};

use crate::stats::{draw_dashboard, StatsTracker};

pub fn run_client(
    bind_addr: String,
    port: u16,
    server_addr_str: String,
    send_interval_secs: u64,
) -> std::io::Result<()> {
    let full_bind_addr = format!("{}:{}", bind_addr, port);
    let socket = UdpSocket::bind(&full_bind_addr)?;
    let socket = Arc::new(socket);

    let server_addr: SocketAddr = server_addr_str.parse()
        .expect("Invalid server IP:port address format");

    let stats = Arc::new(StatsTracker::new());
    let running = Arc::new(AtomicBool::new(true));

    // Spawn stats reporting thread
    let stats_clone = Arc::clone(&stats);
    let running_clone = Arc::clone(&running);
    let bind_info = format!("{} -> {}", full_bind_addr, server_addr_str);

    let stats_thread = thread::spawn(move || {
        let mut is_first = true;
        while running_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            let elapsed = stats_clone.start_time.elapsed();
            let (tx_rate, rx_rate) = stats_clone.tick_rates();
            let tx_total = stats_clone.total_sent.load(Ordering::Relaxed);
            let rx_total = stats_clone.total_received.load(Ordering::Relaxed);

            draw_dashboard(
                &mut is_first,
                "Client",
                &bind_info,
                "Connected & Streaming",
                elapsed,
                tx_rate,
                tx_total,
                rx_rate,
                rx_total,
            );
        }
    });

    // Spawn CSV logging thread to record receive rate every 2 seconds
    let stats_csv = Arc::clone(&stats);
    let running_csv = Arc::clone(&running);
    let csv_thread = thread::spawn(move || {
        use std::fs::File;
        use std::io::Write;

        let file_path = "1.csv";
        let mut file = match File::create(file_path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to create CSV file {}: {}", file_path, e);
                return;
            }
        };

        // Write CSV header
        if let Err(e) = writeln!(file, "Elapsed (s),Rate (B/s),Rate (KB/s),Total Received (Bytes)") {
            eprintln!("Failed to write CSV header: {}", e);
            return;
        }

        let mut last_total_rx = 0;
        let mut last_tick = Instant::now();
        let start_time = stats_csv.start_time;

        while running_csv.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(2));

            let now = Instant::now();
            let elapsed_dur = now.duration_since(last_tick);
            let elapsed_secs = elapsed_dur.as_secs_f64();
            if elapsed_secs == 0.0 {
                continue;
            }

            let total_rx = stats_csv.total_received.load(Ordering::Relaxed);
            let rx_diff = if total_rx >= last_total_rx {
                total_rx - last_total_rx
            } else {
                0
            };

            let rx_rate = rx_diff as f64 / elapsed_secs;
            let rx_rate_kb = rx_rate / 1024.0;
            let total_elapsed = start_time.elapsed().as_secs();

            if let Err(e) = writeln!(
                file,
                "{},{:.2},{:.2},{}",
                total_elapsed,
                rx_rate,
                rx_rate_kb,
                total_rx
            ) {
                eprintln!("Failed to write to CSV: {}", e);
            }
            let _ = file.flush();

            last_total_rx = total_rx;
            last_tick = now;
        }
    });

    // Spawn receiver thread
    let socket_recv = Arc::clone(&socket);
    let stats_recv = Arc::clone(&stats);
    let running_recv = Arc::clone(&running);

    let receiver_thread = thread::spawn(move || {
        let mut buf = vec![0u8; 65536];
        while running_recv.load(Ordering::Relaxed) {
            // Set a read timeout so the loop doesn't block indefinitely on shutdown
            if socket_recv.set_read_timeout(Some(Duration::from_millis(500))).is_err() {
                break;
            }

            match socket_recv.recv_from(&mut buf) {
                Ok((size, _src_addr)) => {
                    stats_recv.add_received(size);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                    // Timeout hit, just loop and check running flag
                    continue;
                }
                Err(_) => {
                    break;
                }
            }
        }
    });

    // Spawn sender thread (Sends a packet immediately, then every send_interval_secs)
    let socket_send = Arc::clone(&socket);
    let stats_send = Arc::clone(&stats);
    let running_send = Arc::clone(&running);

    let sender_thread = thread::spawn(move || {
        let mut rng = rand::thread_rng();

        // 1. Send initial heartbeat to register with the server immediately
        let initial_payload = b"REGISTRATION_HEARTBEAT";
        if socket_send.send_to(initial_payload, server_addr).is_ok() {
            stats_send.add_sent(initial_payload.len());
        }

        // 2. Loop to send packets periodically
        let interval = Duration::from_secs(send_interval_secs);
        let mut last_send = Instant::now();

        while running_send.load(Ordering::Relaxed) {
            if last_send.elapsed() >= interval {
                // Random packet length between 128 and 1024 bytes
                let len = rng.gen_range(128..=1024);
                let mut data = vec![0u8; len];
                rng.fill_bytes(&mut data);

                if socket_send.send_to(&data, server_addr).is_ok() {
                    stats_send.add_sent(len);
                }
                last_send = Instant::now();
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Set Ctrl+C handler to exit cleanly
    let running_ctrlc = Arc::clone(&running);
    ctrlc::set_handler(move || {
        running_ctrlc.store(false, Ordering::Relaxed);
    }).expect("Error setting Ctrl-C handler");

    // Wait until running is set to false
    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    // Join threads
    let _ = receiver_thread.join();
    let _ = sender_thread.join();
    let _ = stats_thread.join();
    let _ = csv_thread.join();

    crate::stats::restore_cursor();
    println!("\nClient stopped gracefully.");
    Ok(())
}
