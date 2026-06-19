use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use rand::RngCore;

use crate::rate_limiter::TokenBucket;
use crate::stats::{draw_dashboard, StatsTracker};

pub fn run_server(
    bind_addr: String,
    port: u16,
    rate_kbps: f64, // Rate in KB/s
) -> std::io::Result<()> {
    let full_addr = format!("{}:{}", bind_addr, port);
    let socket = UdpSocket::bind(&full_addr)?;
    let socket = Arc::new(socket);

    let stats = Arc::new(StatsTracker::new());
    let clients: Arc<Mutex<HashMap<SocketAddr, Arc<AtomicU64>>>> = Arc::new(Mutex::new(HashMap::new()));
    let running = Arc::new(AtomicBool::new(true));

    // Pre-allocate a 64KB random byte pool for high performance
    let mut rng = rand::thread_rng();
    let mut random_pool = vec![0u8; 65536];
    rng.fill_bytes(&mut random_pool);
    let random_pool = Arc::new(random_pool);

    // Spawn stats reporting thread
    let stats_clone = Arc::clone(&stats);
    let clients_clone = Arc::clone(&clients);
    let running_clone = Arc::clone(&running);
    let bind_info = full_addr.clone();

    let stats_thread = thread::spawn(move || {
        let mut is_first = true;
        while running_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            let elapsed = stats_clone.start_time.elapsed();
            let (tx_rate, rx_rate) = stats_clone.tick_rates();
            let tx_total = stats_clone.total_sent.load(Ordering::Relaxed);
            let rx_total = stats_clone.total_received.load(Ordering::Relaxed);

            let active_count = {
                let map = clients_clone.lock().unwrap();
                map.len()
            };
            let status_info = if active_count == 0 {
                "Idle (Waiting for client request)".to_string()
            } else {
                format!("Active ({} client(s) connected)", active_count)
            };

            draw_dashboard(
                &mut is_first,
                "Server",
                &bind_info,
                &status_info,
                elapsed,
                tx_rate,
                tx_total,
                rx_rate,
                rx_total,
            );
        }
    });

    // Receiver loop
    let socket_recv = Arc::clone(&socket);
    let stats_recv = Arc::clone(&stats);
    let clients_recv = Arc::clone(&clients);
    let running_recv = Arc::clone(&running);
    let start_time = stats.start_time;

    let receiver_thread = thread::spawn(move || {
        let mut buf = vec![0u8; 65536];
        while running_recv.load(Ordering::Relaxed) {
            // Set a read timeout so the loop doesn't block indefinitely on shutdown
            if socket_recv.set_read_timeout(Some(Duration::from_millis(500))).is_err() {
                break;
            }

            match socket_recv.recv_from(&mut buf) {
                Ok((size, src_addr)) => {
                    stats_recv.add_received(size);
                    let now_secs = start_time.elapsed().as_secs();

                    // Check if client is already known
                    let mut spawned_sender = false;
                    let last_seen_atomic = {
                        let mut map = clients_recv.lock().unwrap();
                        if let Some(last_seen) = map.get(&src_addr) {
                            last_seen.store(now_secs, Ordering::Relaxed);
                            None
                        } else {
                            let last_seen = Arc::new(AtomicU64::new(now_secs));
                            map.insert(src_addr, Arc::clone(&last_seen));
                            spawned_sender = true;
                            Some(last_seen)
                        }
                    };

                    if spawned_sender {
                        if let Some(last_seen) = last_seen_atomic {
                            let socket_send = Arc::clone(&socket_recv);
                            let stats_send = Arc::clone(&stats_recv);
                            let clients_send = Arc::clone(&clients_recv);
                            let random_pool_send = Arc::clone(&random_pool);
                            let running_send = Arc::clone(&running_recv);

                            thread::spawn(move || {
                                // 5000 KB/s rate limit. 1 token = 1 byte.
                                // Capacity is 64KB for burst tolerance.
                                let rate_bytes = rate_kbps * 1024.0;
                                let mut rate_limiter = TokenBucket::new(rate_bytes, 65536.0);
                                let mut pool_offset = 0;
                                let packet_size = 1400; // Optimal MTU size

                                while running_send.load(Ordering::Relaxed) {
                                    // Check for expiration (40 seconds timeout)
                                    let current_secs = start_time.elapsed().as_secs();
                                    let last_secs = last_seen.load(Ordering::Relaxed);
                                    if current_secs - last_secs > 40 {
                                        // Expired. Remove from clients map and exit thread.
                                        let mut map = clients_send.lock().unwrap();
                                        map.remove(&src_addr);
                                        break;
                                    }

                                    // Consume rate limit tokens
                                    rate_limiter.consume(packet_size);

                                    // Extract data from pre-allocated random pool
                                    let chunk = &random_pool_send[pool_offset..pool_offset + packet_size];
                                    pool_offset = (pool_offset + packet_size) % (random_pool_send.len() - packet_size);

                                    // Send to client
                                    if socket_send.send_to(chunk, src_addr).is_ok() {
                                        stats_send.add_sent(packet_size);
                                    } else {
                                        // Send failure might indicate client disconnected or network down, 
                                        // we let the timeout clean it up or exit if it's persistent.
                                        thread::sleep(Duration::from_millis(10));
                                    }
                                }
                            });
                        }
                    }
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

    // Wait for Ctrl+C signal to shutdown gracefully
    let running_ctrlc = Arc::clone(&running);
    ctrlc::set_handler(move || {
        running_ctrlc.store(false, Ordering::Relaxed);
    }).expect("Error setting Ctrl-C handler");

    // Block main thread until running is false
    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    // Wait for threads to finish
    let _ = receiver_thread.join();
    let _ = stats_thread.join();

    crate::stats::restore_cursor();
    println!("\nServer stopped gracefully.");
    Ok(())
}
