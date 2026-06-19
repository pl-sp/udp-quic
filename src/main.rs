use clap::{Parser, Subcommand};

mod rate_limiter;
mod stats;
mod server;
mod client;

#[derive(Parser)]
#[command(name = "udp-tool")]
#[command(version = "1.0")]
#[command(author = "DeepMind Antigravity Pair Programmer")]
#[command(about = "High-performance UDP client-server rate-limited communication tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Runs the UDP Server (receives client request, streams random bytes back)
    Server {
        /// Port to bind and listen on (default 58000)
        #[arg(short, long, default_value_t = 58000)]
        port: u16,

        /// Bind address
        #[arg(short, long, default_value = "0.0.0.0")]
        addr: String,

        /// Send rate limit (default 5000KB/s). Supports units (e.g. 5000KB/s, 5000kbps, 5MB/s)
        #[arg(short, long, default_value = "5000KB/s", value_parser = parse_rate)]
        rate: f64,
    },
    /// Runs the UDP Client (sends periodic random packets, receives server stream)
    Client {
        /// Port to bind and listen on (default 58001)
        #[arg(short, long, default_value_t = 58001)]
        port: u16,

        /// Bind address
        #[arg(short, long, default_value = "0.0.0.0")]
        addr: String,

        /// Server address to target (IP:port)
        #[arg(short, long, default_value = "127.0.0.1:58000")]
        server: String,

        /// Time interval in seconds to send periodic packets (default 30)
        #[arg(short, long, default_value_t = 30)]
        interval: u64,
    },
}

/// Parses rate limit string (e.g., "5000KB/s", "5MB/s", "40mbps") into float KB/s.
fn parse_rate(s: &str) -> Result<f64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Rate limit cannot be empty".to_string());
    }

    // Find where the numeric part ends and unit begins
    let mut num_end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() || c == '.' {
            num_end = i + c.len_utf8();
        } else {
            break;
        }
    }

    if num_end == 0 {
        return Err(format!("No numeric value found in rate limit: '{}'", s));
    }

    let num_part = &s[..num_end];
    let unit_part = s[num_end..].trim().to_lowercase();

    let value: f64 = num_part.parse().map_err(|_| format!("Invalid rate number: '{}'", num_part))?;

    if unit_part.is_empty() {
        // Default is Kilobytes per second (KB/s)
        return Ok(value);
    }

    // Standardize unit names
    let unit = unit_part
        .replace("/s", "")
        .replace("/sec", "")
        .replace("ps", "");

    match unit.as_str() {
        "kb" | "k" => {
            // Default to KB/s (Kilobytes/s)
            Ok(value)
        }
        "kbps" | "kbits" | "kbit" | "kb_bit" => {
            // Kilobits per second to Kilobytes per second
            Ok(value / 8.0)
        }
        "mb" | "m" => {
            // Megabytes per second to Kilobytes per second
            Ok(value * 1024.0)
        }
        "mbps" | "mbits" | "mbit" | "mb_bit" => {
            // Megabits per second to Kilobytes per second
            Ok((value * 1000.0) / 8.0)
        }
        "b" | "bps" | "bytes" | "byte" => {
            // Bytes per second to Kilobytes per second
            Ok(value / 1024.0)
        }
        _ => Err(format!(
            "Unknown unit: '{}'. Supported: KB/s, kb/s, MB/s, kbps, mbps",
            unit_part
        )),
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port, addr, rate } => {
            println!("Starting server on {}:{} with rate limit {:.2} KB/s...", addr, port, rate);
            if let Err(e) = server::run_server(addr, port, rate) {
                eprintln!("Server error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Client { port, addr, server, interval } => {
            println!("Starting client on {}:{} connecting to server {} (interval: {}s)...", addr, port, server, interval);
            if let Err(e) = client::run_client(addr, port, server, interval) {
                eprintln!("Client error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
