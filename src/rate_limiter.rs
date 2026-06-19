use std::time::{Instant, Duration};

/// A Token Bucket rate limiter to control bandwidth usage.
/// One token represents one byte.
pub struct TokenBucket {
    capacity: f64,
    rate: f64, // tokens (bytes) per second
    tokens: f64,
    last_update: Instant,
}

impl TokenBucket {
    /// Creates a new TokenBucket with a given rate (bytes per second) and capacity.
    pub fn new(rate_bytes_per_sec: f64, capacity_bytes: f64) -> Self {
        Self {
            capacity: capacity_bytes,
            rate: rate_bytes_per_sec,
            tokens: capacity_bytes,
            last_update: Instant::now(),
        }
    }

    /// Consumes a given amount of bytes, sleeping if necessary to comply with the rate limit.
    pub fn consume(&mut self, amount: usize) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;

        // Replenish tokens based on elapsed time
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);

        let amount_f = amount as f64;
        if self.tokens >= amount_f {
            // We have enough tokens, consume them and return immediately
            self.tokens -= amount_f;
        } else {
            // We don't have enough tokens. Calculate the deficit and sleep time required.
            let needed = amount_f - self.tokens;
            let sleep_secs = needed / self.rate;
            
            // Sleep to let tokens accumulate
            std::thread::sleep(Duration::from_secs_f64(sleep_secs));

            // Replenish tokens after sleep
            let post_sleep_now = Instant::now();
            let elapsed_post = post_sleep_now.duration_since(self.last_update).as_secs_f64();
            self.last_update = post_sleep_now;
            self.tokens = (self.tokens + elapsed_post * self.rate).min(self.capacity);

            // Consume tokens
            if self.tokens >= amount_f {
                self.tokens -= amount_f;
            } else {
                self.tokens = 0.0;
            }
        }
    }
}
