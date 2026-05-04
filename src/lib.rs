//! Async HTTP Proxy Server Library
//! 
//! Provides rate limiting, connection pooling, and request routing for HTTP proxies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::error::Error as StdError;

use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use reqwest::Client;
use futures::stream::{Stream, StreamExt};
use tracing::{info, warn, error, debug};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    
    #[error("Route not found: {0}")]
    RouteNotFound(String),
    
    #[error("Invalid host")]
    InvalidHost,
    
    #[error("Max connections reached for host")]
    MaxConnectionsReached,
    
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),
    
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

pub type Result<T> = std::result::Result<T, ProxyError>;

/// Token bucket rate limiter with thread-safe interior mutability
#[derive(Debug)]
pub struct RateLimiter {
    capacity: usize,
    refill_rate: Duration,
    tokens: Arc<Mutex<RateLimiterInner>>,
}

#[derive(Debug)]
struct RateLimiterInner {
    tokens: usize,
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with specified capacity and refill rate
    pub fn new(capacity: usize, refill_rate: Duration) -> Self {
        Self {
            capacity,
            refill_rate,
            tokens: Arc::new(Mutex::new(RateLimiterInner {
                tokens: capacity,
                last_refill: Instant::now(),
            })),
        }
    }
    
    /// Acquire a token asynchronously
    pub async fn acquire(&self) -> Result<()> {
        let mut inner = self.tokens.lock().await;
        
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill);
        let refill_count = (elapsed.as_secs_f64() / self.refill_rate.as_secs_f64()) as usize;
        
        if refill_count > 0 {
            inner.tokens = (inner.tokens + refill_count).min(self.capacity);
            inner.last_refill = now;
            debug!("Refilled {} tokens", refill_count);
        }
        
        if inner.tokens > 0 {
            inner.tokens -= 1;
            debug!("Token acquired, {} remaining", inner.tokens);
            Ok(())
        } else {
            warn!("Rate limit exceeded");
            Err(ProxyError::RateLimitExceeded)
        }
    }
    
    /// Try to acquire without waiting (non-blocking)
    pub fn try_acquire(&self) -> Result<()> {
        let mut inner = match self.tokens.try_lock() {
            Some(guard) => guard,
            None => return Err(ProxyError::RateLimitExceeded),
        };
        
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill);
        let refill_count = (elapsed.as_secs_f64() / self.refill_rate.as_secs_f64()) as usize;
        
        if refill_count > 0 {
            inner.tokens = (inner.tokens + refill_count).min(self.capacity);
            inner.last_refill = now;
        }
        
        if inner.tokens > 0 {
            inner.tokens -= 1;
            Ok(())
        } else {
            Err(ProxyError::RateLimitExceeded)
        }
    }
    
    /// Get current token count
    pub async fn available_tokens(&self) -> usize {
        let inner = self.tokens.lock().await;
        inner.tokens
    }
}

/// Custom future that respects rate limiting
pub struct RateLimitedFuture<F> {
    inner: F,
    limiter: Arc<RateLimiter>,
    acquired: bool,
}

impl<F: Future + Unpin> Future for RateLimitedFuture<F> {
    type Output = Result<F::Output>;
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.acquired {
            match self.limiter.try_acquire() {
                Ok(()) => {
                    self.acquired = true;
                }
                Err(_) => {
                    let waker = cx.waker().clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(100)).await;
                        waker.wake();
                    });
                    return Poll::Pending;
                }
            }
        }
        
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(result) => Poll::Ready(Ok(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Extension trait for adding rate limiting to futures
pub trait WithRateLimiting: Future + Unpin + Sized {
    fn with_rate_limit(self, limiter: Arc<RateLimiter>) -> RateLimitedFuture<Self> {
        RateLimitedFuture {
            inner: self,
            limiter,
            acquired: false,
        }
    }
}

impl<F: Future + Unpin> WithRateLimiting for F {}

/// Connection pool for reusing HTTP clients
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Vec<Client>>>>,
    max_connections_per_host: usize,
    metrics: Arc<PoolMetrics>,
}

struct PoolMetrics {
    total_created: prometheus::IntCounter,
    total_reused: prometheus::IntCounter,
    active_connections: prometheus::IntGauge,
}

impl ConnectionPool {
    /// Create new connection pool
    pub fn new(max_connections_per_host: usize) -> Self {
        let metrics = Arc::new(PoolMetrics {
            total_created: prometheus::IntCounter::new("pool_connections_created", "Total connections created").unwrap(),
            total_reused: prometheus::IntCounter::new("pool_connections_reused", "Total connections reused").unwrap(),
            active_connections: prometheus::IntGauge::new("pool_active_connections", "Active connections").unwrap(),
        });
        
        prometheus::register(Box::new(metrics.total_created.clone())).ok();
        prometheus::register(Box::new(metrics.total_reused.clone())).ok();
        prometheus::register(Box::new(metrics.active_connections.clone())).ok();
        
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_host,
            metrics,
        }
    }
    
    /// Get or create a connection for a host
    pub async fn get_connection(&self, host: &str) -> Result<Client> {
        let mut conns = self.connections.write().await;
        let host_conns = conns.entry(host.to_string()).or_insert_with(Vec::new);
        
        if let Some(client) = host_conns.pop() {
            self.metrics.total_reused.inc();
            self.metrics.active_connections.inc();
            debug!("Reused connection for host: {}", host);
            Ok(client)
        } else if host_conns.len() < self.max_connections_per_host {
            let client = Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(self.max_connections_per_host)
                .build()
                .map_err(|e| ProxyError::HttpClient(e))?;
            self.metrics.total_created.inc();
            self.metrics.active_connections.inc();
            debug!("Created new connection for host: {}", host);
            Ok(client)
        } else {
            warn!("Max connections reached for host: {}", host);
            Err(ProxyError::MaxConnectionsReached)
        }
    }
    
    /// Return connection to pool
    pub async fn return_connection(&self, host: &str, client: Client) {
        let mut conns = self.connections.write().await;
        if let Some(host_conns) = conns.get_mut(host) {
            if host_conns.len() < self.max_connections_per_host {
                host_conns.push(client);
                debug!("Returned connection to pool for host: {}", host);
            }
            // else drop the connection
        }
        self.metrics.active_connections.dec();
    }
    
    /// Get pool statistics
    pub async fn stats(&self) -> HashMap<String, usize> {
        let conns = self.connections.read().await;
        conns.iter().map(|(k, v)| (k.clone(), v.len())).collect()
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        info!("Dropping connection pool");
    }
}

/// Main proxy server orchestrating requests
pub struct ProxyServer {
    rate_limiter: Arc<RateLimiter>,
    connection_pool: Arc<ConnectionPool>,
    routes: Arc<RwLock<HashMap<String, String>>>,
    metrics: Arc<ServerMetrics>,
}

struct ServerMetrics {
    total_requests: prometheus::IntCounter,
    successful_requests: prometheus::IntCounter,
    failed_requests: prometheus::IntCounter,
    request_duration: prometheus::Histogram,
}

impl ProxyServer {
    /// Create new proxy server with rate limit and pool size
    pub fn new(rpm_limit: usize, max_connections_per_host: usize) -> Self {
        let refill_rate = if rpm_limit > 0 {
            Duration::from_secs(60) / rpm_limit as u32
        } else {
            Duration::from_secs(1)
        };
        
        let metrics = Arc::new(ServerMetrics {
            total_requests: prometheus::IntCounter::new("proxy_requests_total", "Total requests").unwrap(),
            successful_requests: prometheus::IntCounter::new("proxy_requests_successful", "Successful requests").unwrap(),
            failed_requests: prometheus::IntCounter::new("proxy_requests_failed", "Failed requests").unwrap(),
            request_duration: prometheus::Histogram::new("proxy_request_duration_seconds", "Request duration").unwrap(),
        });
        
        prometheus::register(Box::new(metrics.total_requests.clone())).ok();
        prometheus::register(Box::new(metrics.successful_requests.clone())).ok();
        prometheus::register(Box::new(metrics.failed_requests.clone())).ok();
        prometheus::register(Box::new(metrics.request_duration.clone())).ok();
        
        Self {
            rate_limiter: Arc::new(RateLimiter::new(rpm_limit, refill_rate)),
            connection_pool: Arc::new(ConnectionPool::new(max_connections_per_host)),
            routes: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        }
    }
    
    /// Add a route mapping
    pub async fn add_route(&self, path: &str, target: &str) {
        self.routes.write().await.insert(path.to_string(), target.to_string());
        info!("Added route: {} -> {}", path, target);
    }
    
    /// Remove a route
    pub async fn remove_route(&self, path: &str) -> Option<String> {
        let result = self.routes.write().await.remove(path);
        if result.is_some() {
            info!("Removed route: {}", path);
        }
        result
    }
    
    /// Handle a single request
    pub async fn handle_request(&self, path: &str) -> Result<String> {
        let start = Instant::now();
        self.metrics.total_requests.inc();
        
        // Rate limiting
        self.rate_limiter.acquire().await?;
        
        // Route lookup
        let target_url = {
            let routes = self.routes.read().await;
            routes.get(path)
                .ok_or_else(|| ProxyError::RouteNotFound(path.to_string()))?
                .clone()
        };
        
        // Parse host
        let url = reqwest::Url::parse(&target_url)
            .map_err(|_| ProxyError::InvalidHost)?;
        let host = url.host_str()
            .ok_or(ProxyError::InvalidHost)?
            .to_string();
        
        // Get connection
        let client = self.connection_pool.get_connection(&host).await?;
        
        // Execute request
        let response = client.get(&target_url)
            .header("User-Agent", "Rust-Proxy-Server/1.0")
            .send()
            .await?;
        
        let body = response.text().await?;
        
        // Return connection
        self.connection_pool.return_connection(&host, client).await;
        
        let duration = start.elapsed();
        self.metrics.successful_requests.inc();
        self.metrics.request_duration.observe(duration.as_secs_f64());
        
        debug!("Request to {} completed in {:?}", path, duration);
        Ok(body)
    }
    
    /// Stream response in chunks
    pub async fn stream_response(&self, path: &str) -> impl Stream<Item = Result<String>> {
        use futures::stream::{self, StreamExt};
        
        let routes = self.routes.read().await;
        let target_url = match routes.get(path).cloned() {
            Some(url) => url,
            None => return stream::once(async { Err(ProxyError::RouteNotFound(path.to_string())) }).boxed(),
        };
        drop(routes);
        
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let limiter = self.rate_limiter.clone();
        
        tokio::spawn(async move {
            if let Err(e) = limiter.acquire().await {
                let _ = tx.send(Err(e)).await;
                return;
            }
            
            let client = match Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(Err(ProxyError::HttpClient(e))).await;
                    return;
                }
            };
            
            let response = match client.get(&target_url).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    let _ = tx.send(Err(ProxyError::HttpClient(e))).await;
                    return;
                }
            };
            
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            if tx.send(Ok(text)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(ProxyError::HttpClient(e))).await;
                        break;
                    }
                }
            }
        });
        
        tokio_stream::wrappers::ReceiverStream::new(rx).boxed()
    }
    
    /// Get server metrics
    pub async fn get_metrics(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
    
    /// Check server health
    pub async fn health_check(&self) -> bool {
        // Check if rate limiter is responsive
        let tokens = self.rate_limiter.available_tokens().await;
        tokens > 0 || true // Proxy is healthy even if rate limited
    }
}

impl Clone for ProxyServer {
    fn clone(&self) -> Self {
        Self {
            rate_limiter: self.rate_limiter.clone(),
            connection_pool: self.connection_pool.clone(),
            routes: self.routes.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
