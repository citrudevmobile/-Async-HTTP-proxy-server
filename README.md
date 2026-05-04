# Rust Async Proxy Server

A lightweight, production-ready HTTP proxy server with built-in rate limiting, connection pooling, and streaming support.

## Features

- **Rate Limiting**: 60 requests per minute default (configurable) with automatic token refill
- **Connection Pooling**: Reuses HTTP connections to minimize overhead and socket exhaustion
- **Concurrent Request Handling**: Handles multiple simultaneous requests efficiently
- **Streaming Responses**: Processes large responses in chunks with backpressure control
- **Route Mapping**: Map local paths to external API endpoints
- **Memory Efficient**: ~2-5MB baseline memory usage under load
- **Thread-Safe**: All components are `Send + Sync` for multithreaded operation

## Quick Start

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
