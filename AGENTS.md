# AGENTS.md — ptwop (P2P TCP Tunnel Over UDP)

## Build & Verify

```sh
cargo build
cargo test                                    # all tests
cargo test test_name                          # single test by name
cargo test protocol::stream::tests            # tests in a module
RUST_LOG=debug cargo run -- <mode> ...        # verbose logging
```

No rustfmt, clippy, or lint CI configured. The only CI is a release workflow on `v*` tags. Integration tests at `tests/integration_test.rs` are placeholder (empty). No CI runs tests.

## Rust Edition 2024

Package uses `edition = "2024"` in Cargo.toml. This affects some syntax behaviors (e.g., `impl Trait` capture rules, `gen` keyword reserved). Be aware when generating code.

## Architecture Summary

Single Rust binary (`ptwop`) with three modes dispatched from `src/main.rs`:

| Mode | Entry | Role |
|------|-------|------|
| `server` | `src/server/mod.rs` | Signaling relay (public IP required), does not carry user data |
| `host` | `src/host/mod.rs` | Service provider, creates rooms, serves multiple clients |
| `client` | `src/client/mod.rs` | Connects to a room, forwards local TCP to tunnel |

Data flow: `App → localhost:port → Client listener → [UDP P2P tunnel] → Host → target TCP service`

## File-to-Module Mapping

| File | Module | Responsibility |
|------|--------|---------------|
| `src/config.rs` | `config` | CLI arguments (clap derive, subcommands for server/host/client) |
| `src/signal.rs` | `signal` | TCP/JSON signaling protocol (newline-delimited, serde tagged enum) |
| `src/punch.rs` | `punch` | UDP hole punching (500ms probes, 10s timeout) |
| `src/protocol/packet.rs` | `protocol::packet` | Binary packet codec: 31B header (magic+flags+connection_id+stream_id+seq_num+ack_num) + payload |
| `src/protocol/stream.rs` | `protocol::stream` | `StreamManager`: per-stream state machines, retransmission (exponential backoff 200ms–3s, max 5 retries, rapid retransmit after 3 dup ACKs), keepalive (5s interval, up to 3 lost pings) |
| `src/protocol/connection.rs` | `protocol::connection` | Connection-level state tracking, activity timeout |
| `src/protocol/window.rs` | `protocol::window` | `SendWindow`/`ReceiveWindow` — sliding window flow control (64KB default) |
| `src/protocol/congestion.rs` | `protocol::congestion` | TCP-style congestion control (slow start → congestion avoidance) |
| `src/server/mod.rs` | `server` | Signaling server: accepts TCP peers, routes messages by room |
| `src/server/room.rs` | `server::room` | Room lifecycle: create/join/leave/remove, peer→room mapping |
| `src/host/mod.rs` | `host` | Host orchestrator: create room, `peer_handler` per client (hole punch + stream loop) |
| `src/host/demux.rs` | `host::demux` | `PacketDemux`: dispatches inbound UDP by source address to registered mpsc receivers |
| `src/host/mapper.rs` | `host::mapper` | `TcpConnectionManager`: per-stream TCP connections to target, buffering for early packets |
| `src/client/mod.rs` | `client` | Client orchestrator: join room, hole punch, main event loop (packet rx, keepalive, retransmit) |
| `src/client/forwarder.rs` | `client::forwarder` | `listen_and_forward`: accepts local TCP, sends SYN/DATA/FIN over UDP |
| `src/crypto/mod.rs` | `crypto` | `Crypto` trait (encrypt/decrypt/max_overhead) |
| `src/crypto/noop.rs` | `crypto::noop` | `NoopCrypto` — placeholder, no encryption |

## Key Design Decisions

- **Binary packet protocol**: Fixed 31B header (magic `PTP\0`, 1B flags, 8B connection_id, 2B stream_id, 8B seq_num, 8B ack_num) + variable payload. Flag bits: SYN/ACK/DATA/FIN/RST/PING/PONG.
- **Connection ID**: u64, generated from system time. Distinguishes between different client↔host pairs on the same host socket.
- **Stream multiplexing**: stream_id=0 is the control stream (PING/PONG), IDs 1–65535 map to individual TCP connections.
- **Flow control**: 64KB send/receive windows via `window.rs`. Congestion control in `congestion.rs` (slow start, congestion avoidance, MSS=1460).
- **Retransmission**: Per-stream unacked packet list, exponential backoff RTO (200ms base, 3s max), max 5 retransmits. Fast retransmit on 3 duplicate ACKs.
- **Single UDP socket per host**: All clients share one UDP socket. `PacketDemux` dispatches by source address to per-peer mpsc channels.
- **Keepalive**: Every 5s of inactivity, PING sent on control stream. After 3 consecutive lost PINGs, connection declared dead.
- **Signaling**: Plain TCP with JSON-over-TCP (newline delimited). No encryption. Serde tagged enum for message types.
- **Shutdown**: `CancellationToken` tree — root on Ctrl-C, child tokens per peer. `PeerLeft` from server triggers per-peer cancellation.

## Known Issues & WIP

- **Client stream manager bug**: The client's `forwarder.rs` allocates streams via `StreamManager::allocate()` and sends SYN immediately, but there's a race condition where the `stream_mgr` in `client/mod.rs` may not have the stream registered when receiving ACK/DATA (documented in `test_client_stream_manager_bug` in `stream.rs`).
- **Crypto**: `NoopCrypto` only. Trait prepared for Noise protocol or similar replacement.
- **Plans in progress**: `docs/superpowers/plans/2026-05-22-quic-style-reliable-udp.md` documents the ongoing QUIC-style reliable UDP refactoring (u64 seq numbers, windows, congestion control were recently added).
- **`docs/` is gitignored**: Changes to files in `docs/` won't be tracked by git. Do not commit them.

## Testing Structure

Unit tests live inside each source file under `#[cfg(test)] mod tests`. Integration tests are `tests/integration_test.rs` (placeholder). Async tests use `#[tokio::test]`. No test prerequisites (no external services needed).

## CLI Quick Reference

```sh
cargo run -- server --listen 0.0.0.0:7788
cargo run -- host --server <ip>:7788 --target <host:port>          # bare port → 127.0.0.1:<port>
cargo run -- client --server <ip>:7788 --room <id> --local-port <p>
```

The `host` normalizes bare `--target` port numbers to `127.0.0.1:<port>` implicitly.

## Signal Messages (TCP/JSON)

| Direction | Message |
|-----------|---------|
| Host→Server | `create_room` |
| Server→Host | `room_created { room_id, my_addr }` |
| Client→Server | `join_room { room_id }` |
| Server→Client | `room_info { host_addr, my_addr, room_id }` |
| Server→Host | `peer_joined { peer_addr, peer_id, room_id }` / `peer_left { peer_id, room_id }` |
| Host/Client→Server | `p2p_ready { room_id, peer_id }` (relayed to peer) |
| Either→Server | `error { reason }` |

## Derived Facts

- The server binds one TCP listener per run; each peer gets one spawned task with independent mpsc writer channel.
- Host uses `CancellationToken` child trees: one root token per `run()`, one child per `peer_handler`. On `PeerLeft`, the specific child token cancels the right handler.
- `PacketDemux::run()` is a single background task holding the recv loop. It reads raw UDP, decodes packets, and fans out to registered mpsc senders by source address.
- The `connection_id` is set independently by host (nanos timestamp) and client (seconds+nano timestamp) — they don't need to match since both sides track their own connection.
- Sequence numbers are u64 and wrap safely using `wrapping_sub` for circular comparison (`seq_gt_u64`, `seq_lt_u64`).
