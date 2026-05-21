# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

- Build: `cargo build`
- Run all tests: `cargo test`
- Run a single test: `cargo test test_name` (e.g. `cargo test test_roundtrip`)
- Run tests in a module: `cargo test protocol::packet::tests`
- Run with logging: `RUST_LOG=debug cargo run`
- No lint or formatting CI configured yet

### Running the three modes

```sh
# Signaling server (needs public IP)
cargo run -- server --listen 0.0.0.0:7788

# Host — exposes a local TCP service
cargo run -- host --server <signaling-ip>:7788 --target <host:port>

# Client — forwards localhost to the tunnel
cargo run -- client --server <signaling-ip>:7788 --room <room-id> --local-port <port>
```

## Architecture

**ptwop** — single Rust binary with three modes: **Server** (signaling relay, needs public IP), **Host** (service provider, creates rooms), **Client** (connects through signaling to host).

### Data flow

```
App → localhost:mapped_port → Client listener → [UDP p2p tunnel] → Host → target TCP service
```

### Multi-client host model

A single host UDP socket is shared across all clients via a `PacketDemux` (`@file:src/host/demux.rs`) that dispatches inbound packets to registered handlers by source address. Each client gets its own `peer_handler` task (`@file:src/host/mod.rs:147`) with an independent `StreamManager` and `TcpConnectionManager`. A `CancellationToken` tree handles graceful shutdown — per-peer cancellation on `PeerLeft`, root cancellation on server disconnect or Ctrl-C.

### Module layout

| Module | Files | Role |
|--------|-------|------|
| `config` | `@file:src/config.rs` | CLI parser (clap subcommands for server/host/client) |
| `signal` | `@file:src/signal.rs` | TCP JSON signaling protocol (newline-delimited, serde tagged enum) |
| `protocol::packet` | `@file:src/protocol/packet.rs` | Binary UDP packet format: 4B magic `PTP\0`, 1B flags, 2B stream_id, 2B seq_num, 2B ack_num, payload |
| `protocol::stream` | `@file:src/protocol/stream.rs` | Per-stream state machine (Init → SynSent → Established → Closing → Closed), retransmission with exponential backoff (200ms–3s, max 5), keepalive pings |
| `punch` | `@file:src/punch.rs` | UDP hole punching — both sides send `FLAG_PING` probes at 500ms intervals, 10s timeout |
| `server` | `@file:src/server/mod.rs`, `@file:src/server/room.rs` | Signaling server: room create/join/leave, peer-to-peer routing via mpsc channels, cleanup on disconnect |
| `host` | `@file:src/host/mod.rs`, `@file:src/host/demux.rs`, `@file:src/host/mapper.rs` | Host orchestrator (create room, wait for clients, punch per-client, main event loop). `PacketDemux` dispatches UDP packets by peer address; `TcpConnectionManager` manages per-stream TCP connections to the target service |
| `client` | `@file:src/client/mod.rs`, `@file:src/client/forwarder.rs` | Client orchestrator (join room, punch, main event loop). `listen_and_forward` accepts local TCP connections and sends SYN/Data/FIN over UDP |
| `crypto` | `@file:src/crypto/mod.rs`, `@file:src/crypto/noop.rs` | `Crypto` trait; noop impl currently used |

### Key design decisions

- **Binary packet protocol**: Fixed 11B header + variable payload, flags bitfield (SYN/ACK/DATA/FIN/RST/PING/PONG)
- **Stream multiplexing**: Multiple TCP connections multiplexed over one UDP socket via `stream_id` field; stream_id=0 is the control stream for keepalive
- **Retransmission**: Per-stream unacked packet list with exponential backoff RTO, max 5 retransmits before dropping stream
- **Hole punching**: Simultaneous probe exchange — both sides send pings to each other's reported UDP address; first to receive declares success
- **Signaling**: Plain TCP with JSON-over-TCP (newline delimited), no encryption on the signaling channel
- **Cleanup**: Server tracks peer→room mapping; on disconnect it tears down the room, notifies remaining peers
- **async**: Tokio with mpsc channels for UDP→stream handler demux; each stream gets a TCP reader+writer task

### Deployment

Server must have a public IP and port 7788 (default) reachable. Host and client can be behind NAT. The signaling exchange communicates observed UDP addresses so hole punching has the correct endpoints.

### References

- Implementation plan: `@file:docs/superpowers/plans/2026-05-21-multi-client-host.md`
