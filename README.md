# ptwop — P2P TCP Tunnel Over UDP

NAT 穿透 TCP 端口映射工具。通过 UDP 打洞建立 P2P 隧道，将远程 TCP 服务映射到本地端口。

典型场景：Minecraft 局域网联机，一方在 NAT 后无法端口映射。

## 三种运行模式

```
┌─────────────────────────────────────────────────────────┐
│  Server         │  Host              │  Client         │
│  (公网，需IP)    │  (提供 TCP 服务)      │  (访问远程服务)   │
└─────────────────────────────────────────────────────────┘
```

### Server — 信令中继

需要公网 IP，**不传输用户数据**，只转发信令：

```sh
cargo run -- server --listen 0.0.0.0:7788
```

### Host — 服务端

在 NAT 后的机器上运行，将本地 TCP 服务暴露给隧道：

```sh
cargo run -- host --server <server-ip>:7788 --target <localhost:port>
# 例如映射本机 Minecraft：
cargo run -- host --server 1.2.3.4:7788 --target 25565
```

Host 创建房间后输出 `room_id`，告知要连接的 Client。

### Client — 客户端

连接到 Host 的房间，将远程服务映射到本地端口：

```sh
cargo run -- client --server <server-ip>:7788 --room <room-id> --local-port 25565
# 之后 localhost:25565 即为远程服务
```

## 数据流

```
用户 App
  └─→ localhost:映射端口
        └─→ Client 本地监听
              └─→ [UDP P2P 隧道] (打洞后直连)
                    └─→ Host
                          └─→ 目标 TCP 服务
```

## 打洞流程

1. Host 连接 Server，创建房间，获取分配的 UDP 端口
2. Client 加入房间，收到 Host 的 UDP 地址
3. 双方同时向对方发送 UDP 探测包
4. NAT "洞"打通后，收到对方包的一方宣告成功
5. 双方切换到 P2P 数据传输

超时 10s 不成功 → 打印错误，Host 继续等待后续 Client

## 协议

### Packet 格式（UDP 层）

```
[4B magic: "PTP\0"] [1B flags] [2B stream_id] [2B seq_num] [2B ack_num] [N payload]
```

| Flag bit | 含义 |
|----------|------|
| SYN | 建立流 |
| ACK | 确认 |
| DATA | 数据 |
| FIN | 关闭流 |
| RST | 重置流 |
| PING/PONG | 保活 |

stream_id=0 保留给控制流（PING/PONG），1~65535 对应各 TCP 连接。

### 信令消息（TCP/JSON）

| 方向 | 消息 |
|------|------|
| Host→Server | `create_room` |
| Server→Host | `room_created { room_id, my_addr }` |
| Client→Server | `join_room { room_id }` |
| Server→Client | `room_info { host_addr, my_addr }` |
| Server→Host | `peer_joined { peer_addr, peer_id }` |
| Server→Host | `peer_left { peer_id }` |
| Host/Client→Server | `p2p_ready { room_id, peer_id }` |
| Server→另一方 | `p2p_ready`（透传） |

## 多客户端支持

Host 可以同时服务多个 Client，每个 Client 有独立的 UDP 隧道和流管理：

- 入站 UDP 包由 `PacketDemux` 按来源地址分发到对应的 handler
- 每个 handler 有独立的 `StreamManager` 和 `TcpConnectionManager`
- Client 断开时 Server 通过 `peer_left` 通知 Host，Host 取消对应 handler

## 模块结构

```
src/
├── main.rs              — 入口，CLI 分发
├── config.rs            — CLI 参数定义
├── signal.rs            — 信令协议 (TCP/JSON，SignalMsg 枚举)
├── punch.rs             — UDP 打洞（来源地址校验）
├── protocol/
│   ├── packet.rs        — Packet 编解码，flags 常量
│   └── stream.rs        — StreamManager（状态机、重传、保活）
├── server/
│   ├── mod.rs           — 信令服务器主循环
│   └── room.rs          — 房间管理（RoomManager）
├── host/
│   ├── mod.rs           — Host 主循环 + peer_handler 任务
│   ├── demux.rs         — PacketDemux（UDP 包按地址分发）
│   └── mapper.rs        — TcpConnectionManager（流→TCP 转发）
├── client/
│   ├── mod.rs           — Client 主循环
│   └── forwarder.rs     — listen_and_forward（本地端口→隧道）
└── crypto/
    ├── mod.rs           — Crypto trait
    └── noop.rs          — 直通实现（未来可替换为 Noise）
```

## 开发

```sh
cargo build
cargo test
cargo test test_roundtrip          # 单个测试
cargo test protocol::packet::tests # 模块内测试
RUST_LOG=debug cargo run -- host ...  # 带日志运行
```

## 设计文档

详细架构说明见 `docs/superpowers/specs/2026-05-21-ptwop-p2p-design.md`