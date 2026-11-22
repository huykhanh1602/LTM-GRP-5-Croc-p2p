# croc-p2p

Công cụ chia sẻ tệp tin ngang hàng **P2P N-N** mở rộng từ kiến trúc của
[`schollz/croc`](https://github.com/schollz/croc), thực hiện cho môn học
**Lập trình Mạng (INT3304)**.

Mục tiêu: nhiều peer trong cùng một "phòng" (room) có thể chia sẻ và tải song
song nhiều nguồn, với bắt tay an toàn qua relay công khai, mã hóa đầu-cuối và
đục lỗ NAT để truyền trực tiếp.

---

## 1. Kiến trúc tổng quan

Hệ thống chia thành hai mặt phẳng rõ ràng:

| Mặt phẳng         | Vai trò                                                          | Thành phần                                |
| ----------------- | ---------------------------------------------------------------- | ----------------------------------------- |
| **Control Plane** | Signaling, quản lý phòng, xác thực, heartbeat, phát tán metadata | Relay `croc.schollz.com`, `internal/room` |
| **Data Plane**    | Truyền mảnh dữ liệu trực tiếp N-N, fallback relay                | `internal/transport`, `internal/chunker`  |
| **Security**      | PAKE khi vào phòng, khóa AES-256-GCM cho phòng                   | `internal/security`                       |

### 1.1 Control Plane (Signaling & Room Management)

- Peer đầu tiên tạo phòng trở thành **Coordinator**.
- Coordinator xác thực thành viên mới, duy trì `[]PeerInfo` và phát `PEER_LIST`.
- Kiểm tra sống/chết bằng `PING`/`PONG`; peer quá hạn bị đánh dấu `offline`.
- Khi coordinator rời phòng hoặc chết, phát `ELECT_COORDINATOR` và luân chuyển
  vai trò coordinator theo term tăng dần.
- Phát tán danh mục file (`FILE_ANNOUNCE`) và trạng thái mảnh (`BITFIELD_UPDATE`).

### 1.2 Security (Handshake & E2EE)

- Bắt tay **PAKE** (`github.com/schollz/pake/v3`) bằng mật khẩu phòng.
- Sinh **Room AES-256 Key** dùng chung cho mọi thành viên.
- Mỗi mảnh dữ liệu được mã hóa **AES-256-GCM** trước khi truyền P2P.
- Khóa được gắn với `roomID` bằng domain separation để tránh tái sử dụng khóa.

### 1.3 Transport & NAT Traversal (Data Plane)

Thứ tự ưu tiên kết nối:

1. **LAN** — kết nối trực tiếp nếu hai peer cùng mạng nội bộ.
2. **STUN + UDP/QUIC Hole Punching** (`github.com/pion/stun`,
   `github.com/quic-go/quic-go`) — đục lỗ NAT/CGNAT để mở kênh trực tiếp.
3. **Relay Fallback** — nếu mạng chặn cứng P2P, chuyển luồng dữ liệu qua
   `croc relay`.

### 1.4 Chunking & Multi-source Swarm

- Cắt file thành các mảnh cố định **512 KiB**.
- Quản lý trạng thái mảnh bằng Bitfield (`github.com/bits-and-blooms/bitset`).
- Kiểm tra toàn vẹn từng mảnh và toàn file bằng **SHA-256**.
- Tải song song đa nguồn: mỗi mảnh thiếu có thể lấy từ bất kỳ peer nào đang có.

---

## 2. Sơ đồ luồng hoạt động

```text
                 ┌──────────────────────────────────────────────┐
                 │          croc relay (croc.schollz.com)       │
                 │   JOIN / JOIN_ACK / PEER_LIST / PING / PONG  │
                 └───────────────┬──────────────────────────────┘
                                 │ Control Plane (signaling)
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
   ┌────▼─────┐            ┌─────▼────┐             ┌─────▼────┐
   │ Peer A   │            │ Peer B   │             │ Peer C   │
   │Coordinator│           │ member   │             │ member   │
   └────┬─────┘            └─────┬────┘             └─────┬────┘
        │  1. PAKE handshake (room secret)                │
        │◄───────────────────────────────────────────────►│
        │  2. Room AES-256 key derived                    │
        │                                                 │
        │  3. FILE_ANNOUNCE + BITFIELD_UPDATE             │
        │────────────────────────────────────────────────►│
        │                                                 │
        │  4. Data Plane: LAN → STUN/QUIC → Relay         │
        │        ChunkRequest / ChunkData (AES-256-GCM)   │
        │◄═══════════════ direct N-N ════════════════════►│
        │                                                 │
        │  5. SHA-256 verify, SetBit, AssembleFile        │
        └─────────────────────────────────────────────────┘
```

Luồng một lần tải file:

```text
create/join ──► PAKE ──► Room Key ──► PEER_LIST
                                        │
share <file> ──► PrepareFile ──► FILE_ANNOUNCE ──► BITFIELD_UPDATE
                                        │
get <file> ──► resolve FileMeta ──► missing chunks ──► parallel fetch
                                        │                   │
                              SHA-256 verify ◄── ChunkData ─┘
                                        │
                                  SetBit ──► IsComplete ──► AssembleFile
```

---

## 3. Cấu trúc thư mục

```text
croc-p2p/
├── cmd/
│   └── croc-p2p/
│       └── main.go              # CLI: create, join, share, get
├── internal/
│   ├── security/
│   │   └── security.go          # PAKE + AES-256-GCM (Member 2)
│   ├── transport/
│   │   └── nat.go               # LAN/STUN/QUIC hole punching + relay (Member 1)
│   ├── room/
│   │   └── manager.go           # Control plane, heartbeat, election (Member 3)
│   └── chunker/
│       └── swarm.go             # 512 KiB chunks, bitfield, SHA-256, swarm (Member 4)
├── pkg/
│   └── protocol/
│       └── types.go             # Shared JSON messages + provider interfaces
├── .gitignore
├── go.mod
├── go.sum
└── README.md
```

---

## 4. Yêu cầu & cài đặt

- **Go 1.27.4** (phiên bản đang dùng để khởi tạo dự án).
- Git.
- Kết nối Internet để dùng STUN và relay công khai.

```bash
# 1. Clone
git clone https://github.com/huykhanh2141/croc-p2p.git
cd croc-p2p

# 2. Tải phụ thuộc
go mod download

# 3. Build
go build ./...

# 4. (tuỳ chọn) build binary CLI
go build -o croc-p2p ./cmd/croc-p2p
```

### Phụ thuộc cốt lõi

| Thư viện                            | Mục đích                           |
| ----------------------------------- | ---------------------------------- |
| `github.com/schollz/pake/v3`        | Bắt tay PAKE khi vào phòng         |
| `github.com/pion/stun`              | STUN discovery cho NAT traversal   |
| `github.com/quic-go/quic-go`        | UDP/QUIC transport + hole punching |
| `github.com/bits-and-blooms/bitset` | Bitfield trạng thái mảnh           |

---

## 5. Sử dụng CLI

```bash
# Tạo phòng và trở thành coordinator
go run ./cmd/croc-p2p create --room int3304 --secret my-secret

# Tham gia phòng từ máy khác
go run ./cmd/croc-p2p join --room int3304 --secret my-secret

# Chuẩn bị và công bố file (512 KiB/chunk, SHA-256)
go run ./cmd/croc-p2p share ./du-lieu.zip

# Tải file từ swarm
go run ./cmd/croc-p2p get du-lieu.zip
```

> Ghi chú: ở giai đoạn scaffolding, `create`/`join` dùng mô hình thành viên
> trong bộ nhớ và `share`/`get` mới chuẩn bị metadata. Các `TODO` đánh dấu điểm
> nối cho từng module.

---

## 6. Giao thức bản tin dùng chung

Toàn bộ bản tin nằm trong [`pkg/protocol/types.go`](pkg/protocol/types.go) và
dùng một envelope chung là `ControlMessage`:

| Loại                | Ý nghĩa                                     |
| ------------------- | ------------------------------------------- |
| `JOIN`              | Peer yêu cầu vào phòng (kèm PAKE)           |
| `JOIN_ACK`          | Coordinator chấp nhận và trả danh sách peer |
| `PEER_LIST`         | Danh sách thành viên hiện tại               |
| `FILE_ANNOUNCE`     | Công bố `FileMeta` của một file             |
| `BITFIELD_UPDATE`   | Cập nhật trạng thái mảnh của một peer       |
| `PING` / `PONG`     | Kiểm tra sống/chết và đo RTT                |
| `ELECT_COORDINATOR` | Luân chuyển coordinator theo term           |
| `LEAVE`             | Peer rời phòng                              |
| `ERROR`             | Báo lỗi control plane                       |

Các `interface` chuẩn để 4 thành viên implement độc lập:

- `SecurityProvider` — PAKE + mã hóa.
- `TransportProvider` — LAN/STUN/QUIC/relay.
- `RoomManager` — quản lý phòng + heartbeat + election.
- `ChunkManager` — chia mảnh + bitfield + swarm download.

---

## 7. Phân công module cho 4 thành viên

| Thành viên       | Vai trò               | Thư mục phụ trách               | Nhiệm vụ chính                                                          |
| ---------------- | --------------------- | ------------------------------- | ----------------------------------------------------------------------- |
| **Thành viên 1** | Trưởng nhóm / Network | `internal/transport/nat.go`     | LAN discovery, STUN, UDP/QUIC hole punching, relay fallback             |
| **Thành viên 2** | Security              | `internal/security/security.go` | PAKE handshake, sinh Room AES-256 key, AES-256-GCM                      |
| **Thành viên 3** | Control Plane         | `internal/room/manager.go`      | Relay signaling, member list, PING/PONG, coordinator election, UI hooks |
| **Thành viên 4** | Chunking & Swarm      | `internal/chunker/swarm.go`     | Chia 512 KiB, bitfield, SHA-256, tải song song đa nguồn                 |

**Dùng chung:** `pkg/protocol/types.go` và `cmd/croc-p2p/main.go`. Mọi thay đổi
vào file dùng chung cần được trưởng nhóm review để tránh phá vỡ hợp đồng
interface.

### Quy ước làm việc nhóm

- Mỗi thành viên chỉ sửa file trong thư mục phụ trách; tránh sửa `pkg/protocol`.
- Nếu cần bổ sung trường bản tin, tạo PR nhỏ và thông báo cả nhóm.
- Chạy `go build ./...` và `go vet ./...` trước khi commit.
- Commit theo Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`.

---

## 8. Trạng thái hiện tại (scaffolding)

- [x] Khởi tạo Go module `github.com/huykhanh2141/croc-p2p`.
- [x] Định nghĩa đầy đủ bản tin JSON và 4 interface dùng chung.
- [x] Khung 4 module với `TODO` rõ ràng cho từng thành viên.
- [x] CLI cơ bản: `create`, `join`, `share`, `get`.
- [x] Build sạch với `go build ./...`.
- [ ] Triển khai PAKE + AES-256-GCM đầy đủ.
- [ ] Triển khai QUIC hole punching và relay framing.
- [ ] Triển khai đồng bộ phòng qua relay thật.
- [ ] Triển khai swarm scheduler rarest-first + retry.
