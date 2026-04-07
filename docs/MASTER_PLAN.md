# zdze: Master Control Plan (System Specification)

This document is the **Comprehensive Technical Specification** and **Operational Blueprint** for `zdze`, a high-performance, lightweight Change Data Capture (CDC) engine written in Zig 0.15.x. It defines the "Source of Truth" for all system components, logic, and implementation standards.

---

## 1. System Architecture & Design Philosophy

`zdze` is built for **mission-critical synchronization** where data loss is unacceptable and performance is paramount.

### 1.1 Core Principles
- **Predictable Latency**: Single-threaded, non-blocking event loop prevents context-switching overhead and locking bottlenecks.
- **Zero-Guess Memory**: All memory is explicitly allocated. No global state. Components use `std.heap.ArenaAllocator` for per-message/per-transaction cycles to eliminate fragmentation and leaks.
- **Resilient Connectivity**: Built-in state machine for automatic recovery with exponential backoff.
- **Agnostic Dispatch**: Decoupled source and sink implementations using VTable-based polymorphism (Type Erasure).

### 1.2 Pipeline Execution Flow
1. **Startup**: Load Config -> Init GPA -> Init Logger.
2. **Infrastructure**: Resolve Source/Sink drivers -> Create VTables.
3. **Replication Session**:
    - **Handshake**: Negotiate Logical Replication mode; validate replication slot.
    - **Replication Loop**:
        - Read `CopyData` packet.
        - **Decode**: Parse `pgoutput` binary message.
        - **Enrich**: Map OIDs to cached `Relation` metadata.
        - **Dispatch**: Sink `emit()`.
        - **Checkpoint**: Log LSN to disk and Ack back to database.
4. **Shutdown**: Signal received -> Drain Sink -> Save final LSN -> Close Socket -> Deinit.

---

## 2. PostgreSQL Logical Replication Specification

`zdze` implements the **PostgreSQL Streaming Replication Protocol** over raw TCP.

### 2.1 The pgoutput Decoder (Binary Mode)
We prioritize speed by parsing messages in their native binary format. The decoder handles these critical message types:

| Msg ID | Name | Role in CDC | Fields |
| :--- | :--- | :--- | :--- |
| **'B'** | Begin | Transaction Start | Final LSN (u64), Timestamp (u64), XID (u32) |
| **'C'** | Commit | Transaction End | Commit LSN (u64), End LSN (u64), Timestamp (u64) |
| **'R'** | Relation | Schema Mapping | RelID (u32), Schema/Name (string), Column Names & Types |
| **'I'** | Insert | Row Creation | RelID (u32), New Tuple (Value sequence) |
| **'U'** | Update | Row Modification | RelID (u32), Old/Key Tuple (Optional), New Tuple |
| **'D'** | Delete | Row Removal | RelID (u32), Old/Key Tuple |

### 2.2 Table Metadata Management
- **Relation Cache**: `zdze` maintains a `std.AutoHashMap(u32, RelationInfo)` to store table schemas.
- **Schema Evolution**: Relation messages are re-sent by Postgres if a table is altered. `zdze` must update the cache immediately to remain consistent.

### 2.3 Data Type Mapping (OID to Value)
`zdze` maps Postgres internal types to a unified `Value` tagged union:
- `Integer/BigInt` -> `i64`
- `Float/Double` -> `f64`
- `Text/Varchar/JSON/JSONB` -> `[]const u8`
- `Boolean` -> `bool`
- `Timestamp/Date` -> `i64` (Microseconds from epoch)
- `Null` -> `void`

---

## 3. Reliability, State & Durability

### 3.1 Exactly-Once Semantics (Target: At-Least-Once)
CDC systems must ensure that no database record is missed. `zdze` uses a **Pull & Ack** model:
1. Receive WAL data.
2. Emit to Sink.
3. Sink confirms write (Flush).
4. **Checkpoint**: Update local LSN state.
5. **Acknowledge**: Send `StandbyStatusUpdate` ('r') to Postgres to advance the replication slot.

### 3.2 State Persistence (The .zdze directory)
The engine maintains a state file:
- **Location**: `./state.bin` or `config.state_path`.
- **Atomic Writing Strategy**: 
    1. Write new state to `state.tmp`.
    2. `fsync(state.tmp)` to ensure disk persistence.
    3. `rename(state.tmp, state.bin)` (POSIX atomic operation).

### 3.3 Error Recovery State Machine
- **Recoverable**: `SocketTimeout`, `ConnectionReset`, `DatabaseRestarting`.
    - **Action**: Enter `RetryWait` state -> Exponential Backoff (base 1s, max 300s).
- **Critical**: `AuthFailed`, `SlotDropped`, `SchemaIncompatible`.
    - **Action**: Log FATAL error -> Stop Engine (preventing data corruption).

---

## 4. Operational Excellence & Observability

### 4.1 Metrics & Monitoring
The engine exposes (or logs) high-frequency metrics:
- **Replication Lag**: `CurrentServerLSN - LastAckLSN` (Indicates how far behind we are).
- **Throughput**: `Events/sec` and `Bytes/sec`.
- **Memory Usage**: Resident Set Size (RSS) monitoring to detect leaks.

### 4.2 Logging Standard
- **Level**: `INFO` for normal operation, `DEBUG` for protocol tracing, `ERROR` for issues.
- **Format**: Structured Logging (JSON) to facilitate ingestion into ELK/Promtail.

---

## 5. Master Implementation Roadmap (Granular)

### [CURRENT] Phase 2: Refined Decoding & Protocol Compliance
- [ ] Implement response to **Primary Keepalive (k)** messages.
- [ ] Implement full **Update/Delete** logic in `pgoutput`.
- [ ] Handle **Multi-byte UTF-8** strings in column names and values.

### Phase 3: Durability Foundation
- [ ] Implement the `Persistence` module with Atomic Writes.
- [ ] Create the **State Management Loop** (Check-pointing).
- [ ] Implement **Auto-reconnect logic** with configurable backoff.

### Phase 4: Production Readiness
- [ ] **Kafka Sink Driver**: Integrating with librdkafka or a native Zig producer.
- [ ] **Config Parser**: YAML/JSON based configuration for users, databases, and filters.
- [ ] **Filter Engine**: Ability to include/exclude specific tables or schemas.
