# zdze: Testing & Validation Log

This document tracks the verification results of `zdze` components. Every technical milestone must be recorded here with its corresponding validation method and outcome.

---

## 1. Automated Test Summary

| Test Category | Command | Result | Date | Notes |
| :--- | :--- | :---: | :--- | :--- |
| **System Build** | `zig build` | ✅ **PASS** | 2026-04-07 | Compiled with Zig 0.15.2 |
| **DML Decoding** | `zig build test` | ✅ **PASS** | 2026-04-07 | Verified mapping for Insert/Update/Delete |
| **Logic Verification** | `zig build test` | ✅ **PASS** | 2026-04-07 | 0 failures in core/source/sink logic |
| **Keepalive Logic** | Static Check | ✅ **SUCCESS** | 2026-04-07 | StandbyStatusUpdate ('r') implemented |

---

## 2. Integration & Functional Tests

### 2.1 Mock Driver Propagation Test
- **Objective**: Verify that the Engine correctly handles polymorphic sources and sinks.
- **Execution**: `zig build run`
- **Results**: 
  - [x] Engine initialized.
  - [x] MockSource emitted 5 synthetic events.
  - [x] StdoutSink serialized events to valid JSON.
  - [x] Graceful termination on EOF.

### 2.2 PostgreSQL Source Handshake (Static Analysis)
- **Objective**: Ensure the PostgreSQL protocol messages are byte-accurate.
- **Results**:
  - [x] `StartupMessage` contains `replication=database`.
  - [x] `MsgType` mappings are aligned with PG 14/15/16 wire protocol.
  - [x] Correct handling of `AuthenticationOk ('R')` and `ReadyForQuery ('Z')`.

---

## 3. Component Regression Tracking

| Component | Last Validated | Status | Blockers |
| :--- | :--- | :---: | :--- |
| **Master Engine** | 2026-04-07 | Stable | None |
| **MockSource** | 2026-04-07 | Stable | None |
| **PostgresSource** | 2026-04-07 | **Beta** | Requires live PG integration test |
| **StdoutSink** | 2026-04-07 | Stable | None |

---

## 4. Performance Benchmarks

*No formal benchmarks registered yet. Targets:*
- Latency (Source to Sink): < 50ms
- Throughput (Events/sec): > 10,000 EPS

---

> [!NOTE]
> **Manual Verification Step**
> When testing the `PostgresSource` with a live database, ensure the `zdze_pub` publication and the replication slot exist on the server before execution.
