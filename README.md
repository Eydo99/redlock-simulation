# LAB 4: Redlock Distributed Locking Simulation

A Python simulation of the **Redlock algorithm** — a distributed mutual exclusion protocol built on top of multiple independent Redis nodes. Demonstrates how distributed locks work across 5 Redis instances using Docker.

---

## What is Redlock?

Redlock is a distributed locking algorithm proposed by Redis. It acquires a lock across `N` independent Redis nodes and considers the lock valid only if it succeeds on a **quorum** (majority: `N/2 + 1`) of nodes within the lock's TTL. This ensures safety even if some nodes fail.

---

## Architecture

```
5 Client Processes
       ↓
  Redlock Algorithm
       ↓
5 Redis Nodes (Docker containers)
  ├── redis-node-1 :63791
  ├── redis-node-2 :63792
  ├── redis-node-3 :63793
  ├── redis-node-4 :63794
  └── redis-node-5 :63795
```

---

## Files

| File | Description |
|------|-------------|
| `redlock_simulation.py` | Main simulation — Redlock implementation + client processes |
| `docker-compose.yml` | Spins up 5 independent Redis nodes |
| `requirements.txt` | Python dependencies (`redis==5.2.1`) |
| `shared_counter.json` | Shared resource — incremented inside the critical section |
| `redlock.log` | Execution log showing lock acquire/release events |

---

## Setup & Run

### 1. Start Redis nodes
```bash
docker-compose up -d
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the simulation
```bash
python3 redlock_simulation.py
```

### 4. Stop Redis nodes
```bash
docker-compose down
```

---

## How It Works

### Lock Acquisition
1. Generate a unique `lock_id` (UUID)
2. Attempt `SET resource lock_id NX PX ttl` on all 5 nodes
3. If quorum (≥3) nodes succeed and elapsed time < TTL → lock is valid
4. Otherwise → release any partial locks and retry

### Lock Release
Uses a Lua script for atomic check-and-delete:
```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
```
This ensures only the lock owner can release it.

### Critical Section
Each client:
1. Acquires the distributed lock
2. Reads, increments, and writes a shared JSON counter
3. Releases the lock

---

## Simulation Details

- **5 client processes** run concurrently via `multiprocessing`
- Clients have **staggered start delays** to simulate realistic contention
- Each client retries every 1 second until it acquires the lock
- The shared counter should equal `5` at the end if mutual exclusion is working correctly

---

## Expected Output

```
All clients finished. Final counter value: 5
```

Each client increments the counter exactly once inside the critical section, with no race conditions.

---

## Key Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Distributed mutual exclusion | Redlock across 5 Redis nodes |
| Quorum-based consensus | Lock valid only if ≥3 nodes agree |
| Atomic operations | Lua script for safe lock release |
| Fault tolerance | Unreachable nodes are skipped, quorum still works |
| Process-level concurrency | Python `multiprocessing` for true parallelism |
