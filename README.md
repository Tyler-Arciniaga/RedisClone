# Redis Clone (Go)

A Redis-like in-memory data store implemented in Go.  
This project intentionally diverges from Redis’s architecture in one important way:

> **This implementation is multi-threaded**, whereas Redis itself is famously single-threaded.

That trade-off is discussed below.

---

## Architecture & Design Choices

### 🧵 Multi-Threaded Command Execution

Redis executes commands on a single thread to avoid locking altogether.  
This clone instead uses a **multi-threaded design with locks**, favoring throughput and parallelism over architectural simplicity.

**Why?**
- Parallel I/O and command parsing
- Ability to leverage multiple CPU cores.
- In all honesty, I realized Redis was single threaded after designing the core architecture and decided to shift this project in to a fun comparison rather than a 1-to-1 clone.

**Pros**
- Higher read throughput under concurrent load
- I/O and parsing aren’t bottlenecked by a single execution loop (though I think more recent versions of Redis actually address this)
- Go’s goroutines keep thread management lightweight

**Cons**
- Locking overhead on shared data structures
- Larger surface area for concurrency bugs (deadlocks, starvation, race conditions ... I've faced them all)
- In practice, slower than Redis for many workloads due to coordination costs

---

### 🔐 Locking Strategy

- Coarse-grained locks around shared state
- Simple and correct before “clever”
- Optimized for clarity and safety over micro-optimizations

While goroutines are lighter than OS threads, synchronization costs still dominate performance, a key reason Redis’s single-threaded model works so well.

---

### 🖥️ Custom Data Structure Implementation

- **List queue with back-pointer design**
  - Efficient push/pop from both ends
  - Internal structure mirrors Redis-style list semantics

---

## Future Work

- Finer-grained locking (per-key or per-data-type instead of global store locks)
- Benchmark comparisons against real Redis server
- Continued expansion of supported Redis commands and data types