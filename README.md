# disruptor-simple

A minimal, educational implementation of the [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) pattern in Java 8. The goal is to preserve the four core mechanisms that make Disruptor fast, with clear comments explaining **why** each design decision exists.

[中文文档](README_zh.md)

## Why Disruptor?

Traditional blocking queues (`LinkedBlockingQueue`, `ArrayBlockingQueue`) use locks and suffer from:

- **Lock contention** between producer and consumer threads
- **GC pressure** from allocating a new node per element
- **False sharing** when head/tail pointers land on the same CPU cache line

Disruptor eliminates all three by combining a **pre-allocated ring buffer**, **lock-free sequence numbers**, and **CPU cache-line padding**.

## Four Core Mechanisms

| Mechanism | Where | What it does |
|-----------|-------|-------------|
| False sharing elimination | `Sequence.java` | Pads `value` with 7 `long` fields before and after, so it occupies its own 64-byte cache line |
| Pre-allocation / zero GC | `RingBuffer.java` | Allocates all event objects at startup; producers overwrite fields in-place, no `new` at runtime |
| Power-of-2 + bitmask | `RingBuffer.java` | `sequence & indexMask` replaces `sequence % size` — avoids integer division entirely |
| Batch consumption | `BatchEventProcessor.java` | A single `waitFor` may return many available sequences; the processor drains them all before updating its progress counter |

## Project Structure

```
src/main/java/com/example/disruptor/
├── Sequence.java              # Padded atomic sequence number
├── WaitStrategy.java          # Consumer wait strategy interface
├── BusySpinWaitStrategy.java  # Spin-wait: lowest latency, 100% CPU
├── YieldingWaitStrategy.java  # Spin 100x then Thread.yield()
├── BlockingWaitStrategy.java  # Lock/Condition: CPU-friendly
├── AlertException.java        # Pre-allocated shutdown signal exception
├── SequenceBarrier.java       # Consumer barrier (also handles pipelines)
├── Sequencer.java             # Single-producer sequence allocator
├── EventFactory.java          # Pre-allocation factory interface
├── RingBuffer.java            # The ring buffer
├── EventHandler.java          # Consumer callback interface
├── EventTranslator.java       # Producer fill interface
├── BatchEventProcessor.java   # Consumer event loop (runnable)
└── Disruptor.java             # Façade: wires everything together
```

## Quick Start

**Maven / JDK 8 — no external dependencies required.**

### 1. Define an event

```java
public class OrderEvent {
    public long orderId;
    public double amount;
}
```

### 2. Wire up the Disruptor

```java
Executor executor = Executors.newSingleThreadExecutor();

Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new,           // pre-allocation factory
    1024,                      // ring buffer size (must be power of 2)
    executor,
    new BlockingWaitStrategy() // or YieldingWaitStrategy / BusySpinWaitStrategy
);

disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
    System.out.println("Processing order: " + event.orderId);
});

RingBuffer<OrderEvent> ringBuffer = disruptor.start();
```

### 3. Publish events

```java
// Option A — EventTranslator (recommended, reuses pre-allocated objects)
disruptor.publishEvent((event, seq) -> {
    event.orderId = 42L;
    event.amount  = 99.9;
});

// Option B — manual next/publish (use try/finally to guarantee publish)
long sequence = ringBuffer.next();
try {
    OrderEvent event = ringBuffer.get(sequence);
    event.orderId = 42L;
    event.amount  = 99.9;
} finally {
    ringBuffer.publish(sequence);
}
```

### 4. Shutdown

```java
// shutdown() sends the halt signal; it does NOT wait for in-flight events.
// Use a CountDownLatch in your handler to drain before shutting down.
disruptor.shutdown();
executor.shutdown();
```

## Pipeline (Consumer Chains)

Consumers can be chained so that handler B only sees events **after** handler A has finished processing them:

```
Producer → [Handler A] → [Handler B]
```

```java
Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new, 1024,
    Executors.newFixedThreadPool(2)
);

// Step 1: register A
disruptor.handleEventsWith((event, seq, eob) -> {
    event.amount = event.amount * 1.1; // A enriches the event
});

// Step 2: register B as downstream of A
disruptor.after(disruptor.getSequences(), (event, seq, eob) -> {
    System.out.println("Enriched amount: " + event.amount); // sees A's writes
});

disruptor.start();
```

## Wait Strategy Comparison

| Strategy | Latency | CPU usage | Use when |
|----------|---------|-----------|----------|
| `BusySpinWaitStrategy` | Lowest | 100% (dedicated core) | Ultra-low latency with pinned threads |
| `YieldingWaitStrategy` | Low | Moderate | Multiple consumers sharing cores |
| `BlockingWaitStrategy` | Moderate | Minimal | Throughput over latency; resource-constrained |

## Build & Test

```bash
mvn clean test -Dsort.skip=true
```

```
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
```

Test coverage includes: single producer/consumer, parallel consumers, ring-buffer wrap-around, pipeline chains, `endOfBatch` flag, buffer-size validation, and duplicate-start protection.

## Key Design Notes

### Why `putOrderedLong` instead of `volatile` write?

`Sequence.set()` uses `Unsafe.putOrderedLong` (a store-store barrier, also known as `lazySet`). On x86 this is equivalent to a plain volatile write. On ARM/Power architectures it avoids a full `StoreLoad` fence, reducing overhead. The trade-off: visibility of the written value may be slightly delayed on those architectures. `setVolatile()` is available when immediate visibility is required.

### Why pre-allocate all ring buffer slots?

Creating objects at publish time would generate GC pressure. Because all slots are allocated upfront and reused, the steady-state heap footprint is fixed and the GC never sees allocation traffic from the ring buffer itself.

### Why must `bufferSize` be a power of 2?

`index = sequence & (bufferSize - 1)` replaces `index = sequence % bufferSize`. The bitwise AND is a single CPU instruction; integer division requires a hardware divide unit and is ~20–40× slower.

## Limitations (by design)

This is an educational implementation. Features intentionally omitted:

- **Multi-producer support** — the real Disruptor has a `MultiProducerSequencer` that uses CAS to coordinate concurrent producers
- **Exception handler** — unhandled exceptions in consumers are silently skipped
- **`shutdown(timeout)`** — graceful drain with a deadline
- **`@Contended` annotation** — JDK 8 supports `@sun.misc.Contended` as an alternative to manual padding

## References

- [LMAX Disruptor — GitHub](https://github.com/LMAX-Exchange/disruptor)
- [Disruptor Technical Paper (PDF)](https://lmax-exchange.github.io/disruptor/disruptor.html)
- [Mechanical Sympathy Blog](https://mechanical-sympathy.blogspot.com/)
- [False Sharing — Wikipedia](https://en.wikipedia.org/wiki/False_sharing)

## License

MIT
