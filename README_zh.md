# disruptor-simple

LMAX Disruptor 模式的极简教学实现，基于 Java 8，无外部依赖。目标是保留让 Disruptor 高性能的四个核心机制，并用注释解释每个设计决策背后的**原因**。

[English](README.md)

## 为什么是 Disruptor？

传统阻塞队列（`LinkedBlockingQueue`、`ArrayBlockingQueue`）存在三个性能瓶颈：

- **锁竞争**：生产者与消费者争抢同一把锁
- **GC 压力**：每发布一个元素就 `new` 一个节点对象
- **伪共享**：队列头尾指针落在同一 CPU 缓存行，相互干扰

Disruptor 通过**预分配环形缓冲区**、**无锁序号**和**缓存行填充**一次性解决了这三个问题。

## 四个核心机制

| 机制 | 位置 | 作用 |
|------|------|------|
| 消除伪共享 | `Sequence.java` | `value` 字段前后各填充 7 个 `long`，独占一条 64 字节缓存行 |
| 预分配/零 GC | `RingBuffer.java` | 启动时填满所有槽位；生产者只改写字段，运行期不产生 `new` |
| 2 的幂 + 位掩码 | `RingBuffer.java` | `sequence & indexMask` 替代 `sequence % size`，避免整数除法 |
| 批量消费 | `BatchEventProcessor.java` | 单次 `waitFor` 可返回多个可用序号，一次性消费完再更新进度 |

## 项目结构

```
src/main/java/com/example/disruptor/
├── Sequence.java              # 带缓存行填充的原子序号
├── WaitStrategy.java          # 消费者等待策略接口
├── BusySpinWaitStrategy.java  # 忙等：延迟最低，CPU 100% 占用
├── YieldingWaitStrategy.java  # 自旋 100 次后 Thread.yield()
├── BlockingWaitStrategy.java  # Lock/Condition：CPU 友好
├── AlertException.java        # 预分配的关闭信号异常
├── SequenceBarrier.java       # 消费者屏障（支持 pipeline 链路）
├── Sequencer.java             # 单生产者序号生成器
├── EventFactory.java          # 预分配工厂接口
├── RingBuffer.java            # 环形缓冲区
├── EventHandler.java          # 消费者回调接口
├── EventTranslator.java       # 生产者填充接口
├── BatchEventProcessor.java   # 消费者事件循环（Runnable）
└── Disruptor.java             # 门面类：组装所有组件
```

## 快速上手

**Maven + JDK 8，无需任何额外依赖。**

### 1. 定义事件类

```java
public class OrderEvent {
    public long orderId;
    public double amount;
}
```

### 2. 组装 Disruptor

```java
Executor executor = Executors.newSingleThreadExecutor();

Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new,           // 预分配工厂
    1024,                      // 环形缓冲区大小（必须是 2 的幂）
    executor,
    new BlockingWaitStrategy() // 也可选 YieldingWaitStrategy / BusySpinWaitStrategy
);

disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
    System.out.println("处理订单：" + event.orderId);
});

RingBuffer<OrderEvent> ringBuffer = disruptor.start();
```

### 3. 发布事件

```java
// 方式 A — EventTranslator（推荐，复用预分配对象）
disruptor.publishEvent((event, seq) -> {
    event.orderId = 42L;
    event.amount  = 99.9;
});

// 方式 B — 手动 next/publish（用 try/finally 确保一定发布）
long sequence = ringBuffer.next();
try {
    OrderEvent event = ringBuffer.get(sequence);
    event.orderId = 42L;
    event.amount  = 99.9;
} finally {
    ringBuffer.publish(sequence);
}
```

### 4. 关闭

```java
// shutdown() 只发送关闭信号，不等待未处理事件消费完毕。
// 如需等待消费完成，在 EventHandler 中使用 CountDownLatch。
disruptor.shutdown();
executor.shutdown();
```

## Pipeline（消费者链路）

消费者可以串联，确保 Handler B 只在 Handler A 处理完毕后才看到同一事件：

```
Producer → [Handler A] → [Handler B]
```

```java
Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new, 1024,
    Executors.newFixedThreadPool(2)
);

// 第一步：注册 A
disruptor.handleEventsWith((event, seq, eob) -> {
    event.amount = event.amount * 1.1; // A 对事件做增强处理
});

// 第二步：将 B 注册为 A 的下游
disruptor.after(disruptor.getSequences(), (event, seq, eob) -> {
    System.out.println("增强后的金额：" + event.amount); // 读到 A 写完的值
});

disruptor.start();
```

## 等待策略对比

| 策略 | 延迟 | CPU 占用 | 适用场景 |
|------|------|----------|----------|
| `BusySpinWaitStrategy` | 最低 | 100%（需专属核心） | 对延迟极敏感，线程绑核部署 |
| `YieldingWaitStrategy` | 低 | 中等 | 多消费者共享 CPU 核心 |
| `BlockingWaitStrategy` | 中等 | 极低 | 吞吐量优先，CPU 资源紧张 |

## 构建与测试

```bash
mvn clean test -Dsort.skip=true
```

```
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
```

测试覆盖：单生产者/消费者、并行消费者、环形缓冲区回绕、Pipeline 链路、`endOfBatch` 标志、缓冲区大小校验、重复启动保护。

## 关键设计说明

### 为什么用 `putOrderedLong` 而不是 `volatile` 写？

`Sequence.set()` 使用 `Unsafe.putOrderedLong`（即 `lazySet`，store-store 屏障）。在 x86（TSO 内存模型）上与 `volatile` 写等效。在 ARM/Power 架构上可以省去一个 `StoreLoad` 屏障，降低开销。代价是：在这些架构上，写入的可见性可能有轻微延迟。需要立即可见时，请使用 `setVolatile()`。

### 为什么要预分配所有槽位？

发布时 `new` 对象会产生 GC 压力。预分配后，稳态堆使用量固定，GC 不会因环形缓冲区自身的分配活动而触发。

### 为什么 `bufferSize` 必须是 2 的幂？

`index = sequence & (bufferSize - 1)` 替代 `index = sequence % bufferSize`。位与运算是单条 CPU 指令；整数除法需要硬件除法单元，慢约 20–40 倍。

### 伪共享消除的原理

现代 CPU 以**缓存行**（通常 64 字节）为单位加载/失效数据，而非单个变量。若生产者的 `cursor` 和消费者的 `sequence` 恰好落在同一缓存行，任何一方写入都会导致另一方的缓存行失效，迫使对方重新从内存加载——这就是伪共享。`Sequence` 通过在 `value` 前后各填充 7 个 `long`（56 字节），确保 `value` 独占一条缓存行，彻底消除这种干扰。

## 设计上刻意省略的功能

这是一个教学实现，以下功能有意未包含：

- **多生产者支持**：真实 Disruptor 有 `MultiProducerSequencer`，用 CAS 协调并发生产者
- **异常处理器**：消费者未处理的异常目前会被静默跳过
- **`shutdown(timeout)`**：带超时的优雅排空
- **`@Contended` 注解**：JDK 8 支持 `@sun.misc.Contended` 作为手动填充的替代方案

## 参考资料

- [LMAX Disruptor — GitHub](https://github.com/LMAX-Exchange/disruptor)
- [Disruptor 技术论文](https://lmax-exchange.github.io/disruptor/disruptor.html)
- [Mechanical Sympathy 博客](https://mechanical-sympathy.blogspot.com/)
- [伪共享 — Wikipedia](https://en.wikipedia.org/wiki/False_sharing)

## 许可证

MIT
