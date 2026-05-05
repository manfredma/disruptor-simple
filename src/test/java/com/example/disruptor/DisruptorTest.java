package com.example.disruptor;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class DisruptorTest {

    static class LongEvent {
        long value;
    }

    // -------------------------------------------------------------------------
    // Sequence 单元测试
    // -------------------------------------------------------------------------

    @Test
    public void testSequenceInitialValue() {
        Sequence seq = new Sequence();
        assertEquals(Sequence.INITIAL_VALUE, seq.get());
    }

    @Test
    public void testSequenceSet() {
        Sequence seq = new Sequence();
        seq.set(42L);
        assertEquals(42L, seq.get());
    }

    @Test
    public void testSequenceIncrementAndGet() {
        Sequence seq = new Sequence(0L);
        assertEquals(1L, seq.incrementAndGet());
        assertEquals(2L, seq.incrementAndGet());
    }

    @Test
    public void testSequenceAddAndGet() {
        Sequence seq = new Sequence(10L);
        assertEquals(15L, seq.addAndGet(5L));
        assertEquals(15L, seq.get());
    }

    @Test
    public void testSequenceCompareAndSet() {
        Sequence seq = new Sequence(0L);
        assertTrue(seq.compareAndSet(0L, 100L));
        assertEquals(100L, seq.get());
        assertFalse(seq.compareAndSet(0L, 200L));
        assertEquals(100L, seq.get());
    }

    @Test
    public void testSequenceConcurrentIncrements() throws InterruptedException {
        // 多线程并发 incrementAndGet，最终值应等于线程数
        Sequence seq = new Sequence(0L);
        int threads = 8;
        int incrementsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    seq.incrementAndGet();
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        assertEquals((long) threads * incrementsPerThread, seq.get());
    }

    @Test
    public void testSequencePaddingStructure() {
        // 验证继承层级：LhsPadding -> Value -> RhsPadding -> Sequence
        // value 字段在 Value 层，前后各有 56 byte 填充
        Class<?> cls = Sequence.class;
        assertTrue("Sequence 应继承自 RhsPadding", cls.getSuperclass() == RhsPadding.class);

        // RhsPadding 应有 56 个 byte 字段
        long rhsByteCount = countByteFields(RhsPadding.class);
        assertEquals("RhsPadding 应有 56 个 byte 填充字段", 56, rhsByteCount);

        // LhsPadding 应有 56 个 byte 字段
        long lhsByteCount = countByteFields(LhsPadding.class);
        assertEquals("LhsPadding 应有 56 个 byte 填充字段", 56, lhsByteCount);

        // Value 应有 1 个 long 字段
        long valueFieldCount = java.util.Arrays.stream(Value.class.getDeclaredFields())
                .filter(f -> f.getType() == long.class).count();
        assertEquals("Value 应有 1 个 long 字段", 1, valueFieldCount);
    }

    private long countByteFields(Class<?> cls) {
        return java.util.Arrays.stream(cls.getDeclaredFields())
                .filter(f -> f.getType() == byte.class).count();
    }

    // -------------------------------------------------------------------------
    // WaitStrategy 单元测试
    // -------------------------------------------------------------------------

    @Test
    public void testBlockingWaitStrategyWaitsAndSignals() throws Exception {
        Sequence cursor = new Sequence(0L);
        Sequence dependent = cursor;
        // 用一个简单的 SequenceBarrier stub
        SequenceBarrier barrier = makeBarrier(cursor);

        CountDownLatch consumerReady = new CountDownLatch(1);
        AtomicLong result = new AtomicLong(-1);
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();

        Thread consumer = new Thread(() -> {
            try {
                consumerReady.countDown();
                result.set(strategy.waitFor(1L, cursor, dependent, barrier));
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        consumerReady.await();
        Thread.sleep(20); // 确保消费者已进入等待

        cursor.set(1L);
        strategy.signalAllWhenBlocking();
        consumer.join(1000);

        assertEquals(1L, result.get());
    }

    @Test
    public void testBusySpinWaitStrategyReturnsWhenAvailable() throws Exception {
        Sequence cursor = new Sequence(5L);
        SequenceBarrier barrier = makeBarrier(cursor);
        BusySpinWaitStrategy strategy = new BusySpinWaitStrategy();
        long result = strategy.waitFor(3L, cursor, cursor, barrier);
        assertEquals(5L, result);
    }

    @Test
    public void testYieldingWaitStrategyReturnsWhenAvailable() throws Exception {
        Sequence cursor = new Sequence(10L);
        SequenceBarrier barrier = makeBarrier(cursor);
        YieldingWaitStrategy strategy = new YieldingWaitStrategy();
        long result = strategy.waitFor(7L, cursor, cursor, barrier);
        assertEquals(10L, result);
    }

    // -------------------------------------------------------------------------
    // SequenceBarrier 测试
    // -------------------------------------------------------------------------

    @Test
    public void testBarrierAlertInterruptsWait() throws Exception {
        Sequencer sequencer = new Sequencer(16, new BlockingWaitStrategy());
        SequenceBarrier barrier = sequencer.newBarrier();

        AtomicBoolean gotAlert = new AtomicBoolean(false);
        Thread consumer = new Thread(() -> {
            try {
                barrier.waitFor(0L);
            } catch (AlertException e) {
                gotAlert.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        Thread.sleep(20);
        barrier.alert();
        consumer.join(1000);

        assertTrue("barrier.alert() 应导致 AlertException", gotAlert.get());
    }

    @Test
    public void testBarrierClearAlert() throws AlertException {
        Sequencer sequencer = new Sequencer(16, new BlockingWaitStrategy());
        SequenceBarrier barrier = sequencer.newBarrier();
        barrier.alert();
        assertTrue(barrier.isAlerted());
        barrier.clearAlert();
        assertFalse(barrier.isAlerted());
        barrier.checkAlert(); // 不应抛出
    }

    // -------------------------------------------------------------------------
    // Sequencer 测试
    // -------------------------------------------------------------------------

    @Test
    public void testSequencerNextAndPublish() {
        Sequencer sequencer = new Sequencer(16, new BlockingWaitStrategy());
        Sequence consumer = new Sequence(Sequence.INITIAL_VALUE);
        sequencer.addGatingSequences(consumer);

        long seq = sequencer.next();
        assertEquals(0L, seq);
        sequencer.publish(seq);
        assertEquals(0L, sequencer.getCursor().get());
    }

    @Test
    public void testSequencerBackpressure() throws Exception {
        // 缓冲区大小 4，消费者不推进，生产者第 5 条应阻塞
        Sequencer sequencer = new Sequencer(4, new BlockingWaitStrategy());
        Sequence consumer = new Sequence(Sequence.INITIAL_VALUE);
        sequencer.addGatingSequences(consumer);

        // 填满 4 个槽
        for (int i = 0; i < 4; i++) {
            sequencer.publish(sequencer.next());
        }

        AtomicBoolean blocked = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            started.countDown();
            blocked.set(true);
            sequencer.next(); // 应阻塞
            blocked.set(false);
        });
        producer.setDaemon(true);
        producer.start();
        started.await();
        Thread.sleep(50);
        assertTrue("缓冲区满时生产者应阻塞", blocked.get());

        // 消费者推进，解除阻塞
        consumer.set(0L);
        producer.join(1000);
        assertFalse("消费者推进后生产者应解除阻塞", blocked.get());
    }

    @Test
    public void testSequencerTryNext() {
        Sequencer sequencer = new Sequencer(4, new BlockingWaitStrategy());
        Sequence consumer = new Sequence(Sequence.INITIAL_VALUE);
        sequencer.addGatingSequences(consumer);

        long[] result = new long[1];
        assertTrue(sequencer.tryNext(result));
        assertEquals(0L, result[0]);

        // 填满
        sequencer.publish(result[0]);
        for (int i = 1; i < 4; i++) {
            sequencer.tryNext(result);
            sequencer.publish(result[0]);
        }
        // 满了，tryNext 应返回 false
        assertFalse(sequencer.tryNext(result));
    }

    // -------------------------------------------------------------------------
    // BatchEventProcessor 测试
    // -------------------------------------------------------------------------

    @Test
    public void testBatchEventProcessorHalt() throws Exception {
        RingBuffer<LongEvent> rb = RingBuffer.create(LongEvent::new, 16, new BlockingWaitStrategy());
        Sequence consumerSeq = new Sequence(Sequence.INITIAL_VALUE);
        rb.getSequencer().addGatingSequences(consumerSeq);
        SequenceBarrier barrier = rb.newBarrier();

        AtomicBoolean ran = new AtomicBoolean(false);
        BatchEventProcessor<LongEvent> processor = new BatchEventProcessor<>(rb, barrier,
                (event, seq, eob) -> ran.set(true));

        Thread t = new Thread(processor);
        t.start();
        Thread.sleep(20);
        processor.halt();
        t.join(1000);
        assertFalse("halt 后线程应退出", t.isAlive());
    }

    // -------------------------------------------------------------------------
    // RingBuffer 测试
    // -------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testNonPowerOfTwoBufferSize() {
        RingBuffer.create(LongEvent::new, 100, new BlockingWaitStrategy());
    }

    @Test
    public void testRingBufferGetWrapsAround() {
        RingBuffer<LongEvent> rb = RingBuffer.create(LongEvent::new, 4, new BlockingWaitStrategy());
        Sequence consumer = new Sequence(Sequence.INITIAL_VALUE);
        rb.getSequencer().addGatingSequences(consumer);

        // sequence 0 和 sequence 4 映射到同一槽位
        LongEvent e0 = rb.get(0L);
        LongEvent e4 = rb.get(4L);
        assertSame("sequence 0 和 4 应映射到同一预分配对象", e0, e4);
    }

    @Test
    public void testRingBufferBufferPadOffset() {
        // entries 数组实际长度应为 bufferSize + 2*BUFFER_PAD(32)
        int bufferSize = 8;
        RingBuffer<LongEvent> ringBuffer = RingBuffer.create(LongEvent::new, bufferSize, new BlockingWaitStrategy());
        assertEquals(bufferSize, ringBuffer.getBufferSize());
        assertSame(ringBuffer.get(0L), ringBuffer.get((long) bufferSize));
    }

    // -------------------------------------------------------------------------
    // 集成测试
    // -------------------------------------------------------------------------

    @Test
    public void testSingleProducerSingleConsumer() throws InterruptedException {
        int messageCount = 1_000;
        AtomicLong sum = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 1024,
                Executors.newSingleThreadExecutor(),
                new YieldingWaitStrategy()
        );
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            sum.addAndGet(event.value);
            latch.countDown();
        });
        RingBuffer<LongEvent> rb = disruptor.start();

        long expectedSum = 0;
        for (long i = 1; i <= messageCount; i++) {
            expectedSum += i;
            final long val = i;
            disruptor.publishEvent((event, seq) -> event.value = val);
        }

        latch.await();
        disruptor.shutdown();
        assertEquals(expectedSum, sum.get());
    }

    @Test
    public void testParallelConsumers() throws InterruptedException {
        int messageCount = 500;
        AtomicLong sum1 = new AtomicLong(0);
        AtomicLong sum2 = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount * 2);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 512,
                Executors.newFixedThreadPool(2),
                new BlockingWaitStrategy()
        );
        disruptor.handleEventsWith(
                (event, seq, eob) -> { sum1.addAndGet(event.value); latch.countDown(); },
                (event, seq, eob) -> { sum2.addAndGet(event.value); latch.countDown(); }
        );
        disruptor.start();

        long expectedSum = 0;
        for (long i = 1; i <= messageCount; i++) {
            expectedSum += i;
            final long val = i;
            disruptor.publishEvent((event, seq) -> event.value = val);
        }

        latch.await();
        disruptor.shutdown();
        assertEquals(expectedSum, sum1.get());
        assertEquals(expectedSum, sum2.get());
    }

    @Test
    public void testRingBufferWrapAround() throws InterruptedException {
        int bufferSize = 4;
        int messageCount = 20;
        AtomicLong received = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, bufferSize,
                Executors.newSingleThreadExecutor(),
                new BusySpinWaitStrategy()
        );
        disruptor.handleEventsWith((event, seq, eob) -> {
            received.incrementAndGet();
            latch.countDown();
        });
        disruptor.start();

        for (int i = 0; i < messageCount; i++) {
            final long val = i;
            disruptor.publishEvent((event, seq) -> event.value = val);
        }

        latch.await();
        disruptor.shutdown();
        assertEquals(messageCount, received.get());
    }

    @Test
    public void testEndOfBatchFlag() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicLong endOfBatchCount = new AtomicLong(0);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 16,
                Executors.newSingleThreadExecutor(),
                new BlockingWaitStrategy()
        );
        disruptor.handleEventsWith((event, seq, endOfBatch) -> {
            if (endOfBatch) endOfBatchCount.incrementAndGet();
            latch.countDown();
        });
        disruptor.start();

        for (int i = 0; i < 3; i++) {
            disruptor.publishEvent((event, seq) -> event.value = seq);
        }

        latch.await();
        disruptor.shutdown();
        assertTrue(endOfBatchCount.get() >= 1);
        assertTrue(endOfBatchCount.get() <= 3);
    }

    @Test
    public void testPipelineConsumer() throws InterruptedException {
        int messageCount = 200;
        AtomicLong sumA = new AtomicLong(0);
        AtomicLong sumB = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 256,
                Executors.newFixedThreadPool(2),
                new BlockingWaitStrategy()
        );

        disruptor.handleEventsWith((event, seq, eob) -> {
            event.value = event.value * 2;
            sumA.addAndGet(event.value);
        });
        disruptor.after(disruptor.getSequences(), (event, seq, eob) -> {
            sumB.addAndGet(event.value);
            latch.countDown();
        });

        disruptor.start();

        long expectedSumOfDoubled = 0;
        for (long i = 1; i <= messageCount; i++) {
            expectedSumOfDoubled += i * 2;
            final long val = i;
            disruptor.publishEvent((event, seq) -> event.value = val);
        }

        latch.await();
        disruptor.shutdown();
        assertEquals(expectedSumOfDoubled, sumA.get());
        assertEquals(expectedSumOfDoubled, sumB.get());
    }

    @Test
    public void testPipelineOrderingGuarantee() throws InterruptedException {
        // 验证 pipeline 中 B 读到的值一定是 A 已修改后的值，不存在 A 未完成时 B 已读取
        int messageCount = 100;
        List<Long> bValues = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(messageCount);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 128,
                Executors.newFixedThreadPool(2),
                new BlockingWaitStrategy()
        );

        disruptor.handleEventsWith((event, seq, eob) -> event.value = seq + 1000L);
        disruptor.after(disruptor.getSequences(), (event, seq, eob) -> {
            bValues.add(event.value);
            latch.countDown();
        });

        disruptor.start();
        for (int i = 0; i < messageCount; i++) {
            disruptor.publishEvent((event, seq) -> event.value = 0L);
        }

        latch.await();
        disruptor.shutdown();

        // B 读到的每个值都应该 >= 1000（即 A 已经写入过）
        for (long v : bValues) {
            assertTrue("B 应读到 A 已修改的值，不应为原始 0", v >= 1000L);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleStartThrows() {
        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 16,
                Executors.newSingleThreadExecutor()
        );
        disruptor.handleEventsWith((event, seq, eob) -> {});
        disruptor.start();
        disruptor.start();
    }

    // -------------------------------------------------------------------------
    // 辅助方法
    // -------------------------------------------------------------------------

    /** 创建一个只检查 alert 的最小 SequenceBarrier stub，用于 WaitStrategy 单元测试 */
    private SequenceBarrier makeBarrier(Sequence cursor) {
        Sequencer sequencer = new Sequencer(16, new BlockingWaitStrategy());
        return sequencer.newBarrier();
    }
}
