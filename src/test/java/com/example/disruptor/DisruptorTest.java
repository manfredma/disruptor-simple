package com.example.disruptor;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class DisruptorTest {

    // 测试用事件类
    static class LongEvent {
        long value;
    }

    // -------------------------------------------------------------------------
    // 基础功能测试
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
        // 两个并行消费者各自收到全量事件
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
        // RingBuffer 大小 4，发送 20 条消息，验证槽位复用正确
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

    // -------------------------------------------------------------------------
    // Sequence 伪共享消除验证
    // -------------------------------------------------------------------------

    @Test
    public void testSequencePaddingSize() throws Exception {
        // Sequence 对象应包含足够的填充字段使 value 独占缓存行
        java.lang.reflect.Field[] fields = Sequence.class.getDeclaredFields();
        int longFieldCount = 0;
        for (java.lang.reflect.Field f : fields) {
            if (f.getType() == long.class) longFieldCount++;
        }
        // p1-p7 + value + p9-p15 = 15 个 long 字段
        assertTrue("Sequence 应有足够的填充字段消除伪共享", longFieldCount >= 9);
    }

    // -------------------------------------------------------------------------
    // RingBuffer 大小校验
    // -------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testNonPowerOfTwoBufferSize() {
        RingBuffer.create(LongEvent::new, 100, new BlockingWaitStrategy());
    }

    // -------------------------------------------------------------------------
    // 批处理验证：endOfBatch 应在正确位置为 true
    // -------------------------------------------------------------------------

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

        // 发布 3 条消息
        for (int i = 0; i < 3; i++) {
            disruptor.publishEvent((event, seq) -> event.value = seq);
        }

        latch.await();
        disruptor.shutdown();

        // 至少有 1 次 endOfBatch=true（最后一条），且不超过消息总数
        assertTrue(endOfBatchCount.get() >= 1);
        assertTrue(endOfBatchCount.get() <= 3);
    }

    // -------------------------------------------------------------------------
    // Pipeline 测试：after() 链式消费 A -> B
    // -------------------------------------------------------------------------

    @Test
    public void testPipelineConsumer() throws InterruptedException {
        int messageCount = 200;
        // handlerA 先处理，把 value 乘以 2；handlerB 后处理，验证 A 已处理完
        AtomicLong sumA = new AtomicLong(0);
        AtomicLong sumB = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 256,
                Executors.newFixedThreadPool(2),
                new BlockingWaitStrategy()
        );

        // A 先消费
        disruptor.handleEventsWith((event, seq, eob) -> {
            event.value = event.value * 2;  // A 改写事件值
            sumA.addAndGet(event.value);
        });

        // B 依赖 A，读到的 value 必须已被 A 改写
        disruptor.after(disruptor.getSequences(), (event, seq, eob) -> {
            sumB.addAndGet(event.value);    // 读 A 写完的值
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

        // A 和 B 的累加值应相同（B 读到的是 A 已翻倍的值）
        assertEquals(expectedSumOfDoubled, sumA.get());
        assertEquals(expectedSumOfDoubled, sumB.get());
    }

    // -------------------------------------------------------------------------
    // 重复启动应抛出异常
    // -------------------------------------------------------------------------

    @Test(expected = IllegalStateException.class)
    public void testDoubleStartThrows() {
        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new, 16,
                Executors.newSingleThreadExecutor()
        );
        disruptor.handleEventsWith((event, seq, eob) -> {});
        disruptor.start();
        disruptor.start(); // 应抛出 IllegalStateException
    }
}
