package com.example.disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Disruptor 门面类：负责组装 RingBuffer、消费者、线程，并管理生命周期。
 *
 * 典型用法：
 * <pre>
 *   Disruptor<MyEvent> disruptor = new Disruptor<>(MyEvent::new, 1024, executor);
 *   disruptor.handleEventsWith(myHandler);
 *   RingBuffer<MyEvent> rb = disruptor.start();
 *
 *   long seq = rb.next();
 *   try {
 *       rb.get(seq).setValue(...);
 *   } finally {
 *       rb.publish(seq);
 *   }
 *
 *   disruptor.shutdown();
 * </pre>
 */
public class Disruptor<E> {

    private final RingBuffer<E> ringBuffer;
    private final Executor executor;
    private final List<BatchEventProcessor<E>> processors = new ArrayList<>();
    private boolean started = false;

    public Disruptor(EventFactory<E> eventFactory, int ringBufferSize, Executor executor) {
        this(eventFactory, ringBufferSize, executor, new BlockingWaitStrategy());
    }

    public Disruptor(EventFactory<E> eventFactory, int ringBufferSize, Executor executor,
                     WaitStrategy waitStrategy) {
        this.ringBuffer = RingBuffer.create(eventFactory, ringBufferSize, waitStrategy);
        this.executor = executor;
    }

    /**
     * 注册一批并行消费者（彼此独立，每人消费全量事件）。
     */
    @SafeVarargs
    public final Disruptor<E> handleEventsWith(EventHandler<E>... handlers) {
        return createEventProcessors(new Sequence[0], handlers);
    }

    /**
     * 注册下游消费者，依赖 barrierSequences 对应的上游消费者处理完毕后再消费。
     * 用于构建消费者链（Pipeline）。
     */
    @SafeVarargs
    public final Disruptor<E> after(Sequence[] barrierSequences, EventHandler<E>... handlers) {
        return createEventProcessors(barrierSequences, handlers);
    }

    private Disruptor<E> createEventProcessors(Sequence[] barrierSequences, EventHandler<E>[] handlers) {
        SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);
        for (EventHandler<E> handler : handlers) {
            BatchEventProcessor<E> processor =
                    new BatchEventProcessor<>(ringBuffer, barrier, handler);
            processors.add(processor);
            // 将消费者 Sequence 注册到 Sequencer，生产者才能感知其进度
            ringBuffer.getSequencer().addGatingSequences(processor.getSequence());
        }
        return this;
    }

    /**
     * 启动所有消费者线程，返回 RingBuffer 供生产者使用。
     */
    public RingBuffer<E> start() {
        if (started) {
            throw new IllegalStateException("Disruptor already started");
        }
        started = true;
        for (BatchEventProcessor<E> processor : processors) {
            executor.execute(processor);
        }
        return ringBuffer;
    }

    /**
     * 优雅关闭：中断所有消费者等待循环。
     */
    /**
     * 发送关闭信号，中断所有消费者的等待循环。
     * 此方法立即返回，不等待消费者完成当前批次处理。
     * 调用方需自行确保消费完成（如通过 CountDownLatch），并负责关闭传入的 Executor。
     */
    public void shutdown() {
        for (BatchEventProcessor<E> processor : processors) {
            processor.halt();
        }
    }

    public RingBuffer<E> getRingBuffer() {
        return ringBuffer;
    }

    /** 获取所有消费者的 Sequence，供构建下游消费者链时使用 */
    public Sequence[] getSequences() {
        Sequence[] sequences = new Sequence[processors.size()];
        for (int i = 0; i < processors.size(); i++) {
            sequences[i] = processors.get(i).getSequence();
        }
        return sequences;
    }

    /**
     * 便捷发布方法：使用 EventTranslator 填充并发布事件。
     */
    public void publishEvent(EventTranslator<E> translator) {
        long sequence = ringBuffer.next();
        try {
            translator.translateTo(ringBuffer.get(sequence), sequence);
        } finally {
            // finally 确保即使 translator 抛异常也能发布，避免消费者永远阻塞
            ringBuffer.publish(sequence);
        }
    }
}
