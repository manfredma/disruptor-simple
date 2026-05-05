package com.example.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 批量事件处理器：消费者的核心循环。
 *
 * 关键设计：
 * 1. 批处理：一次等待可能获得多个可消费序号，减少等待次数
 * 2. sequence 记录消费进度，生产者通过它感知消费者位置
 * 3. 优雅关闭：通过 SequenceBarrier.alert() 中断等待循环
 */
public class BatchEventProcessor<E> implements Runnable {

    private final RingBuffer<E> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<E> eventHandler;

    /** 当前消费进度，生产者需要读取它来判断是否可以覆盖槽位 */
    private final Sequence sequence = new Sequence(Sequence.INITIAL_VALUE);

    private final AtomicBoolean running = new AtomicBoolean(false);

    public BatchEventProcessor(RingBuffer<E> ringBuffer,
                               SequenceBarrier sequenceBarrier,
                               EventHandler<E> eventHandler) {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void halt() {
        running.set(false);
        sequenceBarrier.alert();
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread is already running");
        }
        sequenceBarrier.clearAlert();

        long nextSequence = sequence.get() + 1L;

        while (true) {
            try {
                // 等待直到至少有 nextSequence 可消费，返回可消费的最大序号
                long availableSequence = sequenceBarrier.waitFor(nextSequence);

                // 批量处理：一次循环消费 nextSequence 到 availableSequence 之间所有事件
                while (nextSequence <= availableSequence) {
                    E event = ringBuffer.get(nextSequence);
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                // 更新消费进度（一批处理完后统一更新，减少 volatile 写次数）
                sequence.set(availableSequence);

            } catch (AlertException e) {
                // halt() 触发，退出循环
                if (!running.get()) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // 消费者异常不应影响进度推进，记录后继续（生产场景应接入异常处理器）
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }
}
