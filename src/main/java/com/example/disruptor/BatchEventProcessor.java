package com.example.disruptor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批量事件处理器：消费者的核心循环。
 *
 * 关键设计：
 * 1. 批处理：一次等待可能获得多个可消费序号，减少等待次数
 * 2. sequence 记录消费进度，生产者通过它感知消费者位置
 * 3. 优雅关闭：通过 SequenceBarrier.alert() 中断等待循环
 *
 * 状态机（与真实 Disruptor 一致）：
 * IDLE(0) -> RUNNING(2)：正常启动
 * RUNNING(2) -> HALTED(1)：halt() 调用
 * HALTED(1)/RUNNING(2) -> IDLE(0)：run() 退出时重置
 * halt() 设为 HALTED 而非 IDLE，区分"主动停止"和"未启动"两种状态。
 */
public class BatchEventProcessor<E> implements Runnable {

    private static final int IDLE = 0;
    private static final int HALTED = 1;
    private static final int RUNNING = 2;

    private final AtomicInteger running = new AtomicInteger(IDLE);

    private final RingBuffer<E> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<E> eventHandler;

    private final Sequence sequence = new Sequence(Sequence.INITIAL_VALUE);

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
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    public boolean isRunning() {
        return running.get() != IDLE;
    }

    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();
            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                running.set(IDLE);
            }
        } else {
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            }
        }
    }

    private void processEvents() {
        long nextSequence = sequence.get() + 1L;
        while (true) {
            try {
                long availableSequence = sequenceBarrier.waitFor(nextSequence);
                while (nextSequence <= availableSequence) {
                    E event = ringBuffer.get(nextSequence);
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }
                sequence.set(availableSequence);
            } catch (AlertException e) {
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }
}
