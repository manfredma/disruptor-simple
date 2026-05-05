package com.example.disruptor;

/**
 * 让步等待策略：自旋 100 次后调用 Thread.yield()，兼顾延迟和 CPU 利用率。
 * 适用于多个消费者竞争、需要适当让出 CPU 的场景。
 *
 * 注意：yield 后返回原 counter（0），不重置为 SPIN_TRIES，
 * 使得 yield 后每次仍立即再 yield，直到有事件到来。
 */
public final class YieldingWaitStrategy implements WaitStrategy {

    private static final int SPIN_TRIES = 100;

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        int counter = SPIN_TRIES;
        while ((availableSequence = dependentSequence.get()) < sequence) {
            counter = applyWaitMethod(barrier, counter);
        }
        return availableSequence;
    }

    private int applyWaitMethod(SequenceBarrier barrier, int counter) throws AlertException {
        barrier.checkAlert();
        if (0 == counter) {
            Thread.yield();
        } else {
            return counter - 1;
        }
        return counter;
    }

    @Override
    public void signalAllWhenBlocking() {
    }
}
