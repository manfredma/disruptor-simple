package com.example.disruptor;

/**
 * 让步等待策略：自旋 100 次后调用 Thread.yield()，兼顾延迟和 CPU 利用率。
 * 适用于多个消费者竞争、需要适当让出 CPU 的场景。
 */
public class YieldingWaitStrategy implements WaitStrategy {

    private static final int SPIN_TRIES = 100;

    @Override
    public long waitFor(long sequence, Sequence cursor, SequenceBarrier barrier) throws InterruptedException, AlertException {
        long availableSequence;
        int counter = SPIN_TRIES;
        while ((availableSequence = cursor.get()) < sequence) {
            barrier.checkAlert();
            counter = applyWaitMethod(counter);
        }
        return availableSequence;
    }

    private int applyWaitMethod(int counter) {
        if (counter == 0) {
            Thread.yield();
            // yield 后重置计数器，实现"自旋100次 -> yield -> 再自旋100次"的交替节奏
            return SPIN_TRIES;
        }
        return counter - 1;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 让步等待无需通知
    }
}
