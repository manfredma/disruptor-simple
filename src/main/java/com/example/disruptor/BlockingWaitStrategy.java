package com.example.disruptor;

/**
 * 阻塞等待策略：使用 synchronized + Object.wait/notifyAll。
 * CPU 友好，但延迟最高（线程唤醒开销约数微秒）。
 * 适用于对延迟不敏感、CPU 资源紧张的场景。
 *
 * 与真实 Disruptor 源码一致：用 Object mutex 而非 ReentrantLock，
 * 第二阶段（等待 dependentSequence）用 Thread.onSpinWait() 自旋。
 */
public final class BlockingWaitStrategy implements WaitStrategy {

    private final Object mutex = new Object();

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        if (cursor.get() < sequence) {
            synchronized (mutex) {
                while (cursor.get() < sequence) {
                    barrier.checkAlert();
                    mutex.wait();
                }
            }
        }
        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            Thread.onSpinWait();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }
}
