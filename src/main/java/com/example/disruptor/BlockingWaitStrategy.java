package com.example.disruptor;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 阻塞等待策略：使用 Lock/Condition 阻塞消费者线程。
 * CPU 友好，但延迟最高（线程唤醒开销约数微秒）。
 * 适用于对延迟不敏感、CPU 资源紧张的场景。
 */
public class BlockingWaitStrategy implements WaitStrategy {

    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    @Override
    public long waitFor(long sequence, Sequence cursor, SequenceBarrier barrier) throws InterruptedException, AlertException {
        long availableSequence;
        if (cursor.get() < sequence) {
            lock.lock();
            try {
                while (cursor.get() < sequence) {
                    barrier.checkAlert();
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
        }
        // 快速路径：cursor 已超过，直接返回，无需加锁
        while ((availableSequence = cursor.get()) < sequence) {
            barrier.checkAlert();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
