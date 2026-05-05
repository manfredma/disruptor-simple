package com.example.disruptor;

/**
 * 忙等待策略：持续自旋检查，延迟最低，但 CPU 100% 占用。
 * 适用于延迟极敏感且有专属 CPU 核心的场景。
 */
public class BusySpinWaitStrategy implements WaitStrategy {

    @Override
    public long waitFor(long sequence, Sequence cursor, SequenceBarrier barrier) throws InterruptedException, AlertException {
        long availableSequence;
        while ((availableSequence = cursor.get()) < sequence) {
            barrier.checkAlert();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 忙等无需通知
    }
}
