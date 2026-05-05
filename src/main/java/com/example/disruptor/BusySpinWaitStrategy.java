package com.example.disruptor;

/**
 * 忙等待策略：持续自旋检查，延迟最低，但 CPU 100% 占用。
 * 适用于延迟极敏感且有专属 CPU 核心的场景。
 * Thread.onSpinWait() 提示 CPU 当前处于自旋等待，允许硬件优化功耗。
 */
public final class BusySpinWaitStrategy implements WaitStrategy {

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            Thread.onSpinWait();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
    }
}
