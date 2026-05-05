package com.example.disruptor;

/**
 * 消费者等待新事件时的策略。
 *
 * waitFor 有 4 个参数，与真实 Disruptor 源码一致：
 * - cursor：生产者游标，仅此 Sequence 在 publish 时会被 signal
 * - dependentSequence：依赖的上游序号（单消费者时等于 cursor，多消费者链路时为上游消费者序号）
 * - barrier：用于在等待中检查 alert 状态
 */
public interface WaitStrategy {

    long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException;

    void signalAllWhenBlocking();
}
