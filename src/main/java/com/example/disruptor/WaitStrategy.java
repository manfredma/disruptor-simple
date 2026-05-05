package com.example.disruptor;

/**
 * 消费者等待新事件时的策略。
 *
 * 三种实现对应三种性能/CPU 占用取舍：
 * - BusySpin：最低延迟，持续占用 CPU
 * - Yielding：略高延迟，友好让出 CPU 时间片
 * - Blocking：最高延迟，依赖 Lock/Condition，CPU 占用最低
 */
public interface WaitStrategy {

    /**
     * 等待直到 cursor >= sequence。
     *
     * @param sequence  消费者期望消费的序号
     * @param cursor    生产者游标（RingBuffer 已发布到的序号）
     * @param barrier   依赖的上游 Sequence（多消费者链路时使用）
     * @return 实际可消费的最大序号（>= sequence）
     */
    long waitFor(long sequence, Sequence cursor, SequenceBarrier barrier) throws InterruptedException, AlertException;

    /**
     * 生产者发布事件后通知等待中的消费者（仅 Blocking 策略需要实现）。
     */
    void signalAllWhenBlocking();
}
