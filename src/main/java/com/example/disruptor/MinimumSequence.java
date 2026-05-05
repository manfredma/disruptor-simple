package com.example.disruptor;

/**
 * 聚合多个 Sequence，get() 返回其中最小值。
 * 用于多消费者链路中，下游 barrier 等待所有上游消费者都推进到某个序号。
 * 对应真实 Disruptor 源码的 FixedSequenceGroup。
 */
final class MinimumSequence extends Sequence {

    private final Sequence[] sequences;

    MinimumSequence(Sequence[] sequences) {
        this.sequences = sequences;
    }

    @Override
    public long get() {
        long minimum = Long.MAX_VALUE;
        for (Sequence s : sequences) {
            minimum = Math.min(minimum, s.get());
        }
        return minimum;
    }
}
