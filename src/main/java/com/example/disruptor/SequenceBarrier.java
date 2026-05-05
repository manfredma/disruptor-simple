package com.example.disruptor;

/**
 * 序号屏障：消费者通过它等待可消费的序号，并检查关闭信号。
 *
 * 在多消费者链路中，下游消费者需要等待上游消费者处理完毕，
 * dependentSequences 即为上游消费者的 Sequence 列表。
 */
public class SequenceBarrier {

    private final Sequencer sequencer;
    private final WaitStrategy waitStrategy;
    /** 依赖的上游消费者序号（为空则直接依赖生产者游标） */
    private final Sequence[] dependentSequences;
    private volatile boolean alerted = false;

    public SequenceBarrier(Sequencer sequencer, WaitStrategy waitStrategy, Sequence[] dependentSequences) {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.dependentSequences = dependentSequences;
    }

    /**
     * 等待直到 sequence 可消费，返回实际可消费的最大序号。
     */
    public long waitFor(long sequence) throws InterruptedException, AlertException {
        checkAlert();
        // 先等生产者 cursor 推进到 sequence
        waitStrategy.waitFor(sequence, sequencer.getCursor(), this);

        // 若有上游消费者依赖，必须循环等待直到其最小序号 >= sequence，
        // 才能保证下游不会读到上游尚未处理完的槽位。
        // 不能用 Math.min 一次性截断：上游还没到时需要持续轮询，而非返回一个小于 sequence 的值导致消费者空转。
        long availableSequence;
        if (dependentSequences.length > 0) {
            while ((availableSequence = getMinimumSequence(dependentSequences)) < sequence) {
                checkAlert();
            }
        } else {
            availableSequence = sequencer.getCursor().get();
        }
        return availableSequence;
    }

    public long getCursor() {
        return sequencer.getCursor().get();
    }

    public void checkAlert() throws AlertException {
        if (alerted) {
            throw AlertException.INSTANCE;
        }
    }

    public void alert() {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    public void clearAlert() {
        alerted = false;
    }

    private static long getMinimumSequence(Sequence[] sequences) {
        long minimum = Long.MAX_VALUE;
        for (Sequence s : sequences) {
            minimum = Math.min(minimum, s.get());
        }
        return minimum;
    }
}
