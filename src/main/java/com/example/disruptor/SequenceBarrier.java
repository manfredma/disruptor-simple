package com.example.disruptor;

/**
 * 序号屏障：消费者通过它等待可消费的序号，并检查关闭信号。
 *
 * dependentSequence 含义：
 * - 无上游消费者：等于 cursorSequence（直接等生产者发布）
 * - 有上游消费者：等于上游消费者序号，下游必须等上游处理完才能读取
 */
public class SequenceBarrier {

    private final Sequencer sequencer;
    private final WaitStrategy waitStrategy;
    private final Sequence cursorSequence;
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;

    public SequenceBarrier(Sequencer sequencer, WaitStrategy waitStrategy,
                           Sequence cursorSequence, Sequence[] dependentSequences) {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        // 无上游消费者时 dependentSequence 直接用 cursorSequence
        this.dependentSequence = dependentSequences.length == 0
                ? cursorSequence
                : new MinimumSequence(dependentSequences);
    }

    public long waitFor(long sequence) throws InterruptedException, AlertException {
        checkAlert();
        return waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);
    }

    public long getCursor() {
        return dependentSequence.get();
    }

    public boolean isAlerted() {
        return alerted;
    }

    public void alert() {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    public void clearAlert() {
        alerted = false;
    }

    public void checkAlert() throws AlertException {
        if (alerted) {
            throw AlertException.INSTANCE;
        }
    }
}
