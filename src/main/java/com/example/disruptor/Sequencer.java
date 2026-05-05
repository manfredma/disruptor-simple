package com.example.disruptor;

import java.util.concurrent.locks.LockSupport;

/**
 * 单生产者序号生成器。
 *
 * 三层继承结构消除伪共享：
 * SingleProducerSequencerPad（前置填充 + 冷字段）
 *   -> SingleProducerSequencerFields（热字段：nextValue/cachedValue）
 *     -> Sequencer（后置填充）
 *
 * 单生产者场景无需 CAS，直接用普通 long 递增，性能极高。
 */
public final class Sequencer extends SingleProducerSequencerFields {

    /** 后置填充：将 nextValue/cachedValue 与后续堆对象隔离 */
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    public Sequencer(int bufferSize, WaitStrategy waitStrategy) {
        super(bufferSize, waitStrategy);
    }

    public Sequence getCursor() {
        return cursor;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long next() {
        return next(1);
    }

    public long next(int n) {
        long nextSequence = nextValue + n;
        long wrapPoint = nextSequence - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue) {
            cursor.setVolatile(nextValue);
            long minSequence;
            while (wrapPoint > (minSequence = getMinimumGatingSequence(nextValue))) {
                LockSupport.parkNanos(1L);
            }
            cachedValue = minSequence;
        }

        nextValue = nextSequence;
        return nextSequence;
    }

    public boolean tryNext(long[] result) {
        long nextSequence = nextValue + 1;
        long wrapPoint = nextSequence - bufferSize;
        if (wrapPoint > cachedValue) {
            long minSequence = getMinimumGatingSequence(nextValue);
            cachedValue = minSequence;
            if (wrapPoint > minSequence) {
                return false;
            }
        }
        nextValue = nextSequence;
        result[0] = nextSequence;
        return true;
    }

    public void publish(long sequence) {
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    public SequenceBarrier newBarrier(Sequence... dependentSequences) {
        return new SequenceBarrier(this, waitStrategy, cursor, dependentSequences);
    }
}
