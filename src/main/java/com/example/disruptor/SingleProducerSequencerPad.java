package com.example.disruptor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 前置填充：将 AbstractSequencer 的字段（bufferSize/waitStrategy/cursor/gatingSequences）
 * 与 SingleProducerSequencerFields 的热字段（nextValue/cachedValue）隔离在不同缓存行。
 */
abstract class SingleProducerSequencerPad {

    protected final int bufferSize;
    protected final WaitStrategy waitStrategy;
    protected final Sequence cursor = new Sequence(Sequence.INITIAL_VALUE);

    @SuppressWarnings("unused")
    protected volatile Sequence[] gatingSequences = new Sequence[0];

    protected static final AtomicReferenceFieldUpdater<SingleProducerSequencerPad, Sequence[]> SEQUENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SingleProducerSequencerPad.class, Sequence[].class, "gatingSequences");

    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    SingleProducerSequencerPad(int bufferSize, WaitStrategy waitStrategy) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2, was: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    public void addGatingSequences(Sequence... sequences) {
        Sequence[] current;
        Sequence[] updated;
        do {
            current = gatingSequences;
            updated = Arrays.copyOf(current, current.length + sequences.length);
            System.arraycopy(sequences, 0, updated, current.length, sequences.length);
        } while (!SEQUENCE_UPDATER.compareAndSet(this, current, updated));
    }

    long getMinimumGatingSequence(long minimum) {
        for (Sequence s : gatingSequences) {
            minimum = Math.min(minimum, s.get());
        }
        return minimum;
    }
}
