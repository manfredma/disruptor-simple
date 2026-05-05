package com.example.disruptor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * 单生产者序号生成器（Single Producer Sequencer）。
 *
 * 核心职责：
 * 1. 为生产者分配下一个可写槽位
 * 2. 追踪所有消费者的进度，防止生产者覆盖未消费的槽位
 * 3. 发布序号，通知消费者
 *
 * 单生产者场景无需 CAS，直接用普通 long 递增，性能极高。
 * nextValue / cachedGatingSequence 是生产者私有字段，单线程访问，无需 volatile。
 */
public class Sequencer {

    private final int bufferSize;
    private final WaitStrategy waitStrategy;

    private final Sequence cursor = new Sequence(Sequence.INITIAL_VALUE);

    private long nextValue = Sequence.INITIAL_VALUE;
    private long cachedGatingSequence = Sequence.INITIAL_VALUE;

    @SuppressWarnings("unused")
    private volatile Sequence[] gatingSequences = new Sequence[0];

    private static final AtomicReferenceFieldUpdater<Sequencer, Sequence[]> SEQUENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(Sequencer.class, Sequence[].class, "gatingSequences");

    public Sequencer(int bufferSize, WaitStrategy waitStrategy) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2, was: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    public Sequence getCursor() {
        return cursor;
    }

    public int getBufferSize() {
        return bufferSize;
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

    public long next() {
        return next(1);
    }

    public long next(int n) {
        long nextSequence = nextValue + n;
        long wrapPoint = nextSequence - bufferSize;
        long cachedValue = this.cachedGatingSequence;

        if (wrapPoint > cachedValue || cachedValue > nextValue) {
            // StoreLoad fence：让消费者能看到最新的 cursor
            cursor.setVolatile(nextValue);

            long minSequence;
            while (wrapPoint > (minSequence = getMinimumGatingSequence(nextValue))) {
                LockSupport.parkNanos(1L);
            }
            cachedGatingSequence = minSequence;
        }

        nextValue = nextSequence;
        return nextSequence;
    }

    public boolean tryNext(long[] result) {
        long nextSequence = nextValue + 1;
        long wrapPoint = nextSequence - bufferSize;
        if (wrapPoint > cachedGatingSequence) {
            long minSequence = getMinimumGatingSequence(nextValue);
            cachedGatingSequence = minSequence;
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

    private long getMinimumGatingSequence(long minimum) {
        for (Sequence s : gatingSequences) {
            minimum = Math.min(minimum, s.get());
        }
        return minimum;
    }
}
