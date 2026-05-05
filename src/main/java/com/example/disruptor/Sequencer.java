package com.example.disruptor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 单生产者序号生成器（Single Producer Sequencer）。
 *
 * 核心职责：
 * 1. 为生产者分配下一个可写槽位
 * 2. 追踪所有消费者的进度，防止生产者覆盖未消费的槽位
 * 3. 发布序号，通知消费者
 *
 * 单生产者场景无需 CAS，直接用普通 long 递增，性能极高。
 */
public class Sequencer {

    private final int bufferSize;
    private final WaitStrategy waitStrategy;

    /** 生产者游标：已发布的最大序号 */
    private final Sequence cursor = new Sequence(Sequence.INITIAL_VALUE);

    /** 下一个待申请的序号（生产者私有，单线程使用，无需 volatile） */
    private long nextValue = Sequence.INITIAL_VALUE;

    /** 已缓存的消费者最小序号，减少对 gatingSequences 的重复读取 */
    private long cachedGatingSequence = Sequence.INITIAL_VALUE;

    /** 注册的消费者 Sequence 数组，用 AtomicReferenceFieldUpdater 做无锁更新 */
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

    /**
     * 注册消费者 Sequence，生产者在分配槽位时会检查它们的进度。
     */
    public void addGatingSequences(Sequence... sequences) {
        Sequence[] current;
        Sequence[] updated;
        do {
            current = gatingSequences;
            updated = Arrays.copyOf(current, current.length + sequences.length);
            System.arraycopy(sequences, 0, updated, current.length, sequences.length);
        } while (!SEQUENCE_UPDATER.compareAndSet(this, current, updated));
    }

    /**
     * 申请下一个序号（单生产者版本，无 CAS）。
     * 若 RingBuffer 已满（消费者落后太多），自旋等待。
     */
    public long next() {
        return next(1);
    }

    public long next(int n) {
        long nextSequence = nextValue + n;
        // wrapPoint：如果生产者已超前消费者 bufferSize，说明会覆盖未消费数据
        long wrapPoint = nextSequence - bufferSize;

        if (wrapPoint > cachedGatingSequence) {
            long minSequence;
            // 等待消费者推进，直到安全
            while (wrapPoint > (minSequence = getMinimumGatingSequence())) {
                // 忙等，等待消费者推进
            }
            cachedGatingSequence = minSequence;
        }

        nextValue = nextSequence;
        return nextSequence;
    }

    /**
     * 尝试申请序号，若 RingBuffer 已满则立即返回 false（非阻塞）。
     */
    public boolean tryNext(long[] result) {
        long nextSequence = nextValue + 1;
        long wrapPoint = nextSequence - bufferSize;
        if (wrapPoint > cachedGatingSequence) {
            long minSequence = getMinimumGatingSequence();
            cachedGatingSequence = minSequence;
            if (wrapPoint > minSequence) {
                return false;
            }
        }
        nextValue = nextSequence;
        result[0] = nextSequence;
        return true;
    }

    /**
     * 发布序号，使消费者可见。
     * cursor 的推进是消费者感知到新事件的唯一信号。
     */
    public void publish(long sequence) {
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    public SequenceBarrier newBarrier(Sequence... dependentSequences) {
        return new SequenceBarrier(this, waitStrategy, dependentSequences);
    }

    private long getMinimumGatingSequence() {
        long minimum = Long.MAX_VALUE;
        for (Sequence s : gatingSequences) {
            minimum = Math.min(minimum, s.get());
        }
        // 若无消费者，返回 cursor（不阻塞生产者）
        return gatingSequences.length == 0 ? cursor.get() : minimum;
    }
}
