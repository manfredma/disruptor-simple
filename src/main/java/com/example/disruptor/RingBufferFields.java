package com.example.disruptor;

/**
 * 持有 RingBuffer 核心字段的中间层。
 * 布局意图：RingBufferPad(56B前置) -> RingBufferFields(热字段) -> RingBuffer(56B后置)
 * 热字段被前后两段填充包夹，与对象头和相邻对象隔离在不同缓存行。
 */
abstract class RingBufferFields<E> extends RingBufferPad {

    static final int BUFFER_PAD = 32;

    final long indexMask;
    final Object[] entries;
    final int bufferSize;
    final Sequencer sequencer;

    @SuppressWarnings("unchecked")
    RingBufferFields(EventFactory<E> factory, Sequencer sequencer) {
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        this.indexMask = bufferSize - 1;
        this.entries = new Object[bufferSize + 2 * BUFFER_PAD];
        for (int i = 0; i < bufferSize; i++) {
            entries[BUFFER_PAD + i] = factory.newInstance();
        }
    }

    @SuppressWarnings("unchecked")
    protected final E elementAt(long sequence) {
        return (E) entries[BUFFER_PAD + (int) (sequence & indexMask)];
    }
}
