package com.example.disruptor;

/**
 * 环形缓冲区：Disruptor 的核心数据结构。
 *
 * 关键设计：
 * 1. 大小必须是 2 的幂，通过位掩码（& indexMask）代替取模，速度极快
 * 2. 启动时预分配所有事件对象，运行期只更新对象字段，不产生 GC
 * 3. 生产者通过 next/publish 写入，消费者通过 get(sequence) 读取
 *
 * 伪共享防护（False Sharing），照搬真实 Disruptor 源码的两层设计：
 *
 * 第一层：继承 RingBufferPad（56 byte 前置填充）+ RingBuffer 末尾 56 byte 后置填充，
 * 将 entries/indexMask 等热字段夹在两段填充之间，与对象头和相邻对象隔离到独立缓存行。
 *
 * 第二层：entries 数组申请 bufferSize + 2*BUFFER_PAD 个槽，实际数据从 BUFFER_PAD 偏移开始存放。
 * 这样数组头部（对象头 + length 字段）和尾部都有 32 个空槽作为缓冲，
 * 防止数组首尾元素与数组元数据或相邻堆对象产生伪共享。
 */
public final class RingBuffer<E> extends RingBufferPad {

    private static final int BUFFER_PAD = 32;

    private final long indexMask;
    private final Object[] entries;
    private final int bufferSize;
    private final Sequencer sequencer;

    /** 后置填充：与前置填充共同将上方字段包夹在独立缓存行中 */
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    @SuppressWarnings("unchecked")
    private RingBuffer(EventFactory<E> factory, Sequencer sequencer) {
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        this.indexMask = bufferSize - 1;
        // 数组多申请 2*BUFFER_PAD 个槽，实际元素从 BUFFER_PAD 偏移开始存放，
        // 使数组首尾各有 32 个空槽，防止首尾元素与数组元数据产生伪共享
        this.entries = new Object[bufferSize + 2 * BUFFER_PAD];
        for (int i = 0; i < bufferSize; i++) {
            entries[BUFFER_PAD + i] = factory.newInstance();
        }
    }

    public static <E> RingBuffer<E> create(EventFactory<E> factory, int bufferSize, WaitStrategy waitStrategy) {
        Sequencer sequencer = new Sequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(factory, sequencer);
    }

    /**
     * 获取指定序号对应的事件对象（预分配，永不为 null）。
     * 生产者：先 get 取到对象填充数据，再 publish；消费者：直接读取。
     */
    @SuppressWarnings("unchecked")
    public E get(long sequence) {
        return (E) entries[BUFFER_PAD + (int) (sequence & indexMask)];
    }

    /**
     * 申请下一个可写序号（阻塞直到有空槽）。
     * 必须配合 publish 使用，建议用 try/finally 保证发布。
     */
    public long next() {
        return sequencer.next();
    }

    public long next(int n) {
        return sequencer.next(n);
    }

    /**
     * 发布序号，消费者从此刻起可见该事件。
     */
    public void publish(long sequence) {
        sequencer.publish(sequence);
    }

    public Sequencer getSequencer() {
        return sequencer;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
        return sequencer.newBarrier(sequencesToTrack);
    }
}
