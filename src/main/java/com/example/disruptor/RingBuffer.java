package com.example.disruptor;

/**
 * 环形缓冲区：Disruptor 的核心数据结构。
 *
 * 关键设计：
 * 1. 大小必须是 2 的幂，通过位掩码（& indexMask）代替取模，速度极快
 * 2. 启动时预分配所有事件对象，运行期只更新对象字段，不产生 GC
 * 3. 生产者通过 next/publish 写入，消费者通过 get(sequence) 读取
 */
public class RingBuffer<E> {

    /** 前置填充：防止 entries 数组头部与其他字段产生伪共享 */
    protected long p1, p2, p3, p4, p5, p6, p7;

    private final Object[] entries;
    private final int bufferSize;
    private final int indexMask;  // bufferSize - 1，用于位运算快速取模

    /** 后置填充 */
    protected long p9, p10, p11, p12, p13, p14, p15;

    private final Sequencer sequencer;

    @SuppressWarnings("unchecked")
    private RingBuffer(EventFactory<E> factory, Sequencer sequencer) {
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();
        this.indexMask = bufferSize - 1;
        this.entries = new Object[bufferSize];
        // 预分配：填满所有槽位，后续只复用这些对象
        for (int i = 0; i < bufferSize; i++) {
            entries[i] = factory.newInstance();
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
        // sequence & indexMask 等价于 sequence % bufferSize，但快得多
        return (E) entries[(int) (sequence & indexMask)];
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
