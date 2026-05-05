package com.example.disruptor;

/**
 * 持有生产者私有热字段：nextValue / cachedValue。
 * 这两个字段被前置填充（SingleProducerSequencerPad）和后置填充（Sequencer）包夹，
 * 独占缓存行，避免与 cursor/gatingSequences 产生伪共享。
 */
abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad {

    /** 下一个待申请的序号（生产者私有，单线程访问，无需 volatile） */
    long nextValue = Sequence.INITIAL_VALUE;

    /** 已缓存的消费者最小序号，减少对 gatingSequences 的重复读取 */
    long cachedValue = Sequence.INITIAL_VALUE;

    SingleProducerSequencerFields(int bufferSize, WaitStrategy waitStrategy) {
        super(bufferSize, waitStrategy);
    }
}
