package com.example.disruptor;

/**
 * 事件转换器：生产者用它填充预分配的事件对象。
 * 将业务数据写入 RingBuffer 中已有的事件对象，避免创建新对象。
 */
public interface EventTranslator<E> {
    void translateTo(E event, long sequence);
}
