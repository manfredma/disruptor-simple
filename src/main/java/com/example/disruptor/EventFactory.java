package com.example.disruptor;

/**
 * 事件工厂：RingBuffer 初始化时预分配所有事件对象。
 * 预分配避免运行期 GC，是 Disruptor 零 GC 的关键。
 */
public interface EventFactory<E> {
    E newInstance();
}
