package com.example.disruptor;

/**
 * 消费者事件处理接口。
 *
 * @param <E> 事件类型
 */
public interface EventHandler<E> {

    /**
     * @param event      当前事件对象（预分配，不要持有引用）
     * @param sequence   事件序号
     * @param endOfBatch 是否为本批次最后一个事件（可用于批量 flush）
     */
    void onEvent(E event, long sequence, boolean endOfBatch) throws Exception;
}
