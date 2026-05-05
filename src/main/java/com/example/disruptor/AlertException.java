package com.example.disruptor;

/**
 * 用于中断消费者等待循环的信号异常。
 * 使用预分配单例避免异常构造开销。
 * override fillInStackTrace() 而非构造时传 false，与真实 Disruptor 源码一致。
 */
public final class AlertException extends Exception {

    public static final AlertException INSTANCE = new AlertException();

    private AlertException() {
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
