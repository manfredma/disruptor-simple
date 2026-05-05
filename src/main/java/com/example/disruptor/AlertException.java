package com.example.disruptor;

/**
 * 用于中断消费者等待循环的信号异常。
 * 使用预分配单例避免异常构造开销（不需要堆栈跟踪）。
 */
public class AlertException extends Exception {

    public static final AlertException INSTANCE = new AlertException();

    private AlertException() {
        // 禁用堆栈跟踪，此异常仅作控制流信号使用
        super(null, null, true, false);
    }
}
