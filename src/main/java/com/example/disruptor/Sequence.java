package com.example.disruptor;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * 带缓存行填充的原子序号，消除伪共享（False Sharing）。
 *
 * CPU 缓存行通常 64 字节。若两个变量落在同一缓存行，
 * 一个线程写入会导致另一线程的缓存行失效，造成不必要的缓存同步。
 *
 * 填充方式：前 7 个 long（56字节）+ value（8字节）+ 后 7 个 long（56字节）
 * 确保 value 独占一条缓存行。
 */
public class Sequence {

    // value 前面的填充，占满 56 字节
    protected long p1, p2, p3, p4, p5, p6, p7;

    private volatile long value;

    // value 后面的填充，防止与后续字段共享缓存行
    protected long p9, p10, p11, p12, p13, p14, p15;

    static final long INITIAL_VALUE = -1L;

    private static final Unsafe UNSAFE;
    private static final long VALUE_OFFSET;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Sequence.class.getDeclaredField("value"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Sequence() {
        this(INITIAL_VALUE);
    }

    public Sequence(long initialValue) {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, initialValue);
    }

    public long get() {
        return value;
    }

    public void set(long value) {
        // putOrderedLong：store-store 屏障，比 volatile 写弱但避免 full fence，性能更好
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    public void setVolatile(long value) {
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }

    public boolean compareAndSet(long expectedValue, long newValue) {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedValue, newValue);
    }

    public long incrementAndGet() {
        return addAndGet(1L);
    }

    public long addAndGet(long increment) {
        long currentValue;
        long newValue;
        do {
            currentValue = get();
            newValue = currentValue + increment;
        } while (!compareAndSet(currentValue, newValue));
        return newValue;
    }

    @Override
    public String toString() {
        return Long.toString(get());
    }
}
