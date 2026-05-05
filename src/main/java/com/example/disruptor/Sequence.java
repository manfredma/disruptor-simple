package com.example.disruptor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 带缓存行填充的原子序号，消除伪共享（False Sharing）。
 *
 * 填充结构：LhsPadding(56B) -> value(8B) -> RhsPadding(56B)
 * value 字段被前后各 56 字节的 byte 填充包夹，独占一条 64 字节缓存行。
 * 使用继承层级而非同类字段，保证 JVM 不会消除填充。
 *
 * 内存语义使用 VarHandle（JDK 9+），替代 Unsafe：
 * - get(): acquire 读（对应 volatile read）
 * - set(): release 写（store-store 屏障，比 volatile 写弱，避免 full fence）
 * - setVolatile(): release + full fence（完整 volatile 写）
 */
public class Sequence extends RhsPadding {

    static final long INITIAL_VALUE = -1L;

    private static final VarHandle VALUE_FIELD;

    static {
        try {
            VALUE_FIELD = MethodHandles.lookup().in(Sequence.class)
                    .findVarHandle(Sequence.class, "value", long.class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Sequence() {
        this(INITIAL_VALUE);
    }

    public Sequence(long initialValue) {
        VarHandle.releaseFence();
        this.value = initialValue;
    }

    public long get() {
        long v = this.value;
        VarHandle.acquireFence();
        return v;
    }

    public void set(long value) {
        VarHandle.releaseFence();
        this.value = value;
    }

    public void setVolatile(long value) {
        VarHandle.releaseFence();
        this.value = value;
        VarHandle.fullFence();
    }

    public boolean compareAndSet(long expectedValue, long newValue) {
        return VALUE_FIELD.compareAndSet(this, expectedValue, newValue);
    }

    public long incrementAndGet() {
        return addAndGet(1L);
    }

    public long addAndGet(long increment) {
        return (long) VALUE_FIELD.getAndAdd(this, increment) + increment;
    }

    @Override
    public String toString() {
        return Long.toString(get());
    }
}
