package com.example.disruptor;

/**
 * 前置填充：56 个 byte，目的是将子类字段推离对象头所在的缓存行。
 * 使用 byte 而非 long：精确控制填充字节数，不受 long 8 字节对齐约束影响。
 * 使用继承层级：JVM 保证父类字段先于子类字段布局，不会被优化消除。
 * 局限：JVM 将同类型字段聚集、引用字段排最后，byte 填充无法将引用类型字段
 * 真正包夹在两段填充之间。对 RingBuffer 而言真正有效的防护是数组内部的 BUFFER_PAD 偏移。
 */
abstract class RingBufferPad {
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;
}
