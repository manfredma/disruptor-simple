package com.example.disruptor;

/**
 * 前置填充：56 个 byte，使 RingBufferFields 中的字段与对象头分隔在不同缓存行。
 * 使用 byte 而非 long：更精确控制填充字节数（不受 long 对齐约束影响）。
 * 使用继承层级：JVM 保证父类字段优先于子类字段布局，不会被优化消除。
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
