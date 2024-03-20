/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.hashing;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public final class WyHash {
    private static final VarHandle LE_INTEGER = MethodHandles.byteArrayViewVarHandle(
                    int[].class, ByteOrder.LITTLE_ENDIAN)
            .withInvokeExactBehavior();
    private static final VarHandle LE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)
            .withInvokeExactBehavior();
    private static final long WYP_0 = 0xa0761d6478bd642fL;
    private static final long WYP_1 = 0xe7037ed1a0b428dbL;
    private static final long WYP_2 = 0x8ebc6af09c88c6e3L;
    private static final long WYP_3 = 0x589965cc75374cc3L;
    private static final long WYP_4 = 0x1d8e4e27c47d124fL;

    private WyHash() {}

    public static long hashLong(long input) {
        long hi = input & 0xFFFFFFFFL;
        long lo = (input >>> 32) & 0xFFFFFFFFL;
        return wymum(wymum(hi ^ WYP_0, lo ^ WYP_1), 8 ^ WYP_4);
    }

    public static long hashInt(int input) {
        long longInput = (input & 0xFFFFFFFFL);
        return wymum(wymum(longInput ^ WYP_0, longInput ^ WYP_1), 4 ^ WYP_4);
    }

    public static long hashShort(short input) {
        long hi = (input >>> 8) & 0xFFL;
        long wyr3 = hi | hi << 8 | (input & 0xFFL) << 16;
        return wymum(wymum(wyr3 ^ WYP_0, WYP_1), 2 ^ WYP_4);
    }

    public static long hashChar(final char input) {
        return hashShort((short) input);
    }

    public static long hashByte(final byte input) {
        long hi = input & 0xFFL;
        long wyr3 = hi | hi << 8 | hi << 16;
        return wymum(wymum(wyr3 ^ WYP_0, WYP_1), 1 ^ WYP_4);
    }

    public static long hash(final byte[] input, final int off, final int len) {
        return WyHash.wyHash64(input, off, len);
    }

    private static long unsignedLongMulXorFold(final long lhs, final long rhs) {
        final long upper = Math.multiplyHigh(lhs, rhs) + ((lhs >> 63) & rhs) + ((rhs >> 63) & lhs);
        final long lower = lhs * rhs;
        return lower ^ upper;
    }

    private static long wymum(final long lhs, final long rhs) {
        return unsignedLongMulXorFold(lhs, rhs);
    }

    private static long wyr3(final byte[] in, final int index, final int k) {
        return ((long) in[index] << 16) | ((long) in[index + (k >>> 1)] << 8) | ((long) in[index + k - 1]);
    }

    private static long u64Rorate32(final byte[] in, final int index) {
        return (u32(in, index) << 32) | u32(in, index + 4);
    }

    private static long u32(final byte[] input, final int offset) {
        return (int) LE_INTEGER.get(input, offset) & 0xFFFFFFFFL;
    }

    private static long i64(final byte[] input, final int offset) {
        return (long) LE_LONG.get(input, offset);
    }

    /**
     *
     * @param input the type wrapped by the Access, ex. byte[], ByteBuffer, etc.
     * @param off offset to the input
     * @param length length to read from input
     * @return hash result
     */
    static long wyHash64(final byte[] input, final int off, final int length) {
        if (length <= 0) {
            return 0;
        } else if (length < 4) {
            return wymum(wymum(wyr3(input, off, length) ^ WYP_0, WYP_1), length ^ WYP_4);
        } else if (length <= 8) {
            return wymum(wymum(u32(input, off) ^ WYP_0, u32(input, off + length - 4) ^ WYP_1), length ^ WYP_4);
        } else if (length <= 16) {
            return wymum(
                    wymum(u64Rorate32(input, off) ^ WYP_0, u64Rorate32(input, off + length - 8) ^ WYP_1),
                    length ^ WYP_4);
        } else if (length <= 24) {
            return wymum(
                    wymum(u64Rorate32(input, off) ^ WYP_0, u64Rorate32(input, off + 8) ^ WYP_1)
                            ^ wymum(u64Rorate32(input, off + length - 8) ^ WYP_2, WYP_3),
                    length ^ WYP_4);
        } else if (length <= 32) {
            return wymum(
                    wymum(u64Rorate32(input, off) ^ WYP_0, u64Rorate32(input, off + 8) ^ WYP_1)
                            ^ wymum(u64Rorate32(input, off + 16) ^ WYP_2, u64Rorate32(input, off + length - 8) ^ WYP_3),
                    length ^ WYP_4);
        }
        long seed = 0;
        long see1 = 0;
        int i = length, p = off;
        for (; i > 256; i -= 256, p += 256) {
            seed = wymum(i64(input, p) ^ seed ^ WYP_0, i64(input, p + 8) ^ seed ^ WYP_1)
                    ^ wymum(i64(input, p + 16) ^ seed ^ WYP_2, i64(input, p + 24) ^ seed ^ WYP_3);
            see1 = wymum(i64(input, p + 32) ^ see1 ^ WYP_1, i64(input, p + 40) ^ see1 ^ WYP_2)
                    ^ wymum(i64(input, p + 48) ^ see1 ^ WYP_3, i64(input, p + 56) ^ see1 ^ WYP_0);
            seed = wymum(i64(input, p + 64) ^ seed ^ WYP_0, i64(input, p + 72) ^ seed ^ WYP_1)
                    ^ wymum(i64(input, p + 80) ^ seed ^ WYP_2, i64(input, p + 88) ^ seed ^ WYP_3);
            see1 = wymum(i64(input, p + 96) ^ see1 ^ WYP_1, i64(input, p + 104) ^ see1 ^ WYP_2)
                    ^ wymum(i64(input, p + 112) ^ see1 ^ WYP_3, i64(input, p + 120) ^ see1 ^ WYP_0);
            seed = wymum(i64(input, p + 128) ^ seed ^ WYP_0, i64(input, p + 136) ^ seed ^ WYP_1)
                    ^ wymum(i64(input, p + 144) ^ seed ^ WYP_2, i64(input, p + 152) ^ seed ^ WYP_3);
            see1 = wymum(i64(input, p + 160) ^ see1 ^ WYP_1, i64(input, p + 168) ^ see1 ^ WYP_2)
                    ^ wymum(i64(input, p + 176) ^ see1 ^ WYP_3, i64(input, p + 184) ^ see1 ^ WYP_0);
            seed = wymum(i64(input, p + 192) ^ seed ^ WYP_0, i64(input, p + 200) ^ seed ^ WYP_1)
                    ^ wymum(i64(input, p + 208) ^ seed ^ WYP_2, i64(input, p + 216) ^ seed ^ WYP_3);
            see1 = wymum(i64(input, p + 224) ^ see1 ^ WYP_1, i64(input, p + 232) ^ see1 ^ WYP_2)
                    ^ wymum(i64(input, p + 240) ^ see1 ^ WYP_3, i64(input, p + 248) ^ see1 ^ WYP_0);
        }
        for (; i > 32; i -= 32, p += 32) {
            seed = wymum(i64(input, p) ^ seed ^ WYP_0, i64(input, p + 8) ^ seed ^ WYP_1);
            see1 = wymum(i64(input, p + 16) ^ see1 ^ WYP_2, i64(input, p + 24) ^ see1 ^ WYP_3);
        }
        if (i < 4) {
            seed = wymum(wyr3(input, p, i) ^ seed ^ WYP_0, seed ^ WYP_1);
        } else if (i <= 8) {
            seed = wymum(u32(input, p) ^ seed ^ WYP_0, u32(input, p + i - 4) ^ seed ^ WYP_1);
        } else if (i <= 16) {
            seed = wymum(u64Rorate32(input, p) ^ seed ^ WYP_0, u64Rorate32(input, p + i - 8) ^ seed ^ WYP_1);
        } else if (i <= 24) {
            seed = wymum(u64Rorate32(input, p) ^ seed ^ WYP_0, u64Rorate32(input, p + 8) ^ seed ^ WYP_1);
            see1 = wymum(u64Rorate32(input, p + i - 8) ^ see1 ^ WYP_2, see1 ^ WYP_3);
        } else {
            seed = wymum(u64Rorate32(input, p) ^ seed ^ WYP_0, u64Rorate32(input, p + 8) ^ seed ^ WYP_1);
            see1 = wymum(u64Rorate32(input, p + 16) ^ see1 ^ WYP_2, u64Rorate32(input, p + i - 8) ^ see1 ^ WYP_3);
        }
        return wymum(seed ^ see1, length ^ WYP_4);
    }
}
