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
package org.neo4j.util;

import static java.lang.Integer.toHexString;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;

public final class BitUtils {

    private BitUtils() {}

    public static boolean bitFlag(byte flags, byte flag) {
        assert isPowerOfTwo(flag) : "flag should be a power of 2, not: 0x" + toHexString(flag);
        return (flags & flag) == flag;
    }

    public static boolean bitFlag(int flags, int flag) {
        assert isPowerOfTwo(flag) : "flag should be a power of 2, not: 0x" + toHexString(flag);
        return (flags & flag) == flag;
    }

    public static int bitFlag(boolean value, int flag) {
        assert isPowerOfTwo(flag) : "flag should be a power of 2, not: 0x" + toHexString(flag);
        return value ? flag : 0;
    }

    public static byte bitFlag(boolean value, byte flag) {
        assert isPowerOfTwo(flag) : "flag should be a power of 2, not: 0x" + toHexString(flag);
        return value ? flag : 0;
    }

    public static byte bitFlags(
            int flag1, int flag2, int flag3, int flag4, int flag5, int flag6, int flag7, int flag8) {
        int result = flag1 | flag2 | flag3 | flag4 | flag5 | flag6 | flag7 | flag8;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2, int flag3, int flag4, int flag5, int flag6, int flag7) {
        int result = flag1 | flag2 | flag3 | flag4 | flag5 | flag6 | flag7;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2, int flag3, int flag4, int flag5, int flag6) {
        int result = flag1 | flag2 | flag3 | flag4 | flag5 | flag6;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2, int flag3, int flag4, int flag5) {
        int result = flag1 | flag2 | flag3 | flag4 | flag5;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2, int flag3, int flag4) {
        int result = flag1 | flag2 | flag3 | flag4;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2, int flag3) {
        int result = flag1 | flag2 | flag3;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static byte bitFlags(int flag1, int flag2) {
        int result = flag1 | flag2;
        assert (result & ~0xFF) == 0;
        return (byte) result;
    }

    public static void numberToString(StringBuilder builder, long value, int numberOfBytes) {
        builder.append('[');
        for (int i = 8 * numberOfBytes - 1; i >= 0; i--) {
            boolean isSet = (value & (1L << i)) != 0;
            builder.append(isSet ? "1" : "0");
            if (i > 0 && i % 8 == 0) {
                builder.append(',');
            }
        }
        builder.append(']');
    }

    public static String numbersToBitString(long[] values) {
        StringBuilder builder = new StringBuilder();
        for (long value : values) {
            numberToString(builder, value, 8);
        }
        return builder.toString();
    }
}
