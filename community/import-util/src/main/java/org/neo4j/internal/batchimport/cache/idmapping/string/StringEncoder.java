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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static java.lang.Math.max;

/**
 * Encodes String into a long with very small chance of collision, i.e. two different Strings encoded into
 * the same long value.
 *
 * Assumes a single thread making all calls to {@link #encode(Object)}.
 */
public class StringEncoder implements Encoder {
    private static final long UPPER_INT_MASK = 0x00000000_FFFFFFFFL;
    private static final int FOURTH_BYTE = 0x000000FF;
    private static final int ENCODING_THRESHOLD = 7;

    @Override
    public long encode(Object any) {
        String s = convertToString(any);
        // construct bytes from string
        int inputLength = s.length();
        byte[] bytes = new byte[inputLength];
        for (int i = 0; i < inputLength; i++) {
            bytes[i] = (byte) ((s.charAt(i)) % 127);
        }
        if (inputLength <= ENCODING_THRESHOLD) {
            return simplestCode(bytes, inputLength);
        }
        int low = getCode(bytes, inputLength, 1);
        int high = getCode(bytes, inputLength, inputLength - 1);
        int carryOver = lengthEncoder(inputLength) << 1;
        int temp;
        temp = low & FOURTH_BYTE;
        low = low >>> 8 | carryOver << 24;
        carryOver = temp;
        high = high >>> 8 | carryOver << 24;
        return (long) low << 32 | high & UPPER_INT_MASK;
    }

    private String convertToString(Object any) {
        if (any instanceof String string) {
            return string;
        }
        return any.toString();
    }

    private static int lengthEncoder(int length) {
        if (length < 32) {
            return length;
        } else if (length <= 96) {
            return length >> 1;
        } else if (length <= 324) {
            return length >> 2;
        } else if (length <= 580) {
            return length >> 3;
        } else if (length <= 836) {
            return length >> 4;
        } else {
            return 127;
        }
    }

    private static long simplestCode(byte[] bytes, int inputLength) {
        int low = max(inputLength, 1) << 25;
        int high = 0;
        for (int i = 0; i < 3 && i < inputLength; i++) {
            low = low | bytes[i] << ((2 - i) * 8);
        }
        for (int i = 3; i < 7 && i < inputLength; i++) {
            high = high | (bytes[i]) << ((6 - i) * 8);
        }
        return (long) low << 32 | high & UPPER_INT_MASK;
    }

    private static int getCode(byte[] bytes, int inputLength, int order) {
        long code = 0;
        int size = inputLength;
        for (int i = 0; i < size; i++) {
            // code += (((long)bytes[(i*order) % size]) << (i % 7)*8);
            long val = bytes[(i * order) % size];
            for (int k = 1; k <= i; k++) {
                long prev = val;
                val = (val << 4) + prev; // % Integer.MAX_VALUE;
            }
            code += val;
        }
        return (int) code;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
