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

import java.util.Arrays;

/**
 * Got bits to store, shift and retrieve and they are more than what fits in a long?
 * Use {@link BitBuffer} then.
 */
public final class BitBuffer {
    // 3: ...
    // 2:   [   23    ][   22    ][   21    ][   20    ][   19    ][   18    ][   17    ][   16    ] <--\
    //                                                                                                   |
    //    /---------------------------------------------------------------------------------------------/
    //   |
    // 1: \-[   15    ][   14    ][   13    ][   12    ][   11    ][   10    ][    9    ][    8    ] <--\
    //                                                                                                   |
    //    /---------------------------------------------------------------------------------------------/
    //   |
    // 0: \-[    7    ][    6    ][    5    ][    4    ][    3    ][    2    ][    1    ][    0    ] <---- START
    private final long[] longs;
    private final int numberOfBytes;
    private int writePosition;
    private int readPosition;

    public static BitBuffer bits(int numberOfBytes) {
        int requiredLongs = requiredLongs(numberOfBytes);
        return new BitBuffer(new long[requiredLongs], numberOfBytes);
    }

    public static int requiredLongs(int numberOfBytes) {
        return ((numberOfBytes - 1) >> 3) + 1; // /8
    }

    public static BitBuffer bitsFromLongs(long[] longs) {
        return new BitBuffer(longs, longs.length << 3); // *8
    }

    public static BitBuffer bitsFromBytes(byte[] bytes) {
        return bitsFromBytes(bytes, 0);
    }

    public static BitBuffer bitsFromBytes(byte[] bytes, int startIndex) {
        final int count = bytes.length;
        BitBuffer bits = bits(count - startIndex);
        for (int i = startIndex; i < count; i++) {
            bits.put(bytes[i]);
        }
        return bits;
    }

    public static BitBuffer bitsFromBytes(byte[] bytes, int offset, int length) {
        BitBuffer bits = bits(length - offset);
        for (int i = offset; i < (offset + length); i++) {
            bits.put(bytes[i]);
        }
        return bits;
    }

    private BitBuffer(long[] longs, int numberOfBytes) {
        this.longs = longs;
        this.numberOfBytes = numberOfBytes;
    }

    /**
     * A mask which has the {@code steps} least significant bits set to 1, all others 0.
     * It's used to carry bits over between carriers (longs) when shifting right.
     *
     * @param steps the number of least significant bits to have set to 1 in the mask.
     * @return the created mask.
     */
    public static long rightOverflowMask(int steps) {
        return -1L >>> (64 - steps);
    }

    /**
     * Returns the underlying long values that has got all the bits applied.
     * The first item in the array has got the most significant bits.
     *
     * @return the underlying long values that has got all the bits applied.
     */
    @SuppressWarnings("EI_EXPOSE_REP")
    public long[] getLongs() {
        return longs;
    }

    public byte[] asBytes() {
        return asBytes(0);
    }

    public byte[] asBytes(int offsetBytes) {
        int readPositionBefore = readPosition;
        readPosition = 0;
        try {
            byte[] result = new byte[numberOfBytes + offsetBytes];
            for (int i = 0; i < numberOfBytes; i++) {
                result[i + offsetBytes] = getByte();
            }
            return result;
        } finally {
            readPosition = readPositionBefore;
        }
    }

    /**
     * A very nice toString, showing each bit, divided into groups of bytes and
     * lines of 8 bytes.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int longIndex = longs.length - 1; longIndex >= 0; longIndex--) {
            long value = longs[longIndex];
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(longIndex);
            builder.append(':');
            BitUtils.numberToString(builder, value, 8);
            if (longIndex == 0) {
                builder.append(" <-- START");
            }
        }
        return builder.toString();
    }

    public BitBuffer put(byte value) {
        return put(value, Byte.SIZE);
    }

    public BitBuffer put(byte value, int steps) {
        return put((long) value, steps);
    }

    public BitBuffer put(short value) {
        return put(value, Short.SIZE);
    }

    public BitBuffer put(short value, int steps) {
        return put((long) value, steps);
    }

    public BitBuffer put(int value) {
        return put(value, Integer.SIZE);
    }

    public BitBuffer put(int value, int steps) {
        return put((long) value, steps);
    }

    public BitBuffer put(long value) {
        return put(value, Long.SIZE);
    }

    public BitBuffer put(long value, int steps) {
        int lowLongIndex = writePosition >> 6; // /64
        int lowBitInLong = writePosition % 64;
        int lowBitsAvailable = 64 - lowBitInLong;
        long lowValueMask = rightOverflowMask(Math.min(lowBitsAvailable, steps));
        longs[lowLongIndex] |= (value & lowValueMask) << lowBitInLong;
        if (steps > lowBitsAvailable) { // High bits
            long highValueMask = rightOverflowMask(steps - lowBitsAvailable);
            longs[lowLongIndex + 1] |= (value >>> lowBitsAvailable) & highValueMask;
        }
        writePosition += steps;
        return this;
    }

    public BitBuffer put(byte[] bytes, int offset, int length) {
        for (int i = offset; i < offset + length; i++) {
            put(bytes[i], Byte.SIZE);
        }
        return this;
    }

    public boolean available() {
        return readPosition < writePosition;
    }

    public byte getByte() {
        return getByte(Byte.SIZE);
    }

    public byte getByte(int steps) {
        return (byte) getLong(steps);
    }

    public short getShort() {
        return getShort(Short.SIZE);
    }

    public short getShort(int steps) {
        return (short) getLong(steps);
    }

    public int getInt() {
        return getInt(Integer.SIZE);
    }

    public int getInt(int steps) {
        return (int) getLong(steps);
    }

    public long getUnsignedInt() {
        return getInt(Integer.SIZE) & 0xFFFFFFFFL;
    }

    public long getLong() {
        return getLong(Long.SIZE);
    }

    public long getLong(int steps) {
        int lowLongIndex = readPosition >> 6; // 64
        int lowBitInLong = readPosition % 64;
        int lowBitsAvailable = 64 - lowBitInLong;
        long lowLongMask = rightOverflowMask(Math.min(lowBitsAvailable, steps)) << lowBitInLong;
        long lowValue = longs[lowLongIndex] & lowLongMask;
        long result = lowValue >>> lowBitInLong;
        if (steps > lowBitsAvailable) { // High bits
            long highLongMask = rightOverflowMask(steps - lowBitsAvailable);
            result |= (longs[lowLongIndex + 1] & highLongMask) << lowBitsAvailable;
        }
        readPosition += steps;
        return result;
    }

    /**
     * Clear the position and data.
     */
    public void clear(boolean zeroBits) {
        if (zeroBits) {
            // TODO optimize so that only the touched longs gets cleared
            Arrays.fill(longs, 0L);
        }
        readPosition = writePosition = 0;
    }

    /**
     * Given the write position, how many longs are in use.
     */
    public int longsInUse() {
        return ((writePosition - 1) / Long.SIZE) + 1;
    }
}
