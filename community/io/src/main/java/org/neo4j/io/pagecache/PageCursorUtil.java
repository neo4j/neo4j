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
package org.neo4j.io.pagecache;

import java.io.IOException;

/**
 * {@link PageCursor} functionality commonly used.
 */
public class PageCursorUtil {
    public static final short _1B_MASK = 0xFF;
    public static final int _2B_MASK = 0xFFFF;
    public static final int _3B_MASK = 0xFFFFFF;
    public static final long _4B_MASK = 0xFFFFFFFFL;
    public static final long _6B_MASK = 0xFFFF_FFFFFFFFL;

    private PageCursorUtil() {}

    /**
     * Puts an {@code long}, but only the least significant 6 bytes of it, into the cursor at its current offset.
     * In addition to values 0x0-0xFFFFFFFFFFFE, also value -1 is allowed.
     *
     * @param cursor {@link PageCursor} to put into, at the current offset.
     * @param value the value to put.
     */
    public static void put6BLongMinusOneAware(PageCursor cursor, long value) {
        put6BLong(cursor, value == -1L ? _6B_MASK : value);
    }

    /**
     * Gets 6 bytes from {@code cursor} at given offset and returns that as a {@code long}.
     * If all bits in all 6 bytes are 1's then a value of -1 is returned.
     *
     * @param cursor {@link PageCursor} to get from, at the given offset.
     * @return the 6 bytes as a {@code long}, potentially -1.
     */
    public static long get6BLongMinusOneAware(PageCursor cursor) {
        var value = get6BLong(cursor);
        return value == _6B_MASK ? -1L : value;
    }

    /**
     * Puts the low 6 bytes of the {@code value} into {@code cursor} at current offset.
     * Puts {@link PageCursor#putInt(int) int} followed by {@link PageCursor#putShort(short) short}.
     *
     * @param cursor {@link PageCursor} to put into, at the current offset.
     * @param value the value to put.
     */
    public static void put6BLong(PageCursor cursor, long value) {
        if ((value & ~_6B_MASK) != 0) {
            throw new IllegalArgumentException("Illegal 6B value " + value);
        }

        int lsb = (int) value;
        short msb = (short) (value >>> Integer.SIZE);
        cursor.putInt(lsb);
        cursor.putShort(msb);
    }

    /**
     * Gets 6 bytes from {@code cursor} at current offset and returns that as a {@code long}.
     * Reads {@link PageCursor#getInt()} followed by {@link PageCursor#getShort()}.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @return the 6 bytes as a {@code long}.
     */
    public static long get6BLong(PageCursor cursor) {
        long lsb = getUnsignedInt(cursor);
        long msb = getUnsignedShort(cursor);
        return lsb | (msb << Integer.SIZE);
    }

    /**
     * Puts an {@code int}, but only the least significant 3 bytes of it, into the cursor at its current offset.
     * In addition to values 0x0-0xFFFFFE, also value -1 is allowed.
     *
     * @param cursor {@link PageCursor} to put into, at the current offset.
     * @param value the value to put.
     */
    public static void put3BIntMinusOneAware(PageCursor cursor, int value) {
        if (value < -1 || value >= 0xFFFFFF) {
            throw new IllegalArgumentException("Illegal 3B (minus-one-aware) value " + value);
        }
        put3BInt(cursor, value == -1 ? _3B_MASK : value);
    }

    /**
     * Gets 3 bytes from {@code cursor} at given offset and returns that as an {@code int}.
     * If all bits in all 3 bytes are 1's then a value of -1 is returned.
     *
     * @param cursor {@link PageCursor} to get from, at the given offset.
     * @return the 3 bytes as an {@code int}, potentially -1.
     */
    public static int get3BIntMinusOneAware(PageCursor cursor) {
        var value = get3BInt(cursor);
        return value == _3B_MASK ? -1 : value;
    }

    /**
     * Puts an {@code int}, but only the least significant 3 bytes of it, into the cursor at its current offset.
     *
     * @param cursor {@link PageCursor} to put into, at the current offset.
     * @param value the value to put.
     */
    public static void put3BInt(PageCursor cursor, int value) {
        int offset = cursor.getOffset();
        put3BInt(cursor, offset, value);
        cursor.setOffset(offset + 3);
    }

    /**
     * Puts an {@code int}, but only the least significant 3 bytes of it, into the cursor at the given offset.
     *
     * @param cursor {@link PageCursor} to put into.
     * @param offset offset at which to put the value at.
     * @param value the value to put.
     */
    public static void put3BInt(PageCursor cursor, int offset, int value) {
        if ((value & ~_3B_MASK) != 0) {
            throw new IllegalArgumentException("Illegal 3B value " + value);
        }

        short lsb = (short) value;
        byte msb = (byte) (value >>> Short.SIZE);
        cursor.putShort(offset, lsb);
        cursor.putByte(offset + Short.BYTES, msb);
    }

    /**
     * Gets 3 bytes from {@code cursor} at current offset and returns that as an {@code int}.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @return the 3 bytes as an {@code int}.
     */
    public static int get3BInt(PageCursor cursor) {
        int offset = cursor.getOffset();
        int result = get3BInt(cursor, offset);
        cursor.setOffset(offset + 3);
        return result;
    }

    /**
     * Gets 3 bytes from {@code cursor} at given offset and returns that as an {@code int}.
     *
     * @param cursor {@link PageCursor} to get from, at the given offset.
     * @param offset offset to read from.
     * @return the 3 bytes as an {@code int}.
     */
    public static int get3BInt(PageCursor cursor, int offset) {
        int lsb = getUnsignedShort(cursor, offset);
        int msb = getUnsignedByte(cursor, offset + Short.BYTES);
        return lsb | (msb << Short.SIZE);
    }

    /**
     *  Puts the low 2 bytes of the {@code value} into cursor at current offset.
     *  Puts {@link PageCursor#putShort(short)}.
     *
     * @param cursor {@link PageCursor} to put into, at the current offset.
     * @param value the value to put.
     */
    public static void putUnsignedShort(PageCursor cursor, int value) {
        int offset = cursor.getOffset();
        putUnsignedShort(cursor, offset, value);
        cursor.setOffset(offset + 2);
    }

    /**
     *  Puts the low 2 bytes of the {@code value} into cursor at given offset.
     *  Puts {@link PageCursor#putShort(short)}.
     *
     * @param cursor {@link PageCursor} to put into.
     * @param offset offset into page where to write.
     * @param value the value to put.
     */
    public static void putUnsignedShort(PageCursor cursor, int offset, int value) {
        if ((value & ~_2B_MASK) != 0) {
            throw new IllegalArgumentException("Illegal 2B value " + value);
        }

        cursor.putShort(offset, (short) value);
    }

    /**
     * Gets 2 bytes and returns that value as an {@code int}, ignoring its sign.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @return {@code int} containing the value of the unsigned {@code short}.
     */
    public static int getUnsignedShort(PageCursor cursor) {
        return cursor.getShort() & _2B_MASK;
    }

    /**
     * Gets 2 bytes and returns that value as an {@code int}, ignoring its sign.
     *
     * @param cursor {@link PageCursor} to get from.
     * @param offset offset into page from where to read.
     * @return {@code int} containing the value of the unsigned {@code short}.
     */
    public static int getUnsignedShort(PageCursor cursor, int offset) {
        return cursor.getShort(offset) & _2B_MASK;
    }

    /**
     * Gets 4 bytes and returns that value as an {@code long}, ignoring its sign.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @return {@code long} containing the value of the unsigned {@code int}.
     */
    public static long getUnsignedInt(PageCursor cursor) {
        return cursor.getInt() & _4B_MASK;
    }

    /**
     * Gets 1 byte and returns that value as an {@code int}, ignoring its sign.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @param offset offset into page from where to read.
     * @return {@code int} containing the value of the unsigned {@code byte}.
     */
    public static int getUnsignedByte(PageCursor cursor, int offset) {
        return cursor.getByte(offset) & _1B_MASK;
    }

    /**
     * Gets 1 byte and returns that value as an {@code int}, ignoring its sign.
     *
     * @param cursor {@link PageCursor} to get from, at the current offset.
     * @return {@code int} containing the value of the unsigned {@code byte}.
     */
    public static int getUnsignedByte(PageCursor cursor) {
        return cursor.getByte() & _1B_MASK;
    }

    /**
     * Calls {@link PageCursor#next(long)} with the {@code pageId} and throws {@link IllegalStateException}
     * if that call returns {@code false}.
     * Purpose of this method is to unify exception handling when moving between pages.
     *
     * @param cursor {@link PageCursor} to call {@link PageCursor#next(long)} on.
     * @param messageOnError additional error message to include in exception if {@link PageCursor#next(long)}
     * returned {@code false}, providing more context to the exception message.
     * @param pageId page id to move to.
     * @throws IOException on {@link PageCursor#next(long)} exception.
     */
    public static void goTo(PageCursor cursor, String messageOnError, long pageId) throws IOException {
        if (!cursor.next(pageId)) {
            throw new IllegalStateException("Could not go to page:" + pageId + " [" + messageOnError + "]");
        }
    }
}
