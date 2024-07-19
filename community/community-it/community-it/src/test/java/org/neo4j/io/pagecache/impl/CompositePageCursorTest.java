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
package org.neo4j.io.pagecache.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

public class CompositePageCursorTest {
    private static final int PAYLOAD_SIZE = 16;
    private final int PAGE_SIZE = PAYLOAD_SIZE + getReservedBytes();
    private StubPageCursor first;
    private StubPageCursor second;
    private final byte[] bytes = new byte[4];

    private StubPageCursor generatePage(int initialPageId, int pageSize, int initialValue) {
        int reservedBytes = getReservedBytes();
        // test assumes big-endian byte order
        var cursor = new StubPageCursor(
                initialPageId, ByteBuffers.allocate(pageSize, ByteOrder.BIG_ENDIAN, INSTANCE), reservedBytes);
        for (int i = 0; i < pageSize - reservedBytes; i++) {
            cursor.putByte(i, (byte) (initialValue + i));
        }
        return cursor;
    }

    protected int getReservedBytes() {
        return 0;
    }

    @BeforeEach
    void setUp() {
        first = generatePage(0, PAGE_SIZE, 0xA0);
        second = generatePage(2, PAGE_SIZE + 8, 0xB0);
    }

    @Test
    void getByteMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        assertThat(c.getByte()).isEqualTo((byte) 0xA0);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getByteMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        assertThat(c.getByte()).isEqualTo((byte) 0xA0);
        assertThat(c.getByte()).isEqualTo((byte) 0xB0);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getByteMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        assertThat(c.getByte()).isEqualTo((byte) 0xA1);
        assertThat(c.getByte()).isEqualTo((byte) 0xB0);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getByteMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        assertThat(c.getByte()).isEqualTo((byte) 0xA0);
        assertThat(c.getByte()).isEqualTo((byte) 0xB1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putByteMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        c.putByte((byte) 1);
        c.setOffset(0);
        assertThat(c.getByte()).isEqualTo((byte) 1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putByteMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        c.putByte((byte) 1);
        c.putByte((byte) 2);
        c.setOffset(1);
        assertThat(c.getByte()).isEqualTo((byte) 2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putByteMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 1);
        c.putByte((byte) 1);
        assertThat(first.getByte(1)).isEqualTo((byte) 1);
        assertThat(c.getByte()).isEqualTo((byte) 0xB0);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putByteMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 2);
        c.putByte((byte) 1);
        c.putByte((byte) 2);
        assertThat(second.getByte(1)).isEqualTo((byte) 2);
        assertThat(c.getByte()).isEqualTo((byte) 0xB2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getByteWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 1 + 1, second, 1);
        assertThat(c.getByte(1)).isEqualTo((byte) 0xA2);
        assertThat(c.getByte(1 + 1)).isEqualTo((byte) 0xB2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putByteWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 2 * 1, second, 2 * 1);
        c.putByte(1, (byte) 1);
        c.putByte(1 + 1, (byte) 2);
        assertThat(c.getByte()).isEqualTo((byte) 0xA1);
        assertThat(c.getByte()).isEqualTo((byte) 1);
        assertThat(c.getByte()).isEqualTo((byte) 2);
        assertThat(c.getByte()).isEqualTo((byte) 0xB3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getShortMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        assertThat(c.getShort()).isEqualTo((short) 0xA0A1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getShortMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        assertThat(c.getShort()).isEqualTo((short) 0xA0A1);
        assertThat(c.getShort()).isEqualTo((short) 0xB0B1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getShortMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        assertThat(c.getShort()).isEqualTo((short) 0xA1A2);
        assertThat(c.getShort()).isEqualTo((short) 0xB0B1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getShortMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        assertThat(c.getShort()).isEqualTo((short) 0xA0A1);
        assertThat(c.getShort()).isEqualTo((short) 0xB1B2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putShortMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        c.putShort((short) 1);
        c.setOffset(0);
        assertThat(c.getShort()).isEqualTo((short) 1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putShortMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        c.putShort((short) 1);
        c.putShort((short) 2);
        c.setOffset(2);
        assertThat(c.getShort()).isEqualTo((short) 2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putShortMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 2, second, 2);
        c.putShort((short) 1);
        assertThat(first.getShort(1)).isEqualTo((short) 1);
        assertThat(c.getShort()).isEqualTo((short) 0xB0B1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putShortMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 2, second, 4);
        c.putShort((short) 1);
        c.putShort((short) 2);
        assertThat(second.getShort(1)).isEqualTo((short) 2);
        assertThat(c.getShort()).isEqualTo((short) 0xB3B4);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getShortWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 1 + 2, second, 2);
        assertThat(c.getShort(1)).isEqualTo((short) 0xA2A3);
        assertThat(c.getShort(1 + 2)).isEqualTo((short) 0xB2B3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putShortWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 2 * 2, second, 2 * 2);
        c.putShort(2, (short) 1);
        c.putShort(2 + 2, (short) 2);
        assertThat(c.getShort()).isEqualTo((short) 0xA1A2);
        assertThat(c.getShort()).isEqualTo((short) 1);
        assertThat(c.getShort()).isEqualTo((short) 2);
        assertThat(c.getShort()).isEqualTo((short) 0xB4B5);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getIntMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        assertThat(c.getInt()).isEqualTo(0xA0A1A2A3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getIntMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        assertThat(c.getInt()).isEqualTo(0xA0A1A2A3);
        assertThat(c.getInt()).isEqualTo(0xB0B1B2B3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getIntMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        assertThat(c.getInt()).isEqualTo(0xA1A2A3A4);
        assertThat(c.getInt()).isEqualTo(0xB0B1B2B3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getIntMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        assertThat(c.getInt()).isEqualTo(0xA0A1A2A3);
        assertThat(c.getInt()).isEqualTo(0xB1B2B3B4);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putIntMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.putInt(1);
        c.setOffset(0);
        assertThat(c.getInt()).isEqualTo(1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putIntMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.putInt(1);
        c.putInt(2);
        c.setOffset(4);
        assertThat(c.getInt()).isEqualTo(2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putIntMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.putInt(1);
        assertThat(first.getInt(1)).isEqualTo(1);
        assertThat(c.getInt()).isEqualTo(0xB0B1B2B3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putIntMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 8);
        c.putInt(1);
        c.putInt(2);
        assertThat(second.getInt(1)).isEqualTo(2);
        assertThat(c.getInt()).isEqualTo(0xB5B6B7B8);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getIntWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 1 + 4, second, 4);
        assertThat(c.getInt(1)).isEqualTo(0xA2A3A4A5);
        assertThat(c.getInt(1 + 4)).isEqualTo(0xB2B3B4B5);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putIntWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 2 * 4, second, 2 * 4);
        c.putInt(4, 1);
        c.putInt(4 + 4, 2);
        assertThat(c.getInt()).isEqualTo(0xA1A2A3A4);
        assertThat(c.getInt()).isEqualTo(1);
        assertThat(c.getInt()).isEqualTo(2);
        assertThat(c.getInt()).isEqualTo(0xB6B7B8B9);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getLongMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        assertThat(c.getLong()).isEqualTo(0xA0A1A2A3A4A5A6A7L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getLongMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        assertThat(c.getLong()).isEqualTo(0xA0A1A2A3A4A5A6A7L);
        assertThat(c.getLong()).isEqualTo(0xB0B1B2B3B4B5B6B7L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getLongMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        assertThat(c.getLong()).isEqualTo(0xA1A2A3A4A5A6A7A8L);
        assertThat(c.getLong()).isEqualTo(0xB0B1B2B3B4B5B6B7L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getLongMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        assertThat(c.getLong()).isEqualTo(0xA0A1A2A3A4A5A6A7L);
        assertThat(c.getLong()).isEqualTo(0xB1B2B3B4B5B6B7B8L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putLongMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        c.putLong(1);
        c.setOffset(0);
        assertThat(c.getLong()).isEqualTo(1);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putLongMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        c.putLong(1);
        c.putLong(2);
        c.setOffset(8);
        assertThat(c.getLong()).isEqualTo(2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putLongMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 8, second, 8);
        c.putLong(1);
        assertThat(first.getLong(1)).isEqualTo(1);
        assertThat(c.getLong()).isEqualTo(0xB0B1B2B3B4B5B6B7L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putLongMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 8, second, PAYLOAD_SIZE);
        c.putLong(1);
        c.putLong(2);
        assertThat(second.getLong(1)).isEqualTo(2);
        assertThat(c.getLong()).isEqualTo(0xB9BABBBCBDBEBFC0L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getLongWithOffsetMustHitCorrectCursors() {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 1 + 8, second, 8);
        assertThat(c.getLong(1)).isEqualTo(0xA2A3A4A5A6A7A8A9L);
        assertThat(c.getLong(1 + 8)).isEqualTo(0xB2B3B4B5B6B7B8B9L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putLongWithOffsetMustHitCorrectCursors() {
        first = generatePage(0, PAGE_SIZE + 8, 0xA0);
        second = generatePage(0, PAGE_SIZE + 8, 0xC0);
        first.setOffset(1);
        second.setOffset(2);
        PageCursor c = CompositePageCursor.compose(first, 2 * 8, second, 2 * 8);
        c.putLong(8, 1);
        c.putLong(8 + 8, 2);
        assertThat(c.getLong()).isEqualTo(0xA1A2A3A4A5A6A7A8L);
        assertThat(c.getLong()).isEqualTo(1);
        assertThat(c.getLong()).isEqualTo(2);
        assertThat(c.getLong()).isEqualTo(0xCACBCCCDCECFD0D1L);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getBytesMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xA0, 0xA1, 0xA2, 0xA3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getBytesMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xA0, 0xA1, 0xA2, 0xA3);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xB0, 0xB1, 0xB2, 0xB3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getBytesMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xA1, 0xA2, 0xA3, 0xA4);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xB0, 0xB1, 0xB2, 0xB3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void getBytesMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xA0, 0xA1, 0xA2, 0xA3);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xB1, 0xB2, 0xB3, 0xB4);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putBytesMustHitFirstCursorBeforeFlip() {
        PageCursor c = CompositePageCursor.compose(first, 4, second, 4);
        c.putBytes(new byte[] {1, 2, 3, 4});
        c.setOffset(0);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(1, 2, 3, 4);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putBytesMustHitSecondCursorAfterFlip() {
        PageCursor c = CompositePageCursor.compose(first, 1, second, 4);
        c.putBytes(new byte[] {1});
        c.putBytes(new byte[] {2});
        c.setOffset(1);
        c.getBytes(bytes);
        assertThat(Arrays.copyOfRange(bytes, 0, 1)).containsExactly(2);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putBytesMustRespectOffsetIntoFirstCursor() {
        first.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 4);
        c.putBytes(new byte[] {1});
        first.setOffset(1);
        first.getBytes(bytes);
        assertThat(Arrays.copyOfRange(bytes, 0, 1)).containsExactly(1);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xB0, 0xB1, 0xB2, 0xB3);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void putBytesMustRespectOffsetIntoSecondCursor() {
        second.setOffset(1);
        PageCursor c = CompositePageCursor.compose(first, 1, second, 8);
        c.putBytes(new byte[] {1});
        c.putBytes(new byte[] {2});
        second.setOffset(1);
        second.getBytes(bytes);
        assertThat(Arrays.copyOfRange(bytes, 0, 1)).containsExactly(2);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xB5, 0xB6, 0xB7, 0xB8);
        assertFalse(c.checkAndClearBoundsFlag());
    }

    @Test
    void overlappingGetAccess() {
        PageCursor c = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        c.setOffset(PAYLOAD_SIZE - 2);
        assertThat(c.getInt()).isEqualTo(0xAEAFB0B1);
        c.setOffset(PAYLOAD_SIZE - 1);
        assertThat(c.getShort()).isEqualTo((short) 0xAFB0);
        c.setOffset(PAYLOAD_SIZE - 4);
        assertThat(c.getLong()).isEqualTo(0xACADAEAFB0B1B2B3L);
        c.setOffset(PAYLOAD_SIZE - 2);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0xAE, 0xAF, 0xB0, 0xB1);
    }

    @Test
    void overlappingOffsetGetAccess() {
        PageCursor c = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        assertThat(c.getInt(PAYLOAD_SIZE - 2)).isEqualTo(0xAEAFB0B1);
        assertThat(c.getShort(PAYLOAD_SIZE - 1)).isEqualTo((short) 0xAFB0);
        assertThat(c.getLong(PAYLOAD_SIZE - 4)).isEqualTo(0xACADAEAFB0B1B2B3L);
    }

    @Test
    void overlappingPutAccess() {
        PageCursor c = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        c.setOffset(PAYLOAD_SIZE - 2);
        c.putInt(0x01020304);
        c.setOffset(PAYLOAD_SIZE - 2);
        assertThat(c.getInt()).isEqualTo(0x01020304);

        c.setOffset(PAYLOAD_SIZE - 1);
        c.putShort((short) 0x0102);
        c.setOffset(PAYLOAD_SIZE - 1);
        assertThat(c.getShort()).isEqualTo((short) 0x0102);

        c.setOffset(PAYLOAD_SIZE - 4);
        c.putLong(0x0102030405060708L);
        c.setOffset(PAYLOAD_SIZE - 4);
        assertThat(c.getLong()).isEqualTo(0x0102030405060708L);

        c.setOffset(PAYLOAD_SIZE - 2);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i + 1);
        }
        c.putBytes(bytes);
        c.setOffset(PAYLOAD_SIZE - 2);
        c.getBytes(bytes);
        assertThat(bytes).containsExactly(0x01, 0x02, 0x03, 0x04);
    }

    @Test
    void overlappingOffsetPutAccess() {
        PageCursor c = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        c.putInt(PAYLOAD_SIZE - 2, 0x01020304);
        assertThat(c.getInt(PAYLOAD_SIZE - 2)).isEqualTo(0x01020304);

        c.putShort(PAYLOAD_SIZE - 1, (short) 0x0102);
        assertThat(c.getShort(PAYLOAD_SIZE - 1)).isEqualTo((short) 0x0102);

        c.putLong(PAYLOAD_SIZE - 4, 0x0102030405060708L);
        assertThat(c.getLong(PAYLOAD_SIZE - 4)).isEqualTo(0x0102030405060708L);
    }

    @Test
    void closeBothCursorsOnClose() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.close();

        assertTrue(first.isClosed());
        assertTrue(second.isClosed());
    }

    @Test
    void nextIsNotSupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () -> {
            PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
            pageCursor.next();
        });
    }

    @Test
    void nextWithPageIdIsNotSupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () -> {
            PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
            pageCursor.next(12);
        });
    }

    @Test
    public void markCompositeCursor() {
        // GIVEN
        first.setOffset(1);
        second.setOffset(2);
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);

        first.getByte();
        second.getLong();

        int firstMark = first.getOffset();
        int secondMark = second.getOffset();
        pageCursor.mark();

        first.getByte();
        second.getLong();

        assertNotEquals(firstMark, first.getOffset());
        assertNotEquals(secondMark, second.getOffset());

        // WHEN
        pageCursor.setOffsetToMark();

        // THEN
        assertEquals(firstMark, first.getOffset());
        assertEquals(secondMark, second.getOffset());
    }

    @Test
    void getOffsetMustReturnOffsetIntoView() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.getLong();
        assertThat(pageCursor.getOffset()).isEqualTo(8);
        pageCursor.getLong();
        pageCursor.getLong();
        assertThat(pageCursor.getOffset()).isEqualTo(24);
    }

    @Test
    void setOffsetMustSetOffsetIntoView() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.setOffset(13);
        assertThat(first.getOffset()).isEqualTo(13);
        assertThat(second.getOffset()).isEqualTo(0);
        pageCursor.setOffset(18); // beyond first page cursor
        assertThat(first.getOffset()).isEqualTo(PAYLOAD_SIZE);
        assertThat(second.getOffset()).isEqualTo(18 - PAYLOAD_SIZE);
    }

    @Test
    void pageIdEqualFirstCursorPageIdBeforeFlip() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        assertEquals(first.getCurrentPageId(), pageCursor.getCurrentPageId());

        pageCursor.getLong();
        assertEquals(first.getCurrentPageId(), pageCursor.getCurrentPageId());

        pageCursor.getLong();
        assertNotEquals(first.getCurrentPageId(), pageCursor.getCurrentPageId());
    }

    @Test
    void pageIdEqualSecondCursorPageIdAfterFlip() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        assertNotEquals(second.getCurrentPageId(), pageCursor.getCurrentPageId());

        pageCursor.getLong();
        assertNotEquals(second.getCurrentPageId(), pageCursor.getCurrentPageId());

        pageCursor.getLong();
        assertEquals(second.getCurrentPageId(), pageCursor.getCurrentPageId());
    }

    @Test
    void retryShouldCheckAndResetBothCursors() throws Exception {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);

        assertFalse(pageCursor.shouldRetry());

        first.setNeedsRetry(true);
        assertTrue(pageCursor.shouldRetry());

        first.setNeedsRetry(false);
        assertFalse(pageCursor.shouldRetry());

        second.setNeedsRetry(true);
        assertTrue(pageCursor.shouldRetry());
    }

    @Test
    void retryMustResetOffsetsInBothCursors() throws Exception {
        first.setOffset(1);
        second.setOffset(2);
        PageCursor pageCursor = CompositePageCursor.compose(first, 8, second, 8);

        pageCursor.setOffset(5);
        first.setOffset(3);
        second.setOffset(4);
        first.setNeedsRetry(true);
        pageCursor.shouldRetry();
        assertThat(first.getOffset()).isEqualTo(1);
        assertThat(second.getOffset()).isEqualTo(2);
        assertThat(pageCursor.getOffset()).isEqualTo(0);

        pageCursor.setOffset(5);
        first.setOffset(3);
        second.setOffset(4);
        first.setNeedsRetry(false);
        second.setNeedsRetry(true);
        pageCursor.shouldRetry();
        assertThat(first.getOffset()).isEqualTo(1);
        assertThat(second.getOffset()).isEqualTo(2);
        assertThat(pageCursor.getOffset()).isEqualTo(0);
    }

    @Test
    void retryMustClearTheOutOfBoundsFlags() throws Exception {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.getByte(-1); // Do an out of bounds get
        second.getByte(-1); // Do an out of bounds get
        pageCursor.getByte(-1); // Do an out of bounds get
        first.setNeedsRetry(true);
        pageCursor.shouldRetry();
        assertFalse(first.checkAndClearBoundsFlag());
        assertFalse(second.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void checkAndClearCompositeBoundsFlagMustClearFirstBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.getByte(-1); // Do an out of bounds get
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
        assertFalse(first.checkAndClearBoundsFlag());
    }

    @Test
    void checkAndClearCompositeBoundsFlagMustClearSecondBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        second.getByte(-1); // Do an out of bounds get
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
        assertFalse(second.checkAndClearBoundsFlag());
    }

    @Test
    void composeMustNotThrowIfFirstLengthExpandsBeyondFirstPage() {
        CompositePageCursor.compose(first, Integer.MAX_VALUE, second, PAYLOAD_SIZE);
    }

    @Test
    void composeMustNotThrowIfSecondLengthExpandsBeyondSecondPage() {
        CompositePageCursor.compose(first, PAYLOAD_SIZE, second, Integer.MAX_VALUE);
    }

    @Test
    void compositeCursorDoesNotSupportCopyTo() {
        assertThrows(UnsupportedOperationException.class, () -> {
            PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
            pageCursor.copyTo(0, new StubPageCursor(0, 7), 89, 6);
        });
    }

    @Test
    void compositeCursorDoesNotSupportCopyPage() {
        assertThrows(UnsupportedOperationException.class, () -> {
            PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
            pageCursor.copyPage(new StubPageCursor(0, 1));
        });
    }

    @Test
    void getByteBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAGE_SIZE; i++) {
            pageCursor.getByte();
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getByteOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getByte(i);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putByteBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putByte((byte) 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putByteOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putByte(i, (byte) 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getByteOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.getByte(-1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putByteOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.putByte(-1, (byte) 1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getShortBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getShort();
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getShortOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getShort(i);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putShortBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putShort((short) 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putShortOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putShort(i, (short) 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getShortOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.getShort(-1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putShortOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.putShort(-1, (short) 1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getIntBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getInt();
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getIntOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getInt(i);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putIntBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putInt(1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putIntOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putInt(i, 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getIntOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.getInt(-1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putIntOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.putInt(-1, 1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getLongBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getLong();
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getLongOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getLong(i);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putLongBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putLong(1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putLongOffsetBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putLong(i, 1);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getLongOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.getLong(-1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putLongOffsetBeforeFirstPageMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        pageCursor.putLong(-1, 1);
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void getByteArrayBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.getBytes(bytes);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void putByteArrayBeyondEndOfViewMustRaiseBoundsFlag() {
        PageCursor pageCursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        for (int i = 0; i < 3 * PAYLOAD_SIZE; i++) {
            pageCursor.putBytes(bytes);
        }
        assertTrue(pageCursor.checkAndClearBoundsFlag());
        assertFalse(pageCursor.checkAndClearBoundsFlag());
    }

    @Test
    void setCursorErrorMustApplyToCursorAtCurrentOffset() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        String firstMsg = "first boo";
        String secondMsg = "second boo";

        cursor.setCursorException(firstMsg);
        assertFalse(cursor.checkAndClearBoundsFlag());
        try {
            first.checkAndClearCursorException();
            fail("first checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo(firstMsg);
        }

        cursor.setOffset(PAYLOAD_SIZE);
        cursor.setCursorException(secondMsg);
        assertFalse(cursor.checkAndClearBoundsFlag());
        try {
            second.checkAndClearCursorException();
            fail("second checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo(secondMsg);
        }
    }

    @Test
    void checkAndClearCursorErrorMustNotThrowIfNoErrorsAreSet() throws Exception {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        cursor.checkAndClearCursorException();
    }

    @Test
    void checkAndClearCursorErrorMustThrowIfFirstCursorHasError() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setCursorException("boo");
        try {
            cursor.checkAndClearCursorException();
            fail("composite cursor checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo("boo");
        }
    }

    @Test
    void checkAndClearCursorErrorMustThrowIfSecondCursorHasError() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        second.setCursorException("boo");
        try {
            cursor.checkAndClearCursorException();
            fail("composite cursor checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo("boo");
        }
    }

    @Test
    void checkAndClearCursorErrorWillOnlyCheckFirstCursorIfBothHaveErrorsSet() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setCursorException("first boo");
        second.setCursorException("second boo");
        try {
            cursor.checkAndClearCursorException();
            fail("composite cursor checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo("first boo");
        }
        try {
            second.checkAndClearCursorException();
            fail("second cursor checkAndClearCursorError should have thrown");
        } catch (CursorException e) {
            assertThat(e.getMessage()).isEqualTo("second boo");
        }
    }

    @Test
    void clearCursorErrorMustClearBothCursors() throws Exception {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setCursorException("first boo");
        second.setCursorException("second boo");
        cursor.clearCursorException();

        // Now these must not throw
        first.checkAndClearCursorException();
        second.checkAndClearCursorException();
        cursor.checkAndClearCursorException();
    }

    @Test
    void isWriteLockedMustBeTrueIfBothCursorsAreWriteLocked() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setWriteLocked(true);
        second.setWriteLocked(true);
        assertTrue(cursor.isWriteLocked());
    }

    @Test
    void isWriteLockedMustBeFalseIfBothCursorsAreNotWriteLocked() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setWriteLocked(false);
        second.setWriteLocked(false);
        assertFalse(cursor.isWriteLocked());
    }

    @Test
    void isWriteLockedMustBeFalseIfFirstCursorIsNotWriteLocked() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setWriteLocked(false);
        second.setWriteLocked(true);
        assertFalse(cursor.isWriteLocked());
    }

    @Test
    void isWriteLockedMustBeFalseIfSecondCursorIsNotWriteLocked() {
        PageCursor cursor = CompositePageCursor.compose(first, PAYLOAD_SIZE, second, PAYLOAD_SIZE);
        first.setWriteLocked(true);
        second.setWriteLocked(false);
        assertFalse(cursor.isWriteLocked());
    }
}
