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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.PageCursorUtil._2B_MASK;
import static org.neo4j.io.pagecache.PageCursorUtil._3B_MASK;
import static org.neo4j.io.pagecache.PageCursorUtil._6B_MASK;

import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.opentest4j.AssertionFailedError;

@ExtendWith(PageCursorUtilTest.PrintOnFailure.class)
class PageCursorUtilTest {
    private static Random random;
    private static long seed;

    @BeforeEach
    void setUp() {
        seed = System.currentTimeMillis();
        random = new Random(seed);
    }

    @Test
    void shouldPutAndGet6BLongs() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; i++) {
            long expected = random.nextLong() & _6B_MASK;
            cursor.setOffset(0);
            PageCursorUtil.put6BLong(cursor, expected);
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset(0);
            long read = PageCursorUtil.get6BLong(cursor);
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals(expected, read);
            assertTrue(read >= 0);
            assertEquals(0, read & ~_6B_MASK);
            assertEquals(6, offsetAfterWrite);
            assertEquals(6, offsetAfterRead);
        }
    }

    @Test
    void shouldPutAndGet6BLongMinusOneAware() {
        // given
        var cursor = ByteArrayPageCursor.wrap(20);

        // when
        PageCursorUtil.put6BLongMinusOneAware(cursor, -1);
        var value = random.nextLong() & (_6B_MASK - 1);
        PageCursorUtil.put6BLongMinusOneAware(cursor, value);

        // then
        cursor.setOffset(0);
        assertThat(PageCursorUtil.get6BLongMinusOneAware(cursor)).isEqualTo(-1L);
        assertThat(PageCursorUtil.get6BLongMinusOneAware(cursor)).isEqualTo(value);
    }

    @Test
    void shouldPutAndGet3BInt() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; i++) {
            int expected = random.nextInt() & _3B_MASK;
            cursor.setOffset(0);
            PageCursorUtil.put3BInt(cursor, expected);
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset(0);
            int read = PageCursorUtil.get3BInt(cursor);
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals(expected, read);
            assertTrue(read >= 0);
            assertEquals(0, read & ~_3B_MASK);
            assertEquals(3, offsetAfterWrite);
            assertEquals(3, offsetAfterRead);
        }
    }

    @Test
    void shouldPutAndGet3BIntMinusOneAware() {
        // given
        var cursor = ByteArrayPageCursor.wrap(20);

        // when
        PageCursorUtil.put3BIntMinusOneAware(cursor, -1);
        var value = random.nextInt(_3B_MASK - 1);
        PageCursorUtil.put3BIntMinusOneAware(cursor, value);

        // then
        cursor.setOffset(0);
        assertThat(PageCursorUtil.get3BIntMinusOneAware(cursor)).isEqualTo(-1);
        assertThat(PageCursorUtil.get3BIntMinusOneAware(cursor)).isEqualTo(value);
    }

    @Test
    void shouldPutAndGet3BIntAtOffset() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; i++) {
            int expected = random.nextInt() & _3B_MASK;
            cursor.setOffset(0);
            PageCursorUtil.put3BInt(cursor, 1, expected);
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset(0);
            int read = PageCursorUtil.get3BInt(cursor, 1);
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals(expected, read);
            assertTrue(read >= 0);
            assertEquals(0, read & ~_3B_MASK);
            assertEquals(0, offsetAfterWrite);
            assertEquals(0, offsetAfterRead);
        }
    }

    @Test
    void shouldPutAndGetUnsignedShort() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; i++) {
            int expected = random.nextInt() & _2B_MASK;
            cursor.setOffset(0);
            PageCursorUtil.putUnsignedShort(cursor, expected);
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset(0);
            int read = PageCursorUtil.getUnsignedShort(cursor);
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals(expected, read);
            assertTrue(read >= 0);
            assertEquals(0, read & ~_2B_MASK);
            assertEquals(2, offsetAfterWrite);
            assertEquals(2, offsetAfterRead);
        }
    }

    @Test
    void shouldPutAndGetUnsignedShortAtOffset() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; i++) {
            int expected = random.nextInt() & _2B_MASK;
            cursor.setOffset(0);
            PageCursorUtil.putUnsignedShort(cursor, 1, expected);
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset(0);
            int read = PageCursorUtil.getUnsignedShort(cursor, 1);
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals(expected, read);
            assertTrue(read >= 0);
            assertEquals(0, read & ~_2B_MASK);
            assertEquals(0, offsetAfterWrite);
            assertEquals(0, offsetAfterRead);
        }
    }

    @Test
    void shouldFailOnInvalidValues() {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap(10);

        // WHEN
        for (int i = 0; i < 1_000; ) {
            long expected = random.nextLong();
            if ((expected & ~_6B_MASK) != 0) {
                // OK here we have an invalid value
                cursor.setOffset(0);
                assertThrows(IllegalArgumentException.class, () -> PageCursorUtil.put6BLong(cursor, expected));
                i++;
            }
        }
    }

    public static class PrintOnFailure implements TestWatcher {
        @Override
        public void testFailed(ExtensionContext context, Throwable cause) {
            throw new AssertionFailedError(format("%s [ random seed used: %dL ]", cause.getMessage(), seed), cause);
        }
    }
}
