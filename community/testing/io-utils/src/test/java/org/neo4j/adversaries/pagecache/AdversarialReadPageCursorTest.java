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
package org.neo4j.adversaries.pagecache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.test.utils.PageCacheSupport;

class AdversarialReadPageCursorTest {
    @Test
    void shouldNotMessUpUnrelatedSegmentOnReadBytes() throws Exception {
        // Given
        byte[] buf = new byte[4];
        byte[] page = new byte[] {7};
        AdversarialReadPageCursor cursor = new AdversarialReadPageCursor(
                ByteArrayPageCursor.wrap(page),
                new PageCacheSupport.AtomicBooleanInconsistentReadAdversary(new AtomicBoolean(true), new Random()));

        // When
        cursor.next(0);
        cursor.getBytes(buf, buf.length - 1, 1);
        cursor.shouldRetry();
        cursor.getBytes(buf, buf.length - 1, 1);

        // Then the range outside of buf.length-1, buf.length should be pristine
        assertEquals(0, buf[0]);
        assertEquals(0, buf[1]);
        assertEquals(0, buf[2]);
    }
}
