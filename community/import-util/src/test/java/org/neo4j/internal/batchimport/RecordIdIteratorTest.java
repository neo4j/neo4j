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
package org.neo4j.internal.batchimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;

class RecordIdIteratorTest {
    @Test
    void shouldGoPageWiseBackwards() {
        // GIVEN
        RecordIdIterator ids =
                RecordIdIterator.backwards(0, 33, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(
                ids,
                array(30, 31, 32),
                array(20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
                array(10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    void shouldGoPageWiseBackwardsOnCleanBreak() {
        // GIVEN
        RecordIdIterator ids =
                RecordIdIterator.backwards(0, 20, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(ids, array(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    void shouldGoPageWiseBackwardsOnSingleBatch() {
        // GIVEN
        RecordIdIterator ids = RecordIdIterator.backwards(0, 8, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(ids, array(0, 1, 2, 3, 4, 5, 6, 7));
    }

    @Test
    void shouldGoBackwardsToNonZero() {
        // GIVEN
        RecordIdIterator ids =
                RecordIdIterator.backwards(12, 34, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(
                ids,
                array(30, 31, 32, 33),
                array(20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
                array(12, 13, 14, 15, 16, 17, 18, 19));
    }

    @Test
    void shouldGoForwardsWhenStartingFromNonZero() {
        // GIVEN
        RecordIdIterator ids = RecordIdIterator.forwards(1, 12, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(ids, array(1, 2, 3, 4, 5, 6, 7, 8, 9), array(10, 11));
    }

    @Test
    void shouldGoForwardsWhenStartingFromNonZero2() {
        // GIVEN
        RecordIdIterator ids =
                RecordIdIterator.forwards(34, 66, Configuration.withBatchSize(Configuration.DEFAULT, 10));

        // THEN
        assertIds(
                ids,
                array(34, 35, 36, 37, 38, 39),
                array(40, 41, 42, 43, 44, 45, 46, 47, 48, 49),
                array(50, 51, 52, 53, 54, 55, 56, 57, 58, 59),
                array(60, 61, 62, 63, 64, 65));
    }

    private static void assertIds(RecordIdIterator ids, long[]... expectedIds) {
        for (long[] expectedArray : expectedIds) {
            LongIterator iterator = ids.nextBatch();
            assertNotNull(iterator);
            for (long expectedId : expectedArray) {
                assertEquals(expectedId, iterator.next());
            }
            assertFalse(iterator.hasNext());
        }
        assertNull(ids.nextBatch());
    }

    private static long[] array(long... ids) {
        return ids;
    }
}
