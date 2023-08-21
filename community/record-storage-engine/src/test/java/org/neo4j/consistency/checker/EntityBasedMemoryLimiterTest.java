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
package org.neo4j.consistency.checker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;

class EntityBasedMemoryLimiterTest {
    @Test
    void shouldReturnTheWholeRangeIfItFits() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(50, 1, 40, 40);
        assertEquals(1, limiter.numberOfRanges());

        // when
        EntityBasedMemoryLimiter.CheckRange range = limiter.next();

        // then
        assertRange(range, 0, 40, 40);
        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldHandleRangeWithHighNodeLessThanHighRel() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(50, 1, 20, 40);
        // The ranges are based on highNodeId - a range will never be larger than the number of nodes in the db.
        assertEquals(2, limiter.numberOfRanges());

        // when/then
        assertRange(limiter.next(), 0, 20, 20);

        EntityBasedMemoryLimiter.CheckRange range = limiter.next();
        assertFalse(range.applicableForNodeBasedChecks());
        assertNull(range.getNodeRange());
        assertTrue(range.applicableForRelationshipBasedChecks());
        assertRange(range.getRelationshipRange(), 20, 40);

        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldHandleRangeWithHighRelLessThanHighNodeForOneWholeRange() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(50, 1, 40, 20);
        assertEquals(1, limiter.numberOfRanges());

        // when
        EntityBasedMemoryLimiter.CheckRange range = limiter.next();

        // then
        assertRange(range, 0, 40, 20);
        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldReturnMultipleRangesIfWholeRangeDontFit() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(800, 10, 200, 200);
        assertEquals(3, limiter.numberOfRanges());

        // when/then
        assertRange(limiter.next(), 0, 80, 80);
        assertRange(limiter.next(), 80, 160, 160);
        assertRange(limiter.next(), 160, 200, 200);
        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldReturnMultipleRangesIfWholeRangeDontFitHighRelLessThanHighNode() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(800, 10, 200, 100);
        assertEquals(3, limiter.numberOfRanges());

        // when/then
        assertRange(limiter.next(), 0, 80, 80);
        assertRange(limiter.next(), 80, 160, 100);

        EntityBasedMemoryLimiter.CheckRange range = limiter.next();
        assertTrue(range.applicableForNodeBasedChecks());
        assertRange(range.getNodeRange(), 160, 200);
        assertFalse(range.applicableForRelationshipBasedChecks());
        assertNull(range.getRelationshipRange());

        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldReturnMultipleRangesIfWholeRangeDontFitHighNodeLessThanHighRel() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(800, 10, 100, 200);
        assertEquals(3, limiter.numberOfRanges());

        // when/then
        assertRange(limiter.next(), 0, 80, 80);
        assertRange(limiter.next(), 80, 100, 160);

        EntityBasedMemoryLimiter.CheckRange range = limiter.next();
        assertFalse(range.applicableForNodeBasedChecks());
        assertNull(range.getNodeRange());
        assertTrue(range.applicableForRelationshipBasedChecks());
        assertRange(range.getRelationshipRange(), 160, 200);

        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldReturnMultipleRangesIfWholeRangeDontFitWithLeeway() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(220, 25, 10, 10);
        assertEquals(2, limiter.numberOfRanges());

        // when/then
        assertRange(limiter.next(), 0, 8, 8);
        assertRange(limiter.next(), 8, 10, 10);
        assertFalse(limiter.hasNext());
    }

    @Test
    void shouldReturnCorrectNumberOfRangesOnExactMatch() {
        // given
        EntityBasedMemoryLimiter limiter = new EntityBasedMemoryLimiter(10, 1, 100, 100);

        // then
        assertEquals(10, limiter.numberOfRanges());
    }

    private static void assertRange(LongRange range, long from, long to) {
        assertEquals(from, range.from());
        assertEquals(to, range.to());
    }

    private static void assertRange(
            EntityBasedMemoryLimiter.CheckRange range, long from, long toNode, long toRelationship) {
        assertTrue(range.applicableForNodeBasedChecks());
        assertTrue(range.applicableForRelationshipBasedChecks());
        assertRange(range.getNodeRange(), from, toNode);
        assertRange(range.getRelationshipRange(), from, toRelationship);
    }
}
