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
package org.neo4j.storageengine.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.util.stream.IntStream;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.memory.EmptyMemoryTracker;

class DirectedTypesTest {

    private static final EmptyMemoryTracker NO_TRACKING = EmptyMemoryTracker.INSTANCE;

    private static final Direction[] kernelDirections =
            new Direction[] {Direction.OUTGOING, Direction.INCOMING, Direction.BOTH};

    @Test
    void untypedComputeDirection() {
        for (Direction dir : kernelDirections) {
            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addUntyped(dir);

            assertEquals(dir, dt.computeDirection());
        }
    }

    @Test
    void typedComputeDirection() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addTypes(new int[] {1}, dir);

            assertEquals(dir, dt.computeDirection());
        }
    }

    @Test
    void untypedOutgoingIncludesSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertTrue(dt.hasOutgoing(1));
    }

    @Test
    void untypedOutgoingIncludesAllOutgoing() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertTrue(dt.allowsAllOutgoing());
    }

    @Test
    void untypedOutgoingDoesNotIncludeIncoming() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertFalse(dt.allowsAllIncoming());
        assertFalse(dt.hasIncoming(1));
        assertFalse(dt.hasSomeIncoming());
    }

    @Test
    void untypedOutgoingDoesNotHaveBothDirections() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertFalse(dt.hasTypesInBothDirections());
    }

    @Test
    void untypedOverridesPreviouslyAddedSpecificType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1}, Direction.OUTGOING);
        dt.addUntyped(Direction.OUTGOING);

        assertEquals(1, dt.numberOfCriteria());
    }

    @Test
    void specificCriterionType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertEquals(99, dt.criterionType(0));
    }

    @Test
    void untypedCriterionType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertEquals(ANY_RELATIONSHIP_TYPE, dt.criterionType(1));
    }

    @Test
    void specificCriterionDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertEquals(2, dt.numberOfCriteria());

        var specificCriterionDirection = IntStream.range(0, dt.numberOfCriteria())
                .filter(i -> dt.criterionType(i) == 99)
                .mapToObj(dt::criterionDirection)
                .findFirst()
                .get();

        assertEquals(Direction.OUTGOING, specificCriterionDirection);
    }

    @Test
    void specificCriterionDirectionBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.BOTH);
        dt.addUntyped(Direction.INCOMING);

        assertEquals(2, dt.numberOfCriteria());

        var specificCriterionDirection = IntStream.range(0, dt.numberOfCriteria())
                .filter(i -> dt.criterionType(i) == 99)
                .mapToObj(dt::criterionDirection)
                .findFirst()
                .get();

        assertEquals(Direction.BOTH, specificCriterionDirection);
    }

    @Test
    void untypedCriterionDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertEquals(Direction.INCOMING, dt.criterionDirection(1));
    }

    @Test
    void typedInOneDirectionAndUntypedInOtherDirectionIsBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertTrue(dt.hasOutgoing(99));
        assertTrue(dt.hasIncoming(99));
    }

    @Test
    void typeWildcardsAreOneCriterion() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addUntyped(dir);

            assertEquals(1, dt.numberOfCriteria());
        }
    }

    @Test
    void addingRedundantTypeDoesntChangeNumberOfCriteria() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addUntyped(dir);
            dt.addTypes(new int[] {1}, dir);

            assertEquals(1, dt.numberOfCriteria());
        }
    }

    @Test
    void writeModeResetsSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {1}, Direction.OUTGOING);

        assertTrue(dt.hasOutgoing(1));

        dt.clear();
        dt.addTypes(new int[] {1}, Direction.INCOMING);

        assertFalse(dt.hasOutgoing(1));
        assertTrue(dt.hasIncoming(1));
    }

    @Test
    void writeModeResetsUntyped() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        assertTrue(dt.hasOutgoing(1));

        dt.clear();
        dt.addUntyped(Direction.INCOMING);

        assertFalse(dt.hasOutgoing(1));
        assertTrue(dt.hasIncoming(1));
    }

    @Test
    void duplicatesAreIgnored() {
        for (Direction dir : kernelDirections) {
            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addTypes(new int[] {1, 1}, dir);
            dt.addTypes(new int[] {1}, dir);

            assertEquals(1, dt.numberOfCriteria());
            int[] types = dt.typesWithoutDirections();
            assertNotEquals(null, types);
            assertEquals(1, types.length);
            assertEquals(1, types[0]);
        }
    }

    @Test
    void duplicatesIncomingAndOutgoingAreMergedAsBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 1}, Direction.INCOMING);
        dt.addTypes(new int[] {1}, Direction.OUTGOING);

        assertEquals(Direction.BOTH, dt.computeDirection());
        assertTrue(dt.hasTypesInBothDirections());
        assertTrue(dt.hasIncoming(1));
        assertTrue(dt.hasOutgoing(1));

        assertEquals(1, dt.numberOfCriteria());
        int[] types = dt.typesWithoutDirections();
        assertNotEquals(null, types);
        assertEquals(1, types.length);
        assertEquals(1, types[0]);
    }

    @Test
    void reverseSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        var reversed = dt.reverse();
        assertEquals(Direction.INCOMING, reversed.computeDirection());
        assertTrue(reversed.hasSomeIncoming());
        assertTrue(reversed.allowsAllIncoming());
        assertFalse(reversed.hasSomeOutgoing());
        assertFalse(reversed.allowsAllOutgoing());

        var rereversed = reversed.reverse();
        assertEquals(Direction.OUTGOING, rereversed.computeDirection());
        assertTrue(rereversed.hasSomeOutgoing());
        assertTrue(rereversed.allowsAllOutgoing());
        assertFalse(rereversed.hasSomeIncoming());
        assertFalse(rereversed.allowsAllIncoming());
    }

    @Test
    void reverseAll() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        var reversed = dt.reverse();

        assertEquals(Direction.INCOMING, reversed.computeDirection());
        assertEquals(1, reversed.numberOfCriteria());
        assertFalse(reversed.allowsAll());
        assertTrue(reversed.allowsAllIncoming());
    }

    @Test
    void reverseBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.BOTH);

        var reversed = dt.reverse();

        assertEquals(1, reversed.numberOfCriteria());
        assertTrue(reversed.allowsAll());
    }

    @Test
    void untypedIncomingAndOutgoingMergesIntoBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.INCOMING);
        dt.addUntyped(Direction.OUTGOING);

        assertTrue(dt.allowsAll());
    }

    @Test
    void untypedBothIncludesEverything() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.BOTH);

        assertTrue(dt.hasOutgoing(1));
        assertTrue(dt.hasSomeOutgoing());
        assertTrue(dt.allowsAllOutgoing());

        assertTrue(dt.hasIncoming(2));
        assertTrue(dt.hasSomeIncoming());
        assertTrue(dt.allowsAllIncoming());
    }

    @Test
    void whenNotTypeLimitedThenTypesWithoutDirectionsShouldBeNull() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1}, Direction.OUTGOING);
        dt.addUntyped(Direction.OUTGOING);

        assertEquals(null, dt.typesWithoutDirections());
    }

    @Test
    void complicatedCompaction() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 2}, Direction.OUTGOING);
        dt.addTypes(new int[] {1, 2}, Direction.INCOMING);
        dt.addTypes(new int[] {3}, Direction.INCOMING);
        dt.addTypes(new int[] {4}, Direction.OUTGOING);
        dt.addTypes(new int[] {7, 8, 9}, Direction.INCOMING);
        dt.addTypes(new int[] {5, 6}, Direction.BOTH);
        dt.addTypes(new int[] {1, 2}, Direction.BOTH);
        dt.addUntyped(Direction.INCOMING);

        dt.compact();

        // Expected types in directions
        // ANY_RELATIONSHIP_TYPE - INCOMING
        // 1 - BOTH
        // 2 - BOTH
        // 4 - OUTGOING
        // 5 - BOTH
        // 6 - BOTH

        int numberOfOutgoingCriteria = 1;
        int numberOfIncomingCriteria = 1;
        int numberOfUndirectedCriteria = 4;

        int noCriteria = numberOfOutgoingCriteria + numberOfIncomingCriteria + numberOfUndirectedCriteria;
        assertEquals(noCriteria, dt.numberOfCriteria());

        IntArrayList outgoing = new IntArrayList();
        IntArrayList both = new IntArrayList();
        for (int i = 0; i < noCriteria; i++) {
            int type = dt.criterionType(i);
            Direction dir = dt.criterionDirection(i);
            switch (dir) {
                case OUTGOING -> outgoing.add(type);
                case BOTH -> both.add(type);
                case INCOMING -> assertEquals(ANY_RELATIONSHIP_TYPE, type);
            }
        }
        outgoing.sortThis();
        both.sortThis();

        assertEquals(numberOfOutgoingCriteria, outgoing.size());
        assertEquals(numberOfUndirectedCriteria, both.size());

        IntArrayList expectedOutgoing = new IntArrayList(new int[] {4});
        IntArrayList expectedBoth = new IntArrayList(1, 2, 5, 6);

        for (int i = 0; i < numberOfOutgoingCriteria; i++) {
            assertEquals(outgoing.get(i), expectedOutgoing.get(i));
        }
        for (int i = 0; i < numberOfUndirectedCriteria; i++) {
            assertEquals(both.get(i), expectedBoth.get(i));
        }

        assertTrue(dt.hasOutgoing(1));
        assertTrue(dt.hasOutgoing(2));
        assertFalse(dt.hasOutgoing(3));
        assertTrue(dt.hasOutgoing(4));
        assertTrue(dt.hasOutgoing(5));
        assertTrue(dt.hasOutgoing(6));

        assertTrue(dt.allowsAllIncoming());
    }

    @Test
    void shouldAddSpecificTypesInSpecifiedDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 2, 3}, Direction.OUTGOING);

        assertEquals(dt.computeDirection(), Direction.OUTGOING);

        assertTrue(dt.hasSomeOutgoing());
        assertFalse(dt.hasSomeIncoming());
        assertFalse(dt.hasTypesInBothDirections());

        assertTrue(dt.isTypeLimited());

        assertTrue(dt.hasOutgoing(1));
        assertTrue(dt.hasOutgoing(2));
        assertTrue(dt.hasOutgoing(3));
        assertFalse(dt.hasOutgoing(4));

        assertFalse(dt.hasIncoming(1));
        assertFalse(dt.hasIncoming(2));
        assertFalse(dt.hasIncoming(3));
        assertFalse(dt.hasIncoming(4));

        assertTrue(dt.hasEither(1));
        assertTrue(dt.hasEither(2));
        assertTrue(dt.hasEither(3));
        assertFalse(dt.hasEither(4));

        assertFalse(dt.allowsAll());
        assertFalse(dt.allowsAllOutgoing());
        assertFalse(dt.allowsAllIncoming());

        int[] types = dt.typesWithoutDirections();
        assertNotEquals(types, null);
        assertEquals(3, types.length);
        for (int i = 1; i <= 3; i++) {
            assertEquals(i, types[i - 1]);
        }

        assertEquals(3, dt.numberOfCriteria());

        assertEquals(1, dt.criterionType(0));
        assertEquals(2, dt.criterionType(1));
        assertEquals(3, dt.criterionType(2));

        assertEquals(Direction.OUTGOING, dt.criterionDirection(0));
        assertEquals(Direction.OUTGOING, dt.criterionDirection(1));
        assertEquals(Direction.OUTGOING, dt.criterionDirection(2));
    }

    @Test
    void shouldAddSpecificTypesWithDuplicatesInSpecifiedDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 2, 3, 2, 3}, Direction.OUTGOING);
        dt.addTypes(new int[] {2, 1, 3}, Direction.OUTGOING);

        assertEquals(dt.computeDirection(), Direction.OUTGOING);

        assertTrue(dt.hasSomeOutgoing());
        assertFalse(dt.hasSomeIncoming());
        assertFalse(dt.hasTypesInBothDirections());

        assertTrue(dt.isTypeLimited());

        assertTrue(dt.hasOutgoing(1));
        assertTrue(dt.hasOutgoing(2));
        assertTrue(dt.hasOutgoing(3));
        assertFalse(dt.hasOutgoing(4));

        assertFalse(dt.hasIncoming(1));
        assertFalse(dt.hasIncoming(2));
        assertFalse(dt.hasIncoming(3));
        assertFalse(dt.hasIncoming(4));

        assertTrue(dt.hasEither(1));
        assertTrue(dt.hasEither(2));
        assertTrue(dt.hasEither(3));
        assertFalse(dt.hasEither(4));

        assertFalse(dt.allowsAll());
        assertFalse(dt.allowsAllOutgoing());
        assertFalse(dt.allowsAllIncoming());

        int[] types = dt.typesWithoutDirections();
        assertNotEquals(types, null);
        assertEquals(3, types.length);
        for (int i = 1; i <= 3; i++) {
            assertEquals(i, types[i - 1]);
        }

        assertEquals(3, dt.numberOfCriteria());

        assertEquals(1, dt.criterionType(0));
        assertEquals(2, dt.criterionType(1));
        assertEquals(3, dt.criterionType(2));

        assertEquals(Direction.OUTGOING, dt.criterionDirection(0));
        assertEquals(Direction.OUTGOING, dt.criterionDirection(1));
        assertEquals(Direction.OUTGOING, dt.criterionDirection(2));
    }

    @Test
    void shouldSortAddedTypes() {
        // given
        var directedTypes = new DirectedTypes(NO_TRACKING);
        var unsortedOutTypes = new int[] {4, 2, 3};
        var unsortedInTypes = new int[] {5, 0, 1};

        // when
        directedTypes.addTypes(unsortedOutTypes, Direction.OUTGOING);
        directedTypes.addTypes(unsortedInTypes, Direction.INCOMING);

        // then
        var prevType = -1;
        for (var i = 0; i < directedTypes.numberOfCriteria(); i++) {
            var type = directedTypes.criterionType(i);
            assertThat(type).isGreaterThan(prevType);
            prevType = type;
        }
    }
}
