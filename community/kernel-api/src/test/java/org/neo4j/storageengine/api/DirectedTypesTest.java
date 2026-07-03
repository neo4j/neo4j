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

            assertThat(dt.computeDirection()).isEqualTo(dir);
        }
    }

    @Test
    void typedComputeDirection() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addTypes(new int[] {1}, dir);

            assertThat(dt.computeDirection()).isEqualTo(dir);
        }
    }

    @Test
    void untypedOutgoingIncludesSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.hasOutgoing(1)).isTrue();
    }

    @Test
    void untypedOutgoingIncludesAllOutgoing() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.allowsAllOutgoing()).isTrue();
    }

    @Test
    void untypedOutgoingDoesNotIncludeIncoming() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.allowsAllIncoming()).isFalse();
        assertThat(dt.hasIncoming(1)).isFalse();
        assertThat(dt.hasSomeIncoming()).isFalse();
    }

    @Test
    void untypedOutgoingDoesNotHaveBothDirections() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.hasTypesInBothDirections()).isFalse();
    }

    @Test
    void untypedOverridesPreviouslyAddedSpecificType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1}, Direction.OUTGOING);
        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.numberOfCriteria()).isEqualTo(1);
    }

    @Test
    void specificCriterionType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.criterionType(0)).isEqualTo(99);
    }

    @Test
    void untypedCriterionType() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.criterionType(1)).isEqualTo(ANY_RELATIONSHIP_TYPE);
    }

    @Test
    void specificCriterionDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.numberOfCriteria()).isEqualTo(2);

        var specificCriterionDirection = IntStream.range(0, dt.numberOfCriteria())
                .filter(i -> dt.criterionType(i) == 99)
                .mapToObj(dt::criterionDirection)
                .findFirst()
                .get();

        assertThat(specificCriterionDirection).isEqualTo(Direction.OUTGOING);
    }

    @Test
    void specificCriterionDirectionBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.BOTH);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.numberOfCriteria()).isEqualTo(2);

        var specificCriterionDirection = IntStream.range(0, dt.numberOfCriteria())
                .filter(i -> dt.criterionType(i) == 99)
                .mapToObj(dt::criterionDirection)
                .findFirst()
                .get();

        assertThat(specificCriterionDirection).isEqualTo(Direction.BOTH);
    }

    @Test
    void untypedCriterionDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.criterionDirection(1)).isEqualTo(Direction.INCOMING);
    }

    @Test
    void typedInOneDirectionAndUntypedInOtherDirectionIsBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {99}, Direction.OUTGOING);
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.hasOutgoing(99)).isTrue();
        assertThat(dt.hasIncoming(99)).isTrue();
    }

    @Test
    void typeWildcardsAreOneCriterion() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addUntyped(dir);

            assertThat(dt.numberOfCriteria()).isEqualTo(1);
        }
    }

    @Test
    void addingRedundantTypeDoesntChangeNumberOfCriteria() {
        for (Direction dir : kernelDirections) {

            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addUntyped(dir);
            dt.addTypes(new int[] {1}, dir);

            assertThat(dt.numberOfCriteria()).isEqualTo(1);
        }
    }

    @Test
    void writeModeResetsSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addTypes(new int[] {1}, Direction.OUTGOING);

        assertThat(dt.hasOutgoing(1)).isTrue();

        dt.clear();
        dt.addTypes(new int[] {1}, Direction.INCOMING);

        assertThat(dt.hasOutgoing(1)).isFalse();
        assertThat(dt.hasIncoming(1)).isTrue();
    }

    @Test
    void writeModeResetsUntyped() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.hasOutgoing(1)).isTrue();

        dt.clear();
        dt.addUntyped(Direction.INCOMING);

        assertThat(dt.hasOutgoing(1)).isFalse();
        assertThat(dt.hasIncoming(1)).isTrue();
    }

    @Test
    void duplicatesAreIgnored() {
        for (Direction dir : kernelDirections) {
            DirectedTypes dt = new DirectedTypes(NO_TRACKING);

            dt.addTypes(new int[] {1, 1}, dir);
            dt.addTypes(new int[] {1}, dir);

            assertThat(dt.numberOfCriteria()).isEqualTo(1);
            int[] types = dt.typesWithoutDirections();
            assertThat(types).isNotNull();
            assertThat(types).containsExactly(1);
        }
    }

    @Test
    void duplicatesIncomingAndOutgoingAreMergedAsBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 1}, Direction.INCOMING);
        dt.addTypes(new int[] {1}, Direction.OUTGOING);

        assertThat(dt.computeDirection()).isEqualTo(Direction.BOTH);
        assertThat(dt.hasTypesInBothDirections()).isTrue();
        assertThat(dt.hasIncoming(1)).isTrue();
        assertThat(dt.hasOutgoing(1)).isTrue();

        assertThat(dt.numberOfCriteria()).isEqualTo(1);
        int[] types = dt.typesWithoutDirections();
        assertThat(types).isNotNull();
        assertThat(types).containsExactly(1);
    }

    @Test
    void reverseSpecific() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        var reversed = dt.reverse();
        assertThat(reversed.computeDirection()).isEqualTo(Direction.INCOMING);
        assertThat(reversed.hasSomeIncoming()).isTrue();
        assertThat(reversed.allowsAllIncoming()).isTrue();
        assertThat(reversed.hasSomeOutgoing()).isFalse();
        assertThat(reversed.allowsAllOutgoing()).isFalse();

        var rereversed = reversed.reverse();
        assertThat(rereversed.computeDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(rereversed.hasSomeOutgoing()).isTrue();
        assertThat(rereversed.allowsAllOutgoing()).isTrue();
        assertThat(rereversed.hasSomeIncoming()).isFalse();
        assertThat(rereversed.allowsAllIncoming()).isFalse();
    }

    @Test
    void reverseAll() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.OUTGOING);

        var reversed = dt.reverse();

        assertThat(reversed.computeDirection()).isEqualTo(Direction.INCOMING);
        assertThat(reversed.numberOfCriteria()).isEqualTo(1);
        assertThat(reversed.allowsAll()).isFalse();
        assertThat(reversed.allowsAllIncoming()).isTrue();
    }

    @Test
    void reverseBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.BOTH);

        var reversed = dt.reverse();

        assertThat(reversed.numberOfCriteria()).isEqualTo(1);
        assertThat(reversed.allowsAll()).isTrue();
    }

    @Test
    void untypedIncomingAndOutgoingMergesIntoBoth() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);
        dt.addUntyped(Direction.INCOMING);
        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.allowsAll()).isTrue();
    }

    @Test
    void untypedBothIncludesEverything() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addUntyped(Direction.BOTH);

        assertThat(dt.hasOutgoing(1)).isTrue();
        assertThat(dt.hasSomeOutgoing()).isTrue();
        assertThat(dt.allowsAllOutgoing()).isTrue();

        assertThat(dt.hasIncoming(2)).isTrue();
        assertThat(dt.hasSomeIncoming()).isTrue();
        assertThat(dt.allowsAllIncoming()).isTrue();
    }

    @Test
    void whenNotTypeLimitedThenTypesWithoutDirectionsShouldBeNull() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1}, Direction.OUTGOING);
        dt.addUntyped(Direction.OUTGOING);

        assertThat(dt.typesWithoutDirections()).isNull();
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
        assertThat(dt.numberOfCriteria()).isEqualTo(noCriteria);

        IntArrayList outgoing = new IntArrayList();
        IntArrayList both = new IntArrayList();
        for (int i = 0; i < noCriteria; i++) {
            int type = dt.criterionType(i);
            Direction dir = dt.criterionDirection(i);
            switch (dir) {
                case OUTGOING -> outgoing.add(type);
                case BOTH -> both.add(type);
                case INCOMING -> assertThat(type).isEqualTo(ANY_RELATIONSHIP_TYPE);
            }
        }
        outgoing.sortThis();
        both.sortThis();

        assertThat(outgoing.size()).isEqualTo(numberOfOutgoingCriteria);
        assertThat(both.size()).isEqualTo(numberOfUndirectedCriteria);

        IntArrayList expectedOutgoing = new IntArrayList(new int[] {4});
        IntArrayList expectedBoth = new IntArrayList(1, 2, 5, 6);

        for (int i = 0; i < numberOfOutgoingCriteria; i++) {
            assertThat(outgoing.get(i)).isEqualTo(expectedOutgoing.get(i));
        }
        for (int i = 0; i < numberOfUndirectedCriteria; i++) {
            assertThat(both.get(i)).isEqualTo(expectedBoth.get(i));
        }

        assertThat(dt.hasOutgoing(1)).isTrue();
        assertThat(dt.hasOutgoing(2)).isTrue();
        assertThat(dt.hasOutgoing(3)).isFalse();
        assertThat(dt.hasOutgoing(4)).isTrue();
        assertThat(dt.hasOutgoing(5)).isTrue();
        assertThat(dt.hasOutgoing(6)).isTrue();

        assertThat(dt.allowsAllIncoming()).isTrue();
    }

    @Test
    void shouldAddSpecificTypesInSpecifiedDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 2, 3}, Direction.OUTGOING);

        assertThat(dt.computeDirection()).isEqualTo(Direction.OUTGOING);

        assertThat(dt.hasSomeOutgoing()).isTrue();
        assertThat(dt.hasSomeIncoming()).isFalse();
        assertThat(dt.hasTypesInBothDirections()).isFalse();

        assertThat(dt.isTypeLimited()).isTrue();

        assertThat(dt.hasOutgoing(1)).isTrue();
        assertThat(dt.hasOutgoing(2)).isTrue();
        assertThat(dt.hasOutgoing(3)).isTrue();
        assertThat(dt.hasOutgoing(4)).isFalse();

        assertThat(dt.hasIncoming(1)).isFalse();
        assertThat(dt.hasIncoming(2)).isFalse();
        assertThat(dt.hasIncoming(3)).isFalse();
        assertThat(dt.hasIncoming(4)).isFalse();

        assertThat(dt.hasEither(1)).isTrue();
        assertThat(dt.hasEither(2)).isTrue();
        assertThat(dt.hasEither(3)).isTrue();
        assertThat(dt.hasEither(4)).isFalse();

        assertThat(dt.allowsAll()).isFalse();
        assertThat(dt.allowsAllOutgoing()).isFalse();
        assertThat(dt.allowsAllIncoming()).isFalse();

        int[] types = dt.typesWithoutDirections();
        assertThat(types).isNotNull();
        assertThat(types).containsExactly(1, 2, 3);

        assertThat(dt.numberOfCriteria()).isEqualTo(3);

        assertThat(dt.criterionType(0)).isEqualTo(1);
        assertThat(dt.criterionType(1)).isEqualTo(2);
        assertThat(dt.criterionType(2)).isEqualTo(3);

        assertThat(dt.criterionDirection(0)).isEqualTo(Direction.OUTGOING);
        assertThat(dt.criterionDirection(1)).isEqualTo(Direction.OUTGOING);
        assertThat(dt.criterionDirection(2)).isEqualTo(Direction.OUTGOING);
    }

    @Test
    void shouldAddSpecificTypesWithDuplicatesInSpecifiedDirection() {
        DirectedTypes dt = new DirectedTypes(NO_TRACKING);

        dt.addTypes(new int[] {1, 2, 3, 2, 3}, Direction.OUTGOING);
        dt.addTypes(new int[] {2, 1, 3}, Direction.OUTGOING);

        assertThat(dt.computeDirection()).isEqualTo(Direction.OUTGOING);

        assertThat(dt.hasSomeOutgoing()).isTrue();
        assertThat(dt.hasSomeIncoming()).isFalse();
        assertThat(dt.hasTypesInBothDirections()).isFalse();

        assertThat(dt.isTypeLimited()).isTrue();

        assertThat(dt.hasOutgoing(1)).isTrue();
        assertThat(dt.hasOutgoing(2)).isTrue();
        assertThat(dt.hasOutgoing(3)).isTrue();
        assertThat(dt.hasOutgoing(4)).isFalse();

        assertThat(dt.hasIncoming(1)).isFalse();
        assertThat(dt.hasIncoming(2)).isFalse();
        assertThat(dt.hasIncoming(3)).isFalse();
        assertThat(dt.hasIncoming(4)).isFalse();

        assertThat(dt.hasEither(1)).isTrue();
        assertThat(dt.hasEither(2)).isTrue();
        assertThat(dt.hasEither(3)).isTrue();
        assertThat(dt.hasEither(4)).isFalse();

        assertThat(dt.allowsAll()).isFalse();
        assertThat(dt.allowsAllOutgoing()).isFalse();
        assertThat(dt.allowsAllIncoming()).isFalse();

        int[] types = dt.typesWithoutDirections();
        assertThat(types).isNotNull();
        assertThat(types).containsExactly(1, 2, 3);

        assertThat(dt.numberOfCriteria()).isEqualTo(3);

        assertThat(dt.criterionType(0)).isEqualTo(1);
        assertThat(dt.criterionType(1)).isEqualTo(2);
        assertThat(dt.criterionType(2)).isEqualTo(3);

        assertThat(dt.criterionDirection(0)).isEqualTo(Direction.OUTGOING);
        assertThat(dt.criterionDirection(1)).isEqualTo(Direction.OUTGOING);
        assertThat(dt.criterionDirection(2)).isEqualTo(Direction.OUTGOING);
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
