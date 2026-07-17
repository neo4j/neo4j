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
package org.neo4j.internal.kernel.api.helpers.traversal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.EmptyShortestCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedMultiShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.ZeroLengthShortestCursor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class ShortestPathBFSFactoryTest {

    private static final long NODE_ID = 42L;
    private static final long OTHER_NODE_ID = 99L;
    private static final int[] TYPES = new int[0];
    private static final int MAX_DEPTH = 10;

    private Read read;
    private NodeCursor nodeCursor;
    private RelationshipTraversalCursor relCursor;
    private final MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
    private final LongPredicate nodeFilter = id -> true;
    private final Predicate<RelationshipTraversalEntities> relFilter = rel -> true;

    @BeforeEach
    void setUp() {
        read = mock(Read.class);
        nodeCursor = mock(NodeCursor.class);
        relCursor = mock(RelationshipTraversalCursor.class);
    }

    private ShortestPathBFS create(
            long sourceNodeId,
            long targetNodeId,
            Direction direction,
            boolean allowZeroLength,
            boolean needOnlyOnePath,
            TraversalMode traversalMode,
            ShortestPathBFS oldBfs) {
        return ShortestPathBFSFactory.create(
                sourceNodeId,
                targetNodeId,
                TYPES,
                direction,
                MAX_DEPTH,
                read,
                nodeCursor,
                relCursor,
                memoryTracker,
                nodeFilter,
                relFilter,
                false,
                allowZeroLength,
                needOnlyOnePath,
                traversalMode,
                oldBfs);
    }

    static Stream<Arguments> cursorTypes() {
        return Stream.of(
                // Loop + ACYCLIC
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        false,
                        false,
                        TraversalMode.ACYCLIC,
                        EmptyShortestCursor.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        true,
                        false,
                        TraversalMode.ACYCLIC,
                        EmptyShortestCursor.class),
                // Loop + WALK
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        true,
                        false,
                        TraversalMode.WALK,
                        ZeroLengthShortestCursor.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        false,
                        true,
                        TraversalMode.WALK,
                        UndirectedSingleShortestLoopWalkCursor.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        false,
                        false,
                        TraversalMode.WALK,
                        UndirectedShortestLoopWalkCursor.class),
                // Loop + TRAIL
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        true,
                        false,
                        TraversalMode.TRAIL,
                        ZeroLengthShortestCursor.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        false,
                        true,
                        TraversalMode.TRAIL,
                        UndirectedSingleShortestLoopCursor.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.BOTH,
                        false,
                        false,
                        TraversalMode.TRAIL,
                        UndirectedMultiShortestLoopCursor.class),
                // TRAIL
                Arguments.of(
                        NODE_ID,
                        OTHER_NODE_ID,
                        Direction.BOTH,
                        false,
                        false,
                        TraversalMode.TRAIL,
                        BiDirectionalBFS.class),
                Arguments.of(
                        NODE_ID,
                        NODE_ID,
                        Direction.OUTGOING,
                        false,
                        false,
                        TraversalMode.TRAIL,
                        BiDirectionalBFS.class),
                // ACYCLIC
                Arguments.of(
                        NODE_ID,
                        OTHER_NODE_ID,
                        Direction.BOTH,
                        false,
                        false,
                        TraversalMode.ACYCLIC,
                        BiDirectionalBFS.class));
    }

    @ParameterizedTest
    @MethodSource("cursorTypes")
    void shouldReturnCorrectCursorType(
            long sourceNodeId,
            long targetNodeId,
            Direction direction,
            boolean allowZeroLength,
            boolean needOnlyOnePath,
            TraversalMode traversalMode,
            Class<? extends ShortestPathBFS> expectedType) {
        var result =
                create(sourceNodeId, targetNodeId, direction, allowZeroLength, needOnlyOnePath, traversalMode, null);
        assertThat(result).isInstanceOf(expectedType);
    }

    @Test
    void shouldReuseBiDirectionalBFSWhenProvidedAsOldBfs() {
        var first = create(NODE_ID, OTHER_NODE_ID, Direction.BOTH, false, false, TraversalMode.TRAIL, null);
        var reused = create(NODE_ID, OTHER_NODE_ID, Direction.BOTH, false, false, TraversalMode.TRAIL, first);
        assertThat(reused).isSameAs(first);
    }

    @Test
    void shouldCreateNewBiDirectionalBFSWhenOldBfsIsNotBiDirectional() {
        var nonBiDirectional = mock(ShortestPathBFS.class);
        var result =
                create(NODE_ID, OTHER_NODE_ID, Direction.BOTH, false, false, TraversalMode.TRAIL, nonBiDirectional);
        assertThat(result).isInstanceOf(BiDirectionalBFS.class).isNotSameAs(nonBiDirectional);
    }
}
