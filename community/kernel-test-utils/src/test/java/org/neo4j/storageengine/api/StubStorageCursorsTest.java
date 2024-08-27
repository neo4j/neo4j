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
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.LongObjectMaps;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Direction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class StubStorageCursorsTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldReturnCorrectDegrees() {
        // given
        MutableLongObjectMap<MutableIntObjectMap<int[]>> expectedCountsPerNode = LongObjectMaps.mutable.empty();
        try (var cursors = new StubStorageCursors()) {
            int numNodes = 5;
            int numTypes = 3;
            for (int i = 0; i < numNodes; i++) {
                cursors.withNode(i);
            }
            for (int i = 0; i < 100; i++) {
                long startNode = random.nextLong(numNodes);
                int type = random.nextInt(numTypes);
                long endNode = random.nextLong(numNodes);
                cursors.withRelationship(i, startNode, type, endNode);
                expectedCountsPerNode.getIfAbsentPut(startNode, IntObjectMaps.mutable::empty)
                        .getIfAbsentPut(type, () -> new int[3])[
                        direction(startNode, endNode, startNode).ordinal()]++;
                if (startNode != endNode) {
                    expectedCountsPerNode.getIfAbsentPut(endNode, IntObjectMaps.mutable::empty)
                            .getIfAbsentPut(type, () -> new int[3])[
                            direction(startNode, endNode, endNode).ordinal()]++;
                }
            }

            // when
            try (var nodeCursor =
                    cursors.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE)) {
                expectedCountsPerNode.forEachKeyValue((node, expectedCounts) -> {
                    assertDegrees(nodeCursor, node, expectedCounts, ALL_RELATIONSHIPS);
                    for (int type = 0; type < numTypes; type++) {
                        for (var direction : Direction.values()) {
                            assertDegrees(nodeCursor, node, expectedCounts, selection(type, direction));
                        }
                    }
                });
            }
        }
    }

    private void assertDegrees(
            StorageNodeCursor nodeCursor,
            long node,
            MutableIntObjectMap<int[]> expectedCounts,
            RelationshipSelection selection) {
        nodeCursor.single(node);
        assertThat(nodeCursor.next()).isTrue();
        var degrees = new EagerDegrees();
        nodeCursor.degrees(selection, degrees);
        assertThat(degrees).isEqualTo(degreesOf(expectedCounts, selection));
    }

    private EagerDegrees degreesOf(MutableIntObjectMap<int[]> expectedCounts, RelationshipSelection selection) {
        var degrees = new EagerDegrees();
        expectedCounts.forEachKeyValue((type, counts) -> {
            if (selection.test(type)) {
                int outgoing = selection.test(type, OUTGOING) ? counts[OUTGOING.ordinal()] : 0;
                int incoming = selection.test(type, INCOMING) ? counts[INCOMING.ordinal()] : 0;
                int loop = counts[LOOP.ordinal()];
                if (outgoing > 0 || incoming > 0 || loop > 0) {
                    degrees.add(type, outgoing, incoming, loop);
                }
            }
        });
        return degrees;
    }

    private RelationshipDirection direction(long startNode, long endNode, long fromNodePov) {
        if (startNode == endNode) {
            return LOOP;
        }
        return fromNodePov == startNode ? OUTGOING : INCOMING;
    }
}
