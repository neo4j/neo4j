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
package org.neo4j.internal.batchimport.cache;

import static java.lang.Math.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
public class NodeRelationshipCacheTest {
    @Inject
    private RandomSupport random;

    private NodeRelationshipCache cache;

    @AfterEach
    public void after() {
        cache.close();
    }

    private static Stream<Long> data() {
        return Stream.of(0L, Integer.MAX_VALUE * 2L);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldReportCorrectNumberOfDenseNodes(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.AUTO_WITHOUT_PAGECACHE, 5, 100, base, INSTANCE);
        cache.setNodeCount(26);
        increment(cache, 2, 10);
        increment(cache, 5, 2);
        increment(cache, 7, 12);
        increment(cache, 23, 4);
        increment(cache, 24, 5);
        increment(cache, 25, 6);

        // THEN
        Assertions.assertFalse(cache.isDense(0));
        Assertions.assertTrue(cache.isDense(2));
        Assertions.assertFalse(cache.isDense(5));
        Assertions.assertTrue(cache.isDense(7));
        Assertions.assertFalse(cache.isDense(23));
        Assertions.assertTrue(cache.isDense(24));
        Assertions.assertTrue(cache.isDense(25));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldGoThroughThePhases(long base) {
        // GIVEN
        int nodeCount = 10;
        cache = new NodeRelationshipCache(NumberArrayFactories.OFF_HEAP, 20, 100, base, INSTANCE);
        cache.setNodeCount(nodeCount);
        incrementRandomCounts(cache, nodeCount, nodeCount * 20);

        // Test sparse node semantics
        {
            long node = findNode(cache, nodeCount, false);
            testNode(cache, node, null);
        }

        // Test dense node semantics
        {
            long node = findNode(cache, nodeCount, true);
            testNode(cache, node, Direction.OUTGOING);
            testNode(cache, node, Direction.INCOMING);
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldObserveFirstRelationshipAsEmptyInEachDirection(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.AUTO_WITHOUT_PAGECACHE, 1, 100, base, INSTANCE);
        int nodes = 100;
        int typeId = 5;
        Direction[] directions = Direction.values();
        NodeRelationshipCache.GroupVisitor groupVisitor = mock(NodeRelationshipCache.GroupVisitor.class);
        cache.setForwardScan(true, true);
        cache.setNodeCount(nodes + 1);
        for (int i = 0; i < nodes; i++) {
            Assertions.assertEquals(-1L, cache.getFirstRel(nodes, groupVisitor));
            cache.incrementCount(i);
            long previous = cache.getAndPutRelationship(
                    i, typeId, directions[i % directions.length], random.nextInt(1_000_000), true);
            Assertions.assertEquals(-1L, previous);
        }

        // WHEN
        cache.setForwardScan(false, true);
        for (int i = 0; i < nodes; i++) {
            long previous = cache.getAndPutRelationship(
                    i, typeId, directions[i % directions.length], random.nextInt(1_000_000), false);
            Assertions.assertEquals(-1L, previous);
        }

        // THEN
        cache.setForwardScan(true, true);
        for (int i = 0; i < nodes; i++) {
            Assertions.assertEquals(-1L, cache.getFirstRel(nodes, groupVisitor));
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldResetCountAfterGetOnDenseNodes(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.AUTO_WITHOUT_PAGECACHE, 1, 100, base, INSTANCE);
        long nodeId = 0;
        int typeId = 3;
        cache.setNodeCount(1);
        cache.incrementCount(nodeId);
        cache.incrementCount(nodeId);
        cache.getAndPutRelationship(nodeId, typeId, OUTGOING, 10, true);
        cache.getAndPutRelationship(nodeId, typeId, OUTGOING, 12, true);
        Assertions.assertTrue(cache.isDense(nodeId));

        // WHEN
        long countNoReset = cache.getCount(nodeId, typeId, OUTGOING, false);
        long countDoReset = cache.getCount(nodeId, typeId, OUTGOING, true);
        Assertions.assertEquals(2, countNoReset);
        Assertions.assertEquals(2, countDoReset);

        // THEN
        Assertions.assertEquals(0, cache.getCount(nodeId, typeId, OUTGOING, false));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldGetAndPutRelationshipAroundChunkEdge(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 10, INSTANCE);

        // WHEN
        long nodeId = 1_000_000 - 1;
        int typeId = 10;
        cache.setNodeCount(nodeId + 1);
        Direction direction = Direction.OUTGOING;
        long relId = 10;
        cache.getAndPutRelationship(nodeId, typeId, direction, relId, false);

        // THEN
        Assertions.assertEquals(relId, cache.getFirstRel(nodeId, mock(NodeRelationshipCache.GroupVisitor.class)));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldPutRandomStuff(long base) {
        // GIVEN
        int typeId = 10;
        int nodes = 10_000;
        MutableLongObjectMap<long[]> key = new LongObjectHashMap<>(nodes);
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 1000, base, INSTANCE);

        // mark random nodes as dense (dense node threshold is 1 so enough with one increment
        cache.setNodeCount(nodes);
        for (long nodeId = 0; nodeId < nodes; nodeId++) {
            if (random.nextBoolean()) {
                cache.incrementCount(nodeId);
            }
        }

        // WHEN
        for (int i = 0; i < 100_000; i++) {
            long nodeId = random.nextLong(nodes);
            boolean dense = cache.isDense(nodeId);
            Direction direction = random.among(Direction.values());
            long relationshipId = random.nextLong(1_000_000);
            long previousHead = cache.getAndPutRelationship(nodeId, typeId, direction, relationshipId, false);
            long[] keyIds = key.get(nodeId);
            int keyIndex = dense ? direction.ordinal() : 0;
            if (keyIds == null) {
                key.put(nodeId, keyIds = minusOneLongs(Direction.values().length));
            }
            Assertions.assertEquals(keyIds[keyIndex], previousHead);
            keyIds[keyIndex] = relationshipId;
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldPut6ByteRelationshipIds(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 100, base, INSTANCE);
        long sparseNode = 0;
        long denseNode = 1;
        long relationshipId = (1L << 48) - 2;
        int typeId = 10;
        cache.setNodeCount(2);
        cache.incrementCount(denseNode);

        // WHEN
        Assertions.assertEquals(-1L, cache.getAndPutRelationship(sparseNode, typeId, OUTGOING, relationshipId, false));
        Assertions.assertEquals(-1L, cache.getAndPutRelationship(denseNode, typeId, OUTGOING, relationshipId, false));

        // THEN
        Assertions.assertEquals(relationshipId, cache.getAndPutRelationship(sparseNode, typeId, OUTGOING, 1, false));
        Assertions.assertEquals(relationshipId, cache.getAndPutRelationship(denseNode, typeId, OUTGOING, 1, false));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldFailFastIfTooBigRelationshipId(long base) {
        // GIVEN
        int typeId = 10;
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 100, base, INSTANCE);
        cache.setNodeCount(1);

        // WHEN
        cache.getAndPutRelationship(0, typeId, OUTGOING, (1L << 48) - 2, false);
        try {
            cache.getAndPutRelationship(0, typeId, OUTGOING, (1L << 48) - 1, false);
            Assertions.fail("Should fail");
        } catch (IllegalArgumentException e) {
            // THEN Good
            Assertions.assertTrue(e.getMessage().contains("max"));
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldVisitChangedNodes(long base) {
        // GIVEN
        int nodes = 100;
        int typeId = 10;
        int chunkSize = 10;
        List<Long> changedNodes = new ArrayList<>();
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 2, chunkSize, base, INSTANCE);
        cache.setNodeCount(nodes);
        for (long nodeId = 0; nodeId < nodes; nodeId++) {
            if (nodeId >= chunkSize && nodeId < 2 * chunkSize) {
                // One chunk without any changes
                continue;
            }
            cache.incrementCount(nodeId);
            if (random.nextBoolean()) {
                cache.incrementCount(nodeId);
            }
            changedNodes.add(nodeId);
        }
        MutableLongSet keySparseChanged = new LongHashSet();
        MutableLongSet keyDenseChanged = new LongHashSet();
        for (int i = 0; i < nodes / 2; i++) {
            long nodeId = random.among(changedNodes);
            cache.getAndPutRelationship(nodeId, typeId, Direction.OUTGOING, random.nextLong(1_000_000), false);
            boolean dense = cache.isDense(nodeId);
            (dense ? keyDenseChanged : keySparseChanged).add(nodeId);
        }

        {
            // WHEN (sparse)
            NodeRelationshipCache.NodeChangeVisitor visitor = (nodeId, array) -> {
                // THEN (sparse)
                Assertions.assertTrue(
                        keySparseChanged.remove(nodeId), "Unexpected sparse change reported for " + nodeId);
            };
            cache.visitChangedNodes(visitor, NodeType.NODE_TYPE_SPARSE);
            Assertions.assertTrue(
                    keySparseChanged.isEmpty(),
                    "There was " + keySparseChanged.size() + " expected sparse changes that weren't reported");
        }

        {
            // WHEN (dense)
            NodeRelationshipCache.NodeChangeVisitor visitor = (nodeId, array) -> {
                // THEN (dense)
                Assertions.assertTrue(keyDenseChanged.remove(nodeId), "Unexpected dense change reported for " + nodeId);
            };
            cache.visitChangedNodes(visitor, NodeType.NODE_TYPE_DENSE);
            Assertions.assertTrue(
                    keyDenseChanged.isEmpty(),
                    "There was " + keyDenseChanged.size() + " expected dense changes that weren reported");
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldFailFastOnTooHighCountOnNode(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 10, 100, base, INSTANCE);
        long nodeId = 5;
        long count = NodeRelationshipCache.MAX_COUNT - 1;
        int typeId = 10;
        cache.setNodeCount(10);
        cache.setCount(nodeId, count, typeId, OUTGOING);

        // WHEN
        cache.incrementCount(nodeId);
        try {
            cache.incrementCount(nodeId);
            Assertions.fail("Should have failed");
        } catch (IllegalStateException e) {
            // THEN Good
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldKeepNextGroupIdForNextRound(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 100, base, INSTANCE);
        long nodeId = 0;
        int typeId = 10;
        cache.setNodeCount(nodeId + 1);
        cache.incrementCount(nodeId);
        NodeRelationshipCache.GroupVisitor groupVisitor = mock(NodeRelationshipCache.GroupVisitor.class);
        when(groupVisitor.visit(anyLong(), anyInt(), anyLong(), anyLong(), anyLong()))
                .thenReturn(1L, 2L, 3L);

        long firstRelationshipGroupId;
        {
            // WHEN importing the first type
            long relationshipId = 10;
            cache.getAndPutRelationship(nodeId, typeId, OUTGOING, relationshipId, true);
            firstRelationshipGroupId = cache.getFirstRel(nodeId, groupVisitor);

            // THEN
            Assertions.assertEquals(1L, firstRelationshipGroupId);
            verify(groupVisitor).visit(nodeId, typeId, relationshipId, -1L, -1L);

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan(false, true);
            cache.getAndPutRelationship(nodeId, typeId, OUTGOING, relationshipId, false);
            cache.setForwardScan(true, true);
        }

        long secondRelationshipGroupId;
        {
            // WHEN importing the second type
            long relationshipId = 11;
            cache.getAndPutRelationship(nodeId, typeId, INCOMING, relationshipId, true);
            secondRelationshipGroupId = cache.getFirstRel(nodeId, groupVisitor);

            // THEN
            Assertions.assertEquals(2L, secondRelationshipGroupId);
            verify(groupVisitor).visit(nodeId, typeId, -1, relationshipId, -1L);

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan(false, true);
            cache.getAndPutRelationship(nodeId, typeId, OUTGOING, relationshipId, false);
            cache.setForwardScan(true, true);
        }

        {
            // WHEN importing the third type
            long relationshipId = 10;
            cache.getAndPutRelationship(nodeId, typeId, BOTH, relationshipId, true);
            long thirdRelationshipGroupId = cache.getFirstRel(nodeId, groupVisitor);
            Assertions.assertEquals(3L, thirdRelationshipGroupId);
            verify(groupVisitor).visit(nodeId, typeId, -1L, -1L, relationshipId);
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldHaveDenseNodesWithBigCounts(long base) {
        // A count of a dense node follow a different path during import, first there's counting per node
        // then import goes into actual import of relationships where individual chain degrees are
        // kept. So this test will first set a total count, then set count for a specific chain

        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 100, base, INSTANCE);
        long nodeId = 1;
        int typeId = 10;
        cache.setNodeCount(nodeId + 1);
        cache.setCount(nodeId, 2, typeId, OUTGOING); // surely dense now
        cache.getAndPutRelationship(nodeId, typeId, OUTGOING, 1, true);
        cache.getAndPutRelationship(nodeId, typeId, INCOMING, 2, true);

        // WHEN
        long highCountOut = NodeRelationshipCache.MAX_COUNT - 100;
        long highCountIn = NodeRelationshipCache.MAX_COUNT - 50;
        cache.setCount(nodeId, highCountOut, typeId, OUTGOING);
        cache.setCount(nodeId, highCountIn, typeId, INCOMING);
        cache.getAndPutRelationship(nodeId, typeId, OUTGOING, 1, true /*increment count*/);
        cache.getAndPutRelationship(nodeId, typeId, INCOMING, 2, true /*increment count*/);

        // THEN
        Assertions.assertEquals(highCountOut + 1, cache.getCount(nodeId, typeId, OUTGOING, false));
        Assertions.assertEquals(highCountIn + 1, cache.getCount(nodeId, typeId, INCOMING, false));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldCacheMultipleDenseNodeRelationshipHeads(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, INSTANCE);
        cache.setNodeCount(10);
        long nodeId = 3;
        cache.setCount(nodeId, 10, /*these do not matter ==>*/ 0, OUTGOING);

        // WHEN
        Map<Pair<Integer, Direction>, Long> firstRelationshipIds = new HashMap<>();
        int typeCount = 3;
        for (int typeId = 0, relationshipId = 0; typeId < typeCount; typeId++) {
            for (Direction direction : Direction.values()) {
                long firstRelationshipId = relationshipId++;
                cache.getAndPutRelationship(nodeId, typeId, direction, firstRelationshipId, true);
                firstRelationshipIds.put(Pair.of(typeId, direction), firstRelationshipId);
            }
        }
        AtomicInteger visitCount = new AtomicInteger();
        NodeRelationshipCache.GroupVisitor visitor = (nodeId1, typeId, out, in, loop) -> {
            visitCount.incrementAndGet();
            Assertions.assertEquals(
                    firstRelationshipIds.get(Pair.of(typeId, OUTGOING)).longValue(), out);
            Assertions.assertEquals(
                    firstRelationshipIds.get(Pair.of(typeId, INCOMING)).longValue(), in);
            Assertions.assertEquals(
                    firstRelationshipIds.get(Pair.of(typeId, BOTH)).longValue(), loop);
            return 0;
        };
        cache.getFirstRel(nodeId, visitor);

        // THEN
        Assertions.assertEquals(typeCount, visitCount.get());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldHaveSparseNodesWithBigCounts(long base) {
        // GIVEN
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, 100, base, INSTANCE);
        long nodeId = 1;
        int typeId = 10;
        cache.setNodeCount(nodeId + 1);

        // WHEN
        long highCount = NodeRelationshipCache.MAX_COUNT - 100;
        cache.setCount(nodeId, highCount, typeId, OUTGOING);
        long nextHighCount = cache.incrementCount(nodeId);

        // THEN
        Assertions.assertEquals(highCount + 1, nextHighCount);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldFailFastOnTooHighNodeCount(long base) {
        // given
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, 1, INSTANCE);

        try {
            // when
            cache.setNodeCount(2L << (5 * Byte.SIZE));
            Assertions.fail("Should have failed");
        } catch (IllegalArgumentException e) {
            // then good
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldAllocateRelationshipGroupWithHighTypeId(long base) {
        // given
        int denseNodeThreshold = 1;
        long nodeId = 99;
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, denseNodeThreshold, INSTANCE);
        cache.setNodeCount(nodeId + 1);
        for (int i = 0; i < denseNodeThreshold * 2; i++) {
            cache.incrementCount(nodeId);
        }
        assertThat(cache.isDense(nodeId)).isTrue();

        // when
        int typeId1 = 0xFFFF + 1_000;
        int typeId2 = 0xFFFF + 10_000;
        long firstRelId1 = 2134;
        long firstRelId2 = 34873;
        assertThat(cache.getAndPutRelationship(nodeId, typeId1, OUTGOING, firstRelId1, true))
                .isEqualTo(-1);
        assertThat(cache.getAndPutRelationship(nodeId, typeId2, INCOMING, firstRelId2, true))
                .isEqualTo(-1);

        // then
        MutableIntObjectMap<long[]> expectedGroups = IntObjectMaps.mutable.empty();
        expectedGroups.put(typeId1, new long[] {firstRelId1, -1, -1});
        expectedGroups.put(typeId2, new long[] {-1, firstRelId2, -1});
        cache.getFirstRel(nodeId, (groupNodeId, typeId, out, in, loop) -> {
            assertThat(groupNodeId).isEqualTo(nodeId);
            long[] group = expectedGroups.remove(typeId);
            assertThat(group).isNotNull();
            assertThat(group).isEqualTo(new long[] {out, in, loop});
            return 0;
        });
        assertThat(expectedGroups.isEmpty()).isTrue();
    }

    @Test
    void shouldMarkAsExplicitlyDense() {
        // given
        int denseNodeThreshold = 10;
        long nodeId = 5;
        cache = new NodeRelationshipCache(NumberArrayFactories.HEAP, denseNodeThreshold, INSTANCE);
        cache.setNodeCount(10);

        // when
        assertThat(cache.isDense(nodeId)).isFalse();
        cache.markAsExplicitlyDense(nodeId);

        // then
        assertThat(cache.isDense(nodeId)).isTrue();

        // and when
        cache.incrementCount(nodeId);
        assertThat(cache.isDense(nodeId)).isTrue();
    }

    private static void testNode(NodeRelationshipCache link, long node, Direction direction) {
        int typeId = 0; // doesn't matter here because it's all sparse
        long count = link.getCount(node, typeId, direction, false);
        Assertions.assertEquals(-1, link.getAndPutRelationship(node, typeId, direction, 5, false));
        Assertions.assertEquals(5, link.getAndPutRelationship(node, typeId, direction, 10, false));
        Assertions.assertEquals(count, link.getCount(node, typeId, direction, false));
    }

    private static long findNode(NodeRelationshipCache link, long nodeCount, boolean isDense) {
        for (long i = 0; i < nodeCount; i++) {
            if (link.isDense(i) == isDense) {
                return i;
            }
        }
        throw new IllegalArgumentException("No dense node found");
    }

    private long incrementRandomCounts(NodeRelationshipCache link, int nodeCount, int i) {
        long highestSeenCount = 0;
        while (i-- > 0) {
            long node = random.nextInt(nodeCount);
            highestSeenCount = max(highestSeenCount, link.incrementCount(node));
        }
        return highestSeenCount;
    }

    private static void increment(NodeRelationshipCache cache, long node, int count) {
        for (int i = 0; i < count; i++) {
            cache.incrementCount(node);
        }
    }

    private static long[] minusOneLongs(int length) {
        long[] array = new long[length];
        Arrays.fill(array, -1);
        return array;
    }
}
