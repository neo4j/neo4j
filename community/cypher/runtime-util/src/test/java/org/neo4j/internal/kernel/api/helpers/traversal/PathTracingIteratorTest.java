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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.virtual.PathReference;
import org.neo4j.values.virtual.VirtualValues;

class PathTracingIteratorTest {

    @Test
    void shouldOnlyGiveOnePathWhenGivenOneNode() {
        HeapTrackingLongObjectHashMap<HeapTrackingArrayList<PathTraceStep>> emptyPathTraceData =
                HeapTrackingCollections.newLongObjectMap(new LocalMemoryTracker());

        LongIterator intersection = longIterator(10);

        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                new PathTracingIterator.MultiPathTracingIterator(
                        intersection, 0, 0, emptyPathTraceData, emptyPathTraceData);

        assertThat(pti.hasNext()).isTrue();
        assertThat(pti.next()).isEqualTo(VirtualValues.pathReference(new long[] {10}, new long[] {}));
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldOnlyGiveOneLongerPath() {
        int sourceLength = 10;
        int targetLength = 5;
        int totalLength = sourceLength + targetLength;

        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti = singlePath(sourceLength, targetLength);

        assertThat(pti.hasNext()).isTrue();
        assertThat(pti.next())
                .isEqualTo(VirtualValues.pathReference(longRange(totalLength + 1), longRange(totalLength)));
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldOnlyGiveOneLongerPathEmptySourcePart() {
        int sourceLength = 0;
        int targetLength = 15;
        int totalLength = sourceLength + targetLength;

        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti = singlePath(sourceLength, targetLength);

        assertThat(pti.hasNext()).isTrue();
        assertThat(pti.next())
                .isEqualTo(VirtualValues.pathReference(longRange(totalLength + 1), longRange(totalLength)));
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldOnlyGiveOneLongerPathEmptyTargetPart() {
        int sourceLength = 15;
        int targetLength = 0;
        int totalLength = sourceLength + targetLength;

        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti = singlePath(sourceLength, targetLength);

        assertThat(pti.hasNext()).isTrue();
        assertThat(pti.next())
                .isEqualTo(VirtualValues.pathReference(longRange(totalLength + 1), longRange(totalLength)));
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePaths() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 4;
        int targetLength = 3;
        int width = 4;
        int degree = 3;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePathsZeroSourceLength() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 0;
        int targetLength = 3;
        int width = 4;
        int degree = 3;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePathsZeroTargetLength() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 3;
        int targetLength = 0;
        int width = 4;
        int degree = 3;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePathsSourceLengthOne() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 1;
        int targetLength = 3;
        int width = 4;
        int degree = 3;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePathsTargetLengthOne() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 3;
        int targetLength = 1;
        int width = 4;
        int degree = 3;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    @Test
    void shouldGiveCorrectCardinalityForMultiplePathsManyRelsBetweenNodes() {
        // Keep these constants fairly small as expectedCardinality grows very fast and the loop below is slow
        int sourceLength = 3;
        int targetLength = 2;
        int width = 2;
        int degree = 4;

        int expectedCardinality = expectedCardinalityOfRectangularPathSet(sourceLength, targetLength, width, degree);
        PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> pti =
                rectangularPathSet(sourceLength, targetLength, width, degree);

        Set<PathReference> paths = new HashSet<>(expectedCardinality);
        for (int i = 0; i < expectedCardinality; i++) {
            assertThat(pti.hasNext()).isTrue();
            PathReference nextPath = pti.next();
            assertThat(nextPath).isNotIn(paths);
            paths.add(nextPath);
        }
        assertThat(pti.hasNext()).isFalse();
    }

    private PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> singlePath(int sourceLength, int targetLength) {
        int totalLength = sourceLength + targetLength;

        MemoryTracker mt = new LocalMemoryTracker();

        HeapTrackingLongObjectHashMap<HeapTrackingArrayList<PathTraceStep>> sourcePathTraceData =
                HeapTrackingCollections.newLongObjectMap(mt);

        HeapTrackingLongObjectHashMap<HeapTrackingArrayList<PathTraceStep>> targetPathTraceData =
                HeapTrackingCollections.newLongObjectMap(mt);

        LongIterator single = longIterator(sourceLength);

        for (long i = sourceLength; i > 0; i--) {
            HeapTrackingArrayList<PathTraceStep> sourceStep = HeapTrackingCollections.newArrayList(mt);
            sourceStep.add(new PathTraceStep(i - 1, i - 1));
            sourcePathTraceData.put(i, sourceStep);
        }

        for (long i = sourceLength; i < totalLength; i++) {
            HeapTrackingArrayList<PathTraceStep> targetStep = HeapTrackingCollections.newArrayList(mt);
            targetStep.add(new PathTraceStep(i, i + 1));
            targetPathTraceData.put(i, targetStep);
        }

        var pti = new PathTracingIterator.MultiPathTracingIterator(
                single, sourceLength, targetLength, sourcePathTraceData, targetPathTraceData);
        return pti;
    }

    private PathTracingIterator<HeapTrackingArrayList<PathTraceStep>> rectangularPathSet(
            int sourceLength, int targetLength, int width, int degree) {
        MemoryTracker mt = new LocalMemoryTracker();

        HeapTrackingLongObjectHashMap<HeapTrackingArrayList<PathTraceStep>> sourcePathTraceData =
                HeapTrackingCollections.newLongObjectMap(mt);

        HeapTrackingLongObjectHashMap<HeapTrackingArrayList<PathTraceStep>> targetPathTraceData =
                HeapTrackingCollections.newLongObjectMap(mt);

        int incrementingNodeId = 0;
        int incrementingRelId = 0;

        // The first level is the source node only
        long[] previousLevel = {incrementingNodeId++};
        long[] currentLevel = new long[width];

        // Now add all levels from the source node to the intersection level (both exclusive) to the sourcePathTraceData
        for (int i = 1; i < sourceLength; i++) {
            for (int j = 0; j < width; j++) {
                int nodeId = incrementingNodeId++;
                currentLevel[j] = nodeId;
                HeapTrackingArrayList<PathTraceStep> pathsToNode = HeapTrackingCollections.newArrayList(width, mt);

                for (int k = 0; k < degree; k++) {
                    pathsToNode.add(new PathTraceStep(incrementingRelId++, previousLevel[k % previousLevel.length]));
                }
                sourcePathTraceData.put(nodeId, pathsToNode);
            }
            previousLevel = currentLevel;
            currentLevel = new long[width];
        }

        // We save the currentLevel for the intersection later.
        long[] lastSourceLevel = previousLevel;

        // Now we add the levels on the target side
        previousLevel = new long[] {incrementingNodeId++};

        for (int i = 1; i < targetLength; i++) {
            for (int j = 0; j < width; j++) {
                int nodeId = incrementingNodeId++;
                currentLevel[j] = nodeId;
                HeapTrackingArrayList<PathTraceStep> pathsToNode = HeapTrackingCollections.newArrayList(width, mt);

                for (int k = 0; k < degree; k++) {
                    pathsToNode.add(new PathTraceStep(incrementingRelId++, previousLevel[k % previousLevel.length]));
                }
                targetPathTraceData.put(nodeId, pathsToNode);
            }
            previousLevel = currentLevel;
            currentLevel = new long[width];
        }

        // Finally we add the intersection to both the source and target pathTraceData as well as the intersection
        // iterator
        int intersectionWidth = sourceLength == 0 || targetLength == 0 ? 1 : width;
        currentLevel = new long[intersectionWidth];

        for (int j = 0; j < intersectionWidth; j++) {
            int nodeId = incrementingNodeId++;
            currentLevel[j] = nodeId;

            if (sourceLength > 0) {
                HeapTrackingArrayList<PathTraceStep> pathsToNodeSourceSide =
                        HeapTrackingCollections.newArrayList(width, mt);

                for (int k = 0; k < degree; k++) {
                    pathsToNodeSourceSide.add(
                            new PathTraceStep(incrementingRelId++, lastSourceLevel[k % lastSourceLevel.length]));
                }
                sourcePathTraceData.put(nodeId, pathsToNodeSourceSide);
            }

            if (targetLength > 0) {
                HeapTrackingArrayList<PathTraceStep> pathsToNodeTargetSide =
                        HeapTrackingCollections.newArrayList(width, mt);

                for (int k = 0; k < degree; k++) {
                    pathsToNodeTargetSide.add(
                            new PathTraceStep(incrementingRelId++, previousLevel[k % previousLevel.length]));
                }
                targetPathTraceData.put(nodeId, pathsToNodeTargetSide);
            }
        }

        LongIterator intersectionIterator = longIterator(currentLevel);
        return new PathTracingIterator.MultiPathTracingIterator(
                intersectionIterator, sourceLength, targetLength, sourcePathTraceData, targetPathTraceData);
    }

    private int expectedCardinalityOfRectangularPathSet(int sourceLength, int targetLength, int width, int degree) {
        int nSourcePartsPerIntersectionNode = (int) Math.pow(degree, sourceLength);
        int nTargetPartsPerIntersectionNode = (int) Math.pow(degree, targetLength);

        int intersectionWidth = sourceLength == 0 || targetLength == 0 ? 1 : width;

        return nSourcePartsPerIntersectionNode * nTargetPartsPerIntersectionNode * intersectionWidth;
    }

    private long[] longRange(long exclusiveStop) {
        return longRange(0, exclusiveStop);
    }

    private long[] longRange(long inclusiveStart, long exclusiveStop) {
        long[] ls = new long[(int) (exclusiveStop - inclusiveStart)];
        Arrays.setAll(ls, i -> inclusiveStart + (long) i);
        return ls;
    }

    private LongIterator longIterator(long... ls) {
        return new LongIterator() {
            int i = 0;

            @Override
            public long next() {
                return ls[i++];
            }

            @Override
            public boolean hasNext() {
                return i < ls.length;
            }
        };
    }
}
