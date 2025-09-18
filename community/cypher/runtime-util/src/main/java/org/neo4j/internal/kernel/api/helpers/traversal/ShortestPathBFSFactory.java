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

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedMultiShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.ZeroLengthShortestCursor;
import org.neo4j.memory.MemoryTracker;

public class ShortestPathBFSFactory {

    public static ShortestPathBFS create(
            long sourceNodeId,
            long targetNodeId,
            int[] types,
            Direction direction,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relFilter,
            boolean stopAsapAtIntersect,
            boolean allowZeroLength,
            boolean needOnlyOnePath,
            boolean walkTraversalMode,
            ShortestPathBFS oldBfs) {
        if (sourceNodeId == targetNodeId && direction == Direction.BOTH) {
            if (oldBfs != null) {
                oldBfs.close();
            }
            if (allowZeroLength) {
                return new ZeroLengthShortestCursor(sourceNodeId);
            } else if (walkTraversalMode && needOnlyOnePath) {
                return new UndirectedSingleShortestLoopWalkCursor(
                        sourceNodeId,
                        types,
                        maxDepth,
                        read,
                        nodeCursor,
                        relCursor,
                        nodeFilter,
                        relFilter,
                        memoryTracker);
            } else if (walkTraversalMode) {
                return new UndirectedShortestLoopWalkCursor(
                        sourceNodeId,
                        types,
                        maxDepth,
                        read,
                        nodeCursor,
                        relCursor,
                        nodeFilter,
                        relFilter,
                        memoryTracker);
            } else if (needOnlyOnePath) {
                return new UndirectedSingleShortestLoopCursor(
                        sourceNodeId,
                        types,
                        maxDepth,
                        read,
                        nodeCursor,
                        relCursor,
                        nodeFilter,
                        relFilter,
                        memoryTracker);
            } else {
                return new UndirectedMultiShortestLoopCursor(
                        sourceNodeId,
                        types,
                        maxDepth,
                        read,
                        nodeCursor,
                        relCursor,
                        nodeFilter,
                        relFilter,
                        memoryTracker);
            }
        } else {
            if (oldBfs != null && oldBfs instanceof BiDirectionalBFS bfs) {
                bfs.resetForNewRow(sourceNodeId, targetNodeId, nodeCursor, relCursor, nodeFilter, relFilter);
                return bfs;
            }
            return BiDirectionalBFS.newEmptyBiDirectionalBFS(
                    sourceNodeId,
                    targetNodeId,
                    types,
                    direction,
                    maxDepth,
                    stopAsapAtIntersect,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath,
                    allowZeroLength);
        }
    }
}
