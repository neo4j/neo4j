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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;

/** Encapsulates propagation-related global data & functions. */
public final class Propagator implements AutoCloseable {
    // Possible optimisation: Try to turn nodesToPropagateAtLengthPair into a queue and check for set deduplication
    //                        rather than Map<Set<>>
    private final HeapTrackingIntObjectHashMap<HeapTrackingIntObjectHashMap<HeapTrackingUnifiedSet<NodeState>>>
            nodesToPropagate; // Indexed with (lengthFromSource + lengthToTarget, lengthFromSource)
    private final PPBFSHooks hooks;
    private final MemoryTracker mt;

    public Propagator(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.mt = memoryTracker;
        this.hooks = hooks;
        this.nodesToPropagate = HeapTrackingIntObjectHashMap.createIntObjectHashMap(memoryTracker);
    }

    public void schedule(NodeState nodeState, int lengthFromSource, int lengthToTarget) {
        hooks.schedulePropagation(nodeState, lengthFromSource, lengthToTarget);

        nodesToPropagate
                .getIfAbsentPut(
                        lengthFromSource + lengthToTarget,
                        () -> HeapTrackingIntObjectHashMap.createIntObjectHashMap(mt))
                .getIfAbsentPut(lengthFromSource, () -> HeapTrackingUnifiedSet.createUnifiedSet(mt))
                .add(nodeState);
    }

    /**
     * Propagates all path data that results in paths of length {@param totalLength}. To understand how it works,
     * read the PPBFS under trail semantics section in the PPBFS guide:
     * https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation
     *
     * @param totalLength
     */
    public void propagate(int totalLength) {
        assert nodesToPropagate.keysView().allSatisfy(k -> k >= totalLength)
                : "The current implementation is structured such that we never should schedule nodes to propagate for a"
                        + " depth which has already passed. If we do (as the algo is implemented here), we will loop for ever.";

        hooks.propagateAll(nodesToPropagate, totalLength);

        var nodesToPropagateForLength = nodesToPropagate.get(totalLength);
        if (nodesToPropagateForLength == null) {
            return;
        }

        int minLengthFromSourceToPropagate =
                nodesToPropagateForLength.keysView().min();

        for (int lengthFromSource = minLengthFromSourceToPropagate;
                lengthFromSource <= totalLength;
                lengthFromSource++) {
            int lengthToTarget = totalLength - lengthFromSource;

            hooks.propagateAllAtLengths(lengthFromSource, lengthToTarget);

            HeapTrackingUnifiedSet<NodeState> nodesToPropagateAtLengthPair =
                    nodesToPropagateForLength.get(lengthFromSource);

            if (nodesToPropagateAtLengthPair != null) {
                while (nodesToPropagateAtLengthPair.notEmpty()) {
                    NodeState node = nodesToPropagateAtLengthPair.getLast();
                    nodesToPropagateAtLengthPair.remove(node);
                    node.propagateLengthPair(lengthFromSource, lengthToTarget);
                }

                nodesToPropagateForLength.remove(lengthFromSource);

                nodesToPropagateAtLengthPair.close();
            }
        }

        nodesToPropagate.remove(totalLength).close();
    }

    public boolean hasScheduled() {
        return nodesToPropagate.notEmpty();
    }

    @Override
    public void close() throws Exception {
        nodesToPropagate.forEach(map -> {
            map.forEach(HeapTrackingUnifiedSet::close);
            map.close();
        });
        nodesToPropagate.close();
    }
}
