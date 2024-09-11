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

import java.util.Comparator;
import org.neo4j.collection.trackable.HeapTrackingSkipList;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

/** Encapsulates propagation-related global data & functions. */
public final class Propagator implements AutoCloseable {
    private final HeapTrackingSkipList<QueuedPropagation> nodesToPropagate;
    private final MemoryTracker memoryTracker;
    private final PPBFSHooks hooks;

    public Propagator(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.memoryTracker = memoryTracker;
        this.hooks = hooks;
        this.nodesToPropagate = new HeapTrackingSkipList<>(memoryTracker, QueuedPropagation.COMPARATOR);
    }

    public void schedule(NodeState nodeState, int sourceLength, int targetLength, GlobalState.ScheduleSource source) {
        hooks.schedule(nodeState, sourceLength, targetLength, source);
        nodesToPropagate.insert(new QueuedPropagation(sourceLength + targetLength, sourceLength, nodeState));
        memoryTracker.allocateHeap(QueuedPropagation.SHALLOW_SIZE);
    }

    /**
     * Propagates all path data that results in paths of length {@param totalLength}. To understand how it works,
     * read the PPBFS under trail semantics section in the PPBFS guide:
     * https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation
     *
     * @param totalLength
     */
    public void propagate(int totalLength) {
        hooks.propagate(nodesToPropagate, totalLength);

        while (!nodesToPropagate.isEmpty() && nodesToPropagate.peek().totalLength <= totalLength) {
            var node = nodesToPropagate.pop();
            node.propagate();
            memoryTracker.releaseHeap(QueuedPropagation.SHALLOW_SIZE);
        }
    }

    public boolean hasScheduled() {
        return !nodesToPropagate.isEmpty();
    }

    @Override
    public void close() throws Exception {
        nodesToPropagate.close();
    }

    public static class QueuedPropagation {
        private final int totalLength;
        private final int sourceLength;
        private final NodeState nodeState;

        public QueuedPropagation(int totalLength, int sourceLength, NodeState nodeState) {
            this.totalLength = totalLength;
            this.sourceLength = sourceLength;
            this.nodeState = nodeState;
        }

        @Override
        public String toString() {
            return "QueuedPropagation{" + "totalLength="
                    + totalLength + ", sourceLength="
                    + sourceLength + ", nodeState="
                    + nodeState + '}';
        }

        void propagate() {
            nodeState.propagateLengthPair(sourceLength, totalLength - sourceLength);
        }

        public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(QueuedPropagation.class);

        static Comparator<QueuedPropagation> COMPARATOR = (a, b) -> {
            var tl = Integer.compare(a.totalLength, b.totalLength);
            if (tl != 0) return tl;

            var sl = Integer.compare(a.sourceLength, b.sourceLength);
            if (sl != 0) return sl;

            var nid = Long.compare(a.nodeState.id(), b.nodeState.id());
            if (nid != 0) return nid;

            return Integer.compare(a.nodeState.state().id(), b.nodeState.state().id());
        };
    }
}
