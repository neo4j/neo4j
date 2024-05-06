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

import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;

/**
 * A facade injected into NodeStates to allow them to initiate actions that are global to the algorithm, and access
 * cross-cutting objects like memory tracker and hooks.
 * Has a minimal interface for easier stubbing in tests.
 */
public class GlobalState {
    private final Propagator propagator;
    private final TargetTracker targetTracker;

    public final SearchMode searchMode;
    public final MemoryTracker mt;
    public final PPBFSHooks hooks;
    public final long initialCountForTargetNodes;

    private int depth = 0;

    /**
     * @param initialCountForTargetNodes Initial countdown value for each target node.
     *                                   This is 'K', both when we have SHORTEST K and SHORTEST K GROUPS. The operators
     *                                   are responsible for decrementing it in alignment with whatever semantics are
     *                                   relevant.
     */
    public GlobalState(
            Propagator propagator,
            TargetTracker targetTracker,
            SearchMode searchMode,
            MemoryTracker memoryTracker,
            PPBFSHooks hooks,
            int initialCountForTargetNodes) {
        this.propagator = propagator;
        this.targetTracker = targetTracker;
        this.searchMode = searchMode;
        this.mt = memoryTracker;
        this.hooks = hooks;
        this.initialCountForTargetNodes = initialCountForTargetNodes;
    }

    public void schedule(NodeState nodeState, int lengthFromSource, int lengthToTarget, ScheduleSource source) {
        propagator.schedule(nodeState, lengthFromSource, lengthToTarget, source);
    }

    public void incrementUnsaturatedTargets() {
        targetTracker.incrementUnsaturatedCount();
    }

    public void decrementUnsaturatedTargets() {
        targetTracker.decrementUnsaturatedCount();
    }

    public void addTarget(NodeState nodeState) {
        targetTracker.addTarget(nodeState);
    }

    public int depth() {
        return depth;
    }

    public void nextDepth() {
        depth += 1;
    }

    public enum ScheduleSource {
        SourceSignpost,
        Propagated,
        TargetSignpost,
        Validation
    }
}
