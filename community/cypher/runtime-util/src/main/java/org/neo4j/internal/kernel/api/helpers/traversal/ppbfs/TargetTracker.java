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

import java.util.Iterator;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/** Encapsulates target-related global data & functions. */
public final class TargetTracker implements AutoCloseable {
    // the total number of discovered target nodes which have not yet exhausted their K limit
    private int unsaturatedTargets = 0;

    // set of target nodes at the current depth
    private final HeapTrackingArrayList<NodeState> targets;
    private final PPBFSHooks hooks;

    public TargetTracker(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.targets = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.hooks = hooks;
    }

    public void incrementUnsaturatedCount() {
        unsaturatedTargets += 1;
    }

    public void decrementUnsaturatedCount() {
        unsaturatedTargets -= 1;
        Preconditions.checkState(unsaturatedTargets >= 0, "Unsaturated target count should never be negative");
    }

    public boolean allKnownTargetsSaturated() {
        return unsaturatedTargets == 0;
    }

    public void addTarget(NodeState nodeState) {
        Preconditions.checkArgument(nodeState.isTarget(), "Node must be a target");
        assert !targets.contains(nodeState)
                : // contains on arraylist is expensive
                "Caller is responsible for adding any node as a target at most once per level";
        targets.add(nodeState);
        hooks.addTarget(nodeState);
    }

    public boolean hasCurrentUnsaturatedTargets() {
        for (var t : targets) {
            if (!t.isSaturated()) {
                return true;
            }
        }
        return false;
    }

    public Iterator<NodeState> iterate() {
        return targets.iterator();
    }

    public boolean hasTargets() {
        return targets.notEmpty();
    }

    public void clear() {
        targets.clear();
    }

    @Override
    public void close() throws Exception {
        targets.close();
    }
}
