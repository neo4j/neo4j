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

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntArrayList;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * A stack of TwoWaySignposts (and thus NodeDatas) leading back from the target towards the source. This maintains
 * the state of the depth-first search through the signpost tree, so it also stores the index of the current signpost
 * per node.
 * When a signpost is added to the stack, it is activated.
 * When a signpost is popped from the stack it is deactivated.
 */
public class SignpostStack {

    /**
     * The current path of signposts from the target to the source
     */
    private final HeapTrackingArrayList<TwoWaySignpost> activeSignposts;

    /**
     * The index of each signpost in activeSignposts, relative to its NodeData parent
     */
    private final HeapTrackingIntArrayList nodeSourceSignpostIndices;

    private final PPBFSHooks hooks;

    private NodeData targetNode = null;
    private int dgLength = -1;
    private int dgLengthToTarget = -1;

    SignpostStack(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.activeSignposts = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.nodeSourceSignpostIndices = HeapTrackingIntArrayList.newIntArrayList(memoryTracker);
        this.hooks = hooks;
        this.nodeSourceSignpostIndices.add(-1);
    }

    public boolean hasNext() {
        return nodeSourceSignpostIndices.notEmpty();
    }

    /**
     * Remove NodeData/TwoWaySignpost references, allowing them to be garbage collected.
     * {@link #initialize} must be called after this to correctly set up the SignpostStack.
     */
    public void reset() {
        this.targetNode = null;

        for (var sp : this.activeSignposts) {
            sp.deactivate();
        }
        this.activeSignposts.clear();

        this.nodeSourceSignpostIndices.clear();
        this.dgLength = -1;
        this.dgLengthToTarget = -1;
    }

    /**
     * Set up the SignpostStack ready for tracing.
     * {@link #reset} must be called prior to this if the SignpostStack has been used previously.
     */
    public void initialize(NodeData targetNode, int dgLength) {
        this.targetNode = targetNode;
        this.dgLength = dgLength;
        this.nodeSourceSignpostIndices.add(-1);
        this.dgLengthToTarget = 0;
    }

    /**
     * The top signpost of the stack
     */
    public TwoWaySignpost headSignpost() {
        return activeSignposts.last();
    }

    /**
     * The top node of the stack
     */
    public NodeData headNode() {
        return activeSignposts.isEmpty() ? targetNode : this.activeSignposts.last().prevNode;
    }

    /**
     * The bottom node of the stack (always the target node of the path)
     */
    public NodeData target() {
        return this.targetNode;
    }

    /**
     * The length of the signpost stack
     */
    public int size() {
        return this.activeSignposts.size();
    }

    public TwoWaySignpost signpost(int index) {
        return this.activeSignposts.get(index);
    }

    public NodeData node(int index) {
        return index == 0 ? targetNode : signpost(index - 1).prevNode;
    }

    /**
     * The total datagraph length of the stack
     */
    public int lengthToTarget() {
        return dgLengthToTarget;
    }

    /**
     * The datagraph length from the source node to the tip of the stack
     */
    public int lengthFromSource() {
        return this.dgLength - dgLengthToTarget;
    }

    /**
     * Returns the nodes and relationships that form the current active path, top-down (from source to target).
     */
    public PathTracer.TracedPath currentPath() {
        var entities = new PathTracer.PathEntity[activeSignposts.size() + dgLengthToTarget + 1];

        int index = entities.length - 1;
        entities[index--] = PathTracer.PathEntity.fromNode(targetNode);

        for (var signpost : activeSignposts) {
            if (signpost instanceof TwoWaySignpost.RelSignpost relSignpost) {
                entities[index--] = PathTracer.PathEntity.fromRel(relSignpost);
            }

            entities[index--] = PathTracer.PathEntity.fromNode(signpost.prevNode);
        }

        Preconditions.checkState(
                index == -1,
                "Traced path length was not as expected (expected " + entities.length + " but found "
                        + (entities.length - (index + 1)) + ")");

        return new PathTracer.TracedPath(entities);
    }

    /**
     * Push the next signpost on to the top of the stack and activate it.
     *
     * @return true if signpost found, false otherwise
     */
    public boolean pushNext() {
        var current = headNode();
        int currentIndex = this.nodeSourceSignpostIndices.last();
        int nextIndex = current.nextSignpostIndexForLength(currentIndex, lengthFromSource());
        if (nextIndex == -1) {
            return false;
        }
        var signpost = current.getSourceSignpost(nextIndex);
        activeSignposts.add(signpost);

        dgLengthToTarget += signpost.dataGraphLength();
        nodeSourceSignpostIndices.set(nodeSourceSignpostIndices.size() - 1, nextIndex);
        nodeSourceSignpostIndices.add(-1);

        signpost.activate();

        hooks.activateSignpost(lengthFromSource(), signpost);

        return true;
    }

    /**
     * Pop and deactivate the top signpost of the stack, and return it.
     * If the stack is empty, returns null
     */
    public TwoWaySignpost pop() {
        this.nodeSourceSignpostIndices.removeLast();
        if (activeSignposts.isEmpty()) {
            return null;
        }

        var signpost = activeSignposts.removeLast();
        dgLengthToTarget -= signpost.dataGraphLength();
        signpost.deactivate();

        hooks.deactivateSignpost(lengthFromSource(), signpost);
        return signpost;
    }
}
