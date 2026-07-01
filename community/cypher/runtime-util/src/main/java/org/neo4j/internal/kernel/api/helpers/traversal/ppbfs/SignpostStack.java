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
     * The index of each signpost in activeSignposts, relative to its NodeState parent
     */
    private final HeapTrackingIntArrayList nodeSourceSignpostIndices;

    private final PPBFSHooks hooks;

    private NodeState targetNode = null;
    /** The length of the currently traced path when projected back to the data graph */
    private int dgLength = -1;

    private int dgLengthToTarget = -1;

    private final SignpostTracking signpostTracking;

    SignpostStack(MemoryTracker memoryTracker, SignpostTracking signpostTracking, PPBFSHooks hooks) {
        this.activeSignposts = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.nodeSourceSignpostIndices = HeapTrackingIntArrayList.newIntArrayList(memoryTracker);
        this.signpostTracking = signpostTracking;
        this.hooks = hooks;
        this.nodeSourceSignpostIndices.add(-1);
    }

    public boolean hasNext() {
        return nodeSourceSignpostIndices.notEmpty();
    }

    public boolean isProtectedFromPruning() {
        return signpostTracking.isProtectedFromPruning(this);
    }
    /**
     * Remove NodeState/TwoWaySignpost references, allowing them to be garbage collected.
     * {@link #initialize} must be called after this to correctly set up the SignpostStack.
     */
    public void reset() {
        this.targetNode = null;

        this.signpostTracking.clear();
        this.activeSignposts.clear();

        this.nodeSourceSignpostIndices.clear();
        this.dgLength = -1;
        this.dgLengthToTarget = -1;
    }

    /**
     * Set up the SignpostStack ready for tracing.
     * {@link #reset} must be called prior to this if the SignpostStack has been used previously.
     */
    public void initialize(NodeState targetNode, int dgLength) {
        this.targetNode = targetNode;
        this.dgLength = dgLength;
        this.nodeSourceSignpostIndices.add(-1);
        this.dgLengthToTarget = 0;
        this.signpostTracking.onInitialized(this);
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
    public NodeState headNode() {
        return activeSignposts.isEmpty() ? targetNode : this.activeSignposts.last().prevNode;
    }

    /**
     * The bottom node of the stack (always the target node of the path)
     */
    public NodeState target() {
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

    public NodeState node(int index) {
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

    int dgLength() {
        return this.dgLength;
    }

    public void materialize(PathWriter writer) {
        for (int i = activeSignposts.size() - 1; i >= 0; i--) {
            var signpost = activeSignposts.get(i);
            signpost.materialize(writer);
        }
        writer.writeNode(targetNode.state().slotOrName(), targetNode.id());
    }

    /**
     * Push the next signpost on to the top of the stack and activate it.
     *
     * @return true if signpost found, false otherwise
     */
    public boolean pushSignpost() {
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
        signpostTracking.onPushed(signpost, this);
        hooks.pushSignpost(this);

        return true;
    }

    public boolean validate() {
        return signpostTracking.validate(this);
    }

    /**
     * Pop and deactivate the top signpost of the stack, and return it.
     * If the stack is empty, returns null
     */
    public TwoWaySignpost popSignpost() {
        this.nodeSourceSignpostIndices.removeLast();
        if (activeSignposts.isEmpty()) {
            return null;
        }

        var signpost = activeSignposts.removeLast();
        signpostTracking.onPopped(signpost, this);
        dgLengthToTarget -= signpost.dataGraphLength();

        hooks.popSignpost(this, signpost);
        return signpost;
    }

    public boolean canAbandonTraceBranch() {
        return signpostTracking.canAbandonTraceBranch(this);
    }
}
