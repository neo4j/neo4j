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

import java.util.BitSet;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntArrayList;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
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

    private int entityCount = 0;

    /**
     * The index of each signpost in activeSignposts, relative to its NodeState parent
     */
    private final HeapTrackingIntArrayList nodeSourceSignpostIndices;

    private final BitSet targetTrails;

    private final PPBFSHooks hooks;

    private NodeState targetNode = null;
    private int dgLength = -1;
    private int dgLengthToTarget = -1;

    public final HeapTrackingLongObjectHashMap<BitSet> relationshipPresenceAtDepth;

    SignpostStack(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.activeSignposts = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.nodeSourceSignpostIndices = HeapTrackingIntArrayList.newIntArrayList(memoryTracker);
        this.relationshipPresenceAtDepth = HeapTrackingLongObjectHashMap.createLongObjectHashMap(memoryTracker);
        this.targetTrails = new BitSet();
        this.targetTrails.set(0);
        this.hooks = hooks;
        this.nodeSourceSignpostIndices.add(-1);
    }

    public boolean hasNext() {
        return nodeSourceSignpostIndices.notEmpty();
    }

    /**
     * Remove NodeState/TwoWaySignpost references, allowing them to be garbage collected.
     * {@link #initialize} must be called after this to correctly set up the SignpostStack.
     */
    public void reset() {
        this.targetNode = null;

        this.relationshipPresenceAtDepth.clear();
        this.activeSignposts.clear();
        this.entityCount = 0;

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
    public boolean pushNext() {
        var current = headNode();
        int currentIndex = this.nodeSourceSignpostIndices.last();
        int nextIndex = current.nextSignpostIndexForLength(currentIndex, lengthFromSource());
        if (nextIndex == -1) {
            return false;
        }
        var signpost = current.getSourceSignpost(nextIndex);
        activeSignposts.add(signpost);
        entityCount += signpost.entityCount();

        targetTrails.set(size(), targetTrails.get(size() - 1) && distanceToDuplicate() == 0);

        dgLengthToTarget += signpost.dataGraphLength();
        nodeSourceSignpostIndices.set(nodeSourceSignpostIndices.size() - 1, nextIndex);
        nodeSourceSignpostIndices.add(-1);

        if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
            var depths = this.relationshipPresenceAtDepth.get(rel.relId);
            if (depths == null) {
                depths = new BitSet();
                this.relationshipPresenceAtDepth.put(rel.relId, depths);
            }
            depths.set(size() - 1);
        } else if (signpost instanceof TwoWaySignpost.MultiRelSignpost multiRel) {
            for (long relId : multiRel.rels) {
                var depths = this.relationshipPresenceAtDepth.get(relId);
                if (depths == null) {
                    depths = new BitSet();
                    this.relationshipPresenceAtDepth.put(relId, depths);
                }
                // here we take advantage of the fact that multi rel signposts already have relationship uniqueness,
                // so we can compress them into a single bit of the depth bitset per rel
                depths.set(size() - 1);
            }
        }

        hooks.activateSignpost(lengthFromSource(), signpost);

        return true;
    }

    public int distanceToDuplicate() {
        if (headSignpost() instanceof TwoWaySignpost.RelSignpost rel) {
            var stack = relationshipPresenceAtDepth.get(rel.relId);
            if (stack == null) {
                return 0;
            }
            int last = stack.length();
            if (last == 0) {
                return 0;
            }

            int next = stack.previousSetBit(last - 2);
            if (next == -1) {
                return 0;
            }
            return last - 1 - next;
        } else if (headSignpost() instanceof TwoWaySignpost.MultiRelSignpost rels) {
            var min = 0;
            for (var relId : rels.rels) {
                var stack = relationshipPresenceAtDepth.get(relId);
                if (stack == null) {
                    continue;
                }
                int last = stack.length();
                if (last == 0) {
                    continue;
                }

                int next = stack.previousSetBit(last - 2);
                if (next == -1) {
                    continue;
                }

                int value = last - 1 - next;

                if (min == 0) {
                    min = value;
                } else {
                    min = Math.min(min, value);
                }
            }
            return min;
        }
        return 0;
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
        entityCount -= signpost.entityCount();
        dgLengthToTarget -= signpost.dataGraphLength();
        if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
            var depths = relationshipPresenceAtDepth.get(rel.relId);
            depths.clear(size());
            if (depths.isEmpty()) {
                relationshipPresenceAtDepth.remove(rel.relId);
            }
        } else if (signpost instanceof TwoWaySignpost.MultiRelSignpost relsSignpost) {
            for (long relId : relsSignpost.rels) {
                var depths = relationshipPresenceAtDepth.get(relId);
                depths.clear(size());
                if (depths.isEmpty()) {
                    relationshipPresenceAtDepth.remove(relId);
                }
            }
        }

        hooks.deactivateSignpost(lengthFromSource(), signpost);
        return signpost;
    }

    public boolean isTargetTrail() {
        return this.targetTrails.get(this.size());
    }
}
