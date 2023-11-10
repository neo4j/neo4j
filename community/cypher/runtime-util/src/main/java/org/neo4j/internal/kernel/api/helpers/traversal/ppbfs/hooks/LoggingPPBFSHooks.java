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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks;

import java.util.function.Supplier;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.cypher.internal.runtime.debug.DebugSupport;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeData;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost;
import org.neo4j.values.virtual.VirtualNodeValue;

public class LoggingPPBFSHooks extends PPBFSHooks {

    @Override
    public void addSourceSignpost(NodeData nodeData, TwoWaySignpost signpost, int lengthFromSource) {
        println("nodeData: " + nodeData + ", signpost: " + signpost + ", lengthFromSource" + lengthFromSource);
    }

    @Override
    public void addPropagatedSignpost(
            NodeData nodeData, TwoWaySignpost signpost, int lengthFromSource, int lengthToTarget) {
        println("nodeData: " + nodeData + ", signpost: " + signpost + ", lengthFromSource" + lengthFromSource
                + ", lengthToTarget: " + lengthToTarget);
    }

    @Override
    public void addTargetSignpost(NodeData nodeData, TwoWaySignpost signpost, int lengthToTarget) {
        println("nodeData: " + nodeData + ", signpost: " + signpost + ", lengthToTarget: " + lengthToTarget);
    }

    @Override
    public void propagateLengthPair(NodeData nodeData, int lengthFromSource, int lengthToTarget) {
        println("nodeData: " + nodeData + ", lengthFromSource: " + lengthFromSource + ", lengthToTarget: "
                + lengthToTarget);
    }

    @Override
    public void validateLengthState(NodeData nodeData, int lengthFromSource, int tracedLengthToTarget) {
        println("nodeData: " + nodeData + ", lengthFromSource: " + lengthFromSource + ", tracedLengthToTarget: "
                + tracedLengthToTarget);
    }

    @Override
    public void decrementTargetCount(NodeData nodeData, int remainingTargetCount) {
        println("node: " + nodeData + ", remainingTargetCount before: " + remainingTargetCount);
    }

    @Override
    public void removeLengthFromSignpost(TwoWaySignpost sourceSignpost, int lengthFromSource) {
        println("removeLengthFromSignpost: lengthFromSource: " + lengthFromSource + ", sourceSignpost: "
                + sourceSignpost);
    }

    @Override
    public void addVerifiedToSourceSignpost(TwoWaySignpost sourceSignpost, int lengthFromSource) {
        println("addVerifiedToSourceSignpost: lengthFromSource: " + lengthFromSource + ", sourceSignpost: "
                + sourceSignpost);
    }

    @Override
    public void skippingDuplicateRelationship(NodeData target, HeapTrackingArrayList<TwoWaySignpost> activeSignposts) {
        StringBuilder sb = new StringBuilder();

        for (int index = activeSignposts.size() - 1; index >= 0; index--) {
            var current = activeSignposts.get(index);

            if (current instanceof TwoWaySignpost.RelSignpost) {
                var id = ((TwoWaySignpost.RelSignpost) current).relId;
                var node = current.prevNode;
                sb.append("(")
                        .append(node.id())
                        .append(',')
                        .append(node.state().id())
                        .append(")-[")
                        .append(id)
                        .append("]->");
            }
        }
        sb.append("(")
                .append(target.id())
                .append(',')
                .append(target.state().id())
                .append(")");

        println("skipping subpaths with duplicate relationship: " + sb);
    }

    @Override
    public void returnPath(PathTracer.TracedPath tracedPath) {
        println("returning path: " + tracedPath.toString());
    }

    @Override
    public void invalidTrail(Supplier<PathTracer.TracedPath> getTracedPath) {
        println("invalidTrail: " + getTracedPath.get().toString());
    }

    @Override
    public void registerNodeToPropagate(NodeData nodeData, int lengthFromSource, int lengthToTarget) {
        println("nodeData: " + nodeData + ", lengthFromSource: " + lengthFromSource + ", lengthToTarget: "
                + lengthToTarget);
    }

    @Override
    public void beginPropagation(
            HeapTrackingIntObjectHashMap<HeapTrackingIntObjectHashMap<HeapTrackingUnifiedSet<NodeData>>>
                    nodesToPropagate) {
        SectionLogger minilog = println("nodesToPropagate:");
        nodesToPropagate.forEachKeyValue((totalLength, v) -> {
            v.forEachKeyValue((lengthFromSource, s) -> {
                var str = new StringBuilder();
                str.append("- totalLength: ")
                        .append(totalLength)
                        .append(", lengthFromSource: ")
                        .append(lengthFromSource)
                        .append(", nodes: [");
                s.forEach(n -> str.append(n).append(", "));
                str.append("]");
                minilog.println(str.toString());
            });
        });
        if (nodesToPropagate.isEmpty()) {
            minilog.println("(none)");
        }
    }

    @Override
    public void nextLevel(int currentDepth) {
        if (color == DebugSupport.Blue()) {
            color = DebugSupport.Yellow();
        } else {
            color = DebugSupport.Blue();
        }

        println("(level " + currentDepth + ") ");
    }

    @Override
    public void zeroHopLevel() {
        if (color == DebugSupport.Blue()) {
            color = DebugSupport.Yellow();
        } else {
            color = DebugSupport.Blue();
        }

        println("Zero-hop node juxtaposition expansion (level 0)");
    }

    private String color = DebugSupport.Blue();

    public void newRow(VirtualNodeValue node) {
        System.out.println("\n*** New row from node " + node.id() + " ***\n");
    }

    @Override
    public void finishedPropagation(HeapTrackingArrayList<NodeData> targets) {
        var str = new StringBuilder("[");
        targets.forEach(target -> str.append(target).append(", "));
        str.append("]");
        print("targets:").println(str.toString());
    }

    @Override
    public void activatingSignpost(int currentLength, TwoWaySignpost signpost) {
        println("activating signpost at length " + currentLength + ": " + signpost);
    }

    @Override
    public void deactivatingSignpost(int currentLength, TwoWaySignpost signpost) {
        println("deactivating signpost at length " + currentLength + ": " + signpost);
    }

    private SectionLogger println(String message) {
        return print(message + "\n", 4);
    }

    private SectionLogger print(String message) {
        return print(message, 4);
    }

    SectionLogger print(String message, int offset) {

        var frame = Thread.currentThread().getStackTrace()[offset];

        var qualifiedName = frame.getClassName().split("\\.");
        var simpleClassName = qualifiedName[qualifiedName.length - 1];

        var simpleName = simpleClassName + '.' + frame.getMethodName();

        var builder = new StringBuilder()
                .append(color)
                .append(DebugSupport.Bold())
                .append(simpleName)
                .append(' ')
                .append(DebugSupport.Reset())
                .append(message);
        System.out.print(builder);

        return new SectionLogger();
    }

    public class SectionLogger {
        public void println(String message) {

            var builder = new StringBuilder();
            builder.append("\t\t").append(message);
            System.out.println(builder);
        }
    }
}
