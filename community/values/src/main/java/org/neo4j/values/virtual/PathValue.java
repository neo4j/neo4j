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
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.VirtualValue;

public abstract class PathValue extends VirtualPathValue {
    public abstract NodeValue startNode();

    public abstract NodeValue endNode();

    public abstract NodeValue[] nodes();

    public abstract RelationshipValue[] relationships();

    @Override
    public long startNodeId() {
        return startNode().id();
    }

    @Override
    public long endNodeId() {
        return endNode().id();
    }

    @Override
    public long[] nodeIds() {
        var nodes = nodes();
        var nodeIds = new long[nodes.length];
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = nodes[i].id();
        }
        return nodeIds;
    }

    @Override
    public long[] relationshipIds() {
        var relationionships = relationships();
        var relIds = new long[relationionships.length];
        for (int i = 0; i < relIds.length; i++) {
            relIds[i] = relationionships[i].id();
        }
        return relIds;
    }

    @Override
    public ListValue relationshipsAsList() {
        return VirtualValues.fromList(Arrays.asList(relationships()));
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (other instanceof PathValue that) {
            return size() == that.size()
                    && Arrays.equals(nodes(), that.nodes())
                    && Arrays.equals(relationships(), that.relationships());
        } else {
            return super.equals(other);
        }
    }

    @Override
    protected int computeHashToMemoize() {
        NodeValue[] nodes = nodes();
        VirtualRelationshipValue[] relationships = relationships();
        int result = nodes[0].hashCode();
        for (int i = 1; i < nodes.length; i++) {
            result += HASH_CONSTANT * (result + relationships[i - 1].hashCode());
            result += HASH_CONSTANT * (result + nodes[i].hashCode());
        }
        return result;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        if (writer.entityMode() == REFERENCE) {
            writer.writePathReference(nodeIds(), relationshipIds());
        } else {
            writer.writePath(nodes(), relationships());
        }
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        if (other instanceof PathValue otherPath) {
            NodeValue[] nodes = nodes();
            RelationshipValue[] relationships = relationships();
            NodeValue[] otherNodes = otherPath.nodes();
            RelationshipValue[] otherRelationships = otherPath.relationships();

            int x = nodes[0].unsafeCompareTo(otherNodes[0], comparator);
            if (x == 0) {
                int i = 0;
                int length = Math.min(relationships.length, otherRelationships.length);

                while (x == 0 && i < length) {
                    x = relationships[i].unsafeCompareTo(otherRelationships[i], comparator);
                    ++i;
                }

                if (x == 0) {
                    x = Integer.compare(relationships.length, otherRelationships.length);
                }
            }

            return x;
        } else {
            return super.unsafeCompareTo(other, comparator);
        }
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        return Comparison.from(unsafeCompareTo(other, comparator));
    }

    @Override
    public String toString() {
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        StringBuilder sb = new StringBuilder(getTypeName() + "{");
        int i = 0;
        for (; i < relationships.length; i++) {
            sb.append(nodes[i]);
            sb.append(relationships[i]);
        }
        sb.append(nodes[i]);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public ListValue asList() {
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        int size = nodes.length + relationships.length;
        ListValueBuilder builder = ListValueBuilder.newListBuilder(size);
        for (int i = 0; i < size; i++) {
            if (i % 2 == 0) {
                builder.add(nodes[i / 2]);
            } else {
                builder.add(relationships[i / 2]);
            }
        }
        return builder.build();
    }

    @Override
    public int size() {
        return relationships().length;
    }

    private static final long DIRECT_PATH_SHALLOW_SIZE = shallowSizeOfInstance(DirectPathValue.class);

    static class DirectPathValue extends PathValue {
        private final NodeValue[] nodes;
        private final RelationshipValue[] edges;
        private final long payloadSize;

        DirectPathValue(NodeValue[] nodes, RelationshipValue[] edges, long payloadSize) {
            assert nodes != null;
            assert edges != null;
            assert nodes.length == edges.length + 1;

            this.nodes = nodes;
            this.edges = edges;
            this.payloadSize = payloadSize;
        }

        @Override
        public NodeValue startNode() {
            return nodes[0];
        }

        @Override
        public NodeValue endNode() {
            return nodes[nodes.length - 1];
        }

        @Override
        public NodeValue[] nodes() {
            return nodes;
        }

        @Override
        public RelationshipValue[] relationships() {
            return edges;
        }

        @Override
        public long estimatedHeapUsage() {
            return DIRECT_PATH_SHALLOW_SIZE + payloadSize;
        }
    }
}
