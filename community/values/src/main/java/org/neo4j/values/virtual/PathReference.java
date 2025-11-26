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
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

public abstract class PathReference extends VirtualPathValue {

    public static PathReference path(long[] nodes, long[] relationships) {
        return new PathReferencePrimitive(nodes, relationships);
    }

    public static PathReference path(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships) {
        return new PathReferenceReferences(nodes, relationships);
    }

    @Override
    public abstract ListValue relationshipsAsList();

    private static class PathReferencePrimitive extends PathReference {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(PathReferencePrimitive.class);

        private final long[] nodes;
        private final long[] relationships;

        PathReferencePrimitive(long[] nodes, long[] relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE;
        }

        @Override
        public final long startNodeId() {
            return nodes[0];
        }

        @Override
        public final long endNodeId() {
            return nodes[nodes.length - 1];
        }

        @Override
        public long[] nodeIds() {
            return nodes;
        }

        @Override
        public long[] relationshipIds() {
            return relationships;
        }

        @Override
        public ListValue relationshipsAsList() {

            int size = relationships.length;
            ListValueBuilder builder = ListValueBuilder.newListBuilder(size);
            for (long relationship : relationships) {
                builder.add(VirtualValues.relationship(relationship));
            }
            return builder.build();
        }

        @Override
        public int size() {
            return relationships.length;
        }

        @Override
        public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
            writer.writePathReference(nodes, relationships);
        }

        @Override
        public ListValue asList() {

            int size = nodes.length + relationships.length;
            ListValueBuilder builder = ListValueBuilder.newListBuilder(size);
            for (int i = 0; i < size; i++) {
                if (i % 2 == 0) {
                    builder.add(VirtualValues.node(nodes[i / 2]));
                } else {
                    builder.add(VirtualValues.relationship(relationships[i / 2]));
                }
            }
            return builder.build();
        }

        @Override
        public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
            if (!(other instanceof PathReferencePrimitive otherPath)) {
                return super.unsafeCompareTo(other, comparator);
            }

            int x = Long.compare(nodes[0], otherPath.nodes[0]);
            if (x == 0) {
                int i = 0;
                long[] otherRelationships = otherPath.relationships;
                int length = Math.min(relationships.length, otherRelationships.length);

                while (x == 0 && i < length) {
                    x = Long.compare(relationships[i], otherRelationships[i]);
                    ++i;
                }

                if (x == 0) {
                    x = Integer.compare(relationships.length, otherRelationships.length);
                }
            }

            return x;
        }

        @Override
        public boolean equals(VirtualValue other) {
            if (!(other instanceof PathReferencePrimitive that)) {
                return super.equals(other);
            }
            return size() == that.size()
                    && Arrays.equals(nodeIds(), that.nodeIds())
                    && Arrays.equals(relationshipIds(), that.relationshipIds());
        }

        @Override
        protected int computeHashToMemoize() {
            int result = Long.hashCode(nodes[0]);
            int length = relationships.length;
            for (int i = 0; i < length; i++) {
                result += HASH_CONSTANT * (result + Long.hashCode(relationships[i]));
            }
            return result;
        }

        @Override
        public final long relationshipId(int index) {
            return relationships[index];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(getTypeName() + "{");
            int i = 0;
            sb.append("(").append(nodes[0]).append(")");
            for (; i < relationships.length; i++) {
                sb.append("-");
                sb.append("[").append(relationships[i]).append("]");
                sb.append("-");
                sb.append("(").append(nodes[i + 1]).append(")");
            }
            sb.append('}');
            return sb.toString();
        }
    }

    private static class PathReferenceReferences extends PathReference {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(PathReferencePrimitive.class);

        private final List<VirtualNodeValue> nodes;
        private final List<VirtualRelationshipValue> relationships;

        PathReferenceReferences(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE;
        }

        @Override
        public final long startNodeId() {
            return nodes.get(0).id();
        }

        @Override
        public long endNodeId() {
            return nodes.get(nodes.size() - 1).id();
        }

        @Override
        public long[] nodeIds() {
            long[] res = new long[nodes.size()];
            for (int i = 0; i < nodes.size(); i++) {
                res[i] = nodes.get(i).id();
            }
            return res;
        }

        @Override
        public long[] relationshipIds() {
            long[] res = new long[relationships.size()];
            for (int i = 0; i < relationships.size(); i++) {
                res[i] = relationships.get(i).id();
            }
            return res;
        }

        @Override
        public ListValue relationshipsAsList() {
            return VirtualValues.fromRelationships(relationships);
        }

        @Override
        public int size() {
            return relationships.size();
        }

        @Override
        protected int computeHashToMemoize() {
            int result = Long.hashCode(startNodeId());
            int length = size();
            for (int i = 0; i < length; i++) {
                result += HASH_CONSTANT * (result + Long.hashCode(relationshipId(i)));
            }
            return result;
        }

        @Override
        public final long relationshipId(int index) {
            // NOTE: Unless relationships is a List of type RandomAccess, this code is inefficient and should be avoided
            //       in higher-order methods like equals() and unsafeCompareTo()
            return relationships.get(index).id();
        }

        @Override
        public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
            writer.writePathReference(nodes, relationships);
        }

        @Override
        public ListValue asList() {

            int size = nodes.size() + relationships.size();
            ListValueBuilder builder = ListValueBuilder.newListBuilder(size);
            for (int i = 0; i < size; i++) {
                if (i % 2 == 0) {
                    builder.add(nodes.get(i / 2));
                } else {
                    builder.add(relationships.get(i / 2));
                }
            }
            return builder.build();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(getTypeName() + "{");
            int i = 0;
            sb.append("(").append(nodes.get(0).id()).append(")");
            for (; i < relationships.size(); i++) {
                sb.append("-");
                sb.append("[").append(relationships.get(i).id()).append("]");
                sb.append("-");
                sb.append("(").append(nodes.get(i + 1).id()).append(")");
            }
            sb.append('}');
            return sb.toString();
        }
    }
}
