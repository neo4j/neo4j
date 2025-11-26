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
package org.neo4j.kernel.impl.util;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Comparator;
import java.util.List;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualPathValue;

@SuppressWarnings("removal")
public class PathWrappingPathValue extends PathValue {
    static final long SHALLOW_SIZE = shallowSizeOfInstance(PathWrappingPathValue.class);

    private final Path path;

    PathWrappingPathValue(Path path) {
        this.path = path;
    }

    @Override
    public final long startNodeId() {
        return path.startNode().getId();
    }

    @Override
    public final long endNodeId() {
        return path.endNode().getId();
    }

    @Override
    public NodeValue startNode() {
        return (NodeValue) ValueUtils.wrapNodeEntity(path.startNode());
    }

    @Override
    public NodeValue endNode() {
        return (NodeValue) ValueUtils.wrapNodeEntity(path.endNode());
    }

    @Override
    public NodeValue[] nodes() {
        int length = path.length() + 1;
        NodeValue[] values = new NodeValue[length];
        int i = 0;
        for (Node node : path.nodes()) {
            values[i++] = (NodeValue) ValueUtils.wrapNodeEntity(node);
        }
        return values;
    }

    @Override
    public RelationshipValue[] relationships() {
        int length = path.length();
        RelationshipValue[] values = new RelationshipValue[length];
        int i = 0;
        for (Relationship relationship : path.relationships()) {
            values[i++] = (RelationshipValue) ValueUtils.wrapRelationshipEntity(relationship);
        }
        return values;
    }

    @Override
    public int size() {
        return path.length();
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        if (other instanceof PathWrappingPathValue otherPath) {
            return unsafeCompareTo(otherPath);
        } else if (other instanceof VirtualPathValue otherPath) {
            return unsafeCompareTo(otherPath);
        } else {
            return super.unsafeCompareTo(other, comparator);
        }
    }

    private int unsafeCompareTo(PathWrappingPathValue otherPath) {
        int x = Long.compare(startNodeId(), otherPath.startNodeId());
        if (x == 0) {
            int i = 0;
            int thisSize = size();
            int otherSize = otherPath.size();
            int commonLength = Math.min(thisSize, otherSize);
            var relIterator = path.relationships().iterator();
            var otherRelIterator = otherPath.path.relationships().iterator();
            while (x == 0 && i < commonLength) {
                long relationshipId = relIterator.next().getId();
                long otherRelationshipId = otherRelIterator.next().getId();
                x = Long.compare(relationshipId, otherRelationshipId);
                ++i;
            }
            if (x == 0) {
                x = Integer.compare(thisSize, otherSize);
            }
        }
        return x;
    }

    private int unsafeCompareTo(VirtualPathValue otherPath) {
        int x = Long.compare(startNodeId(), otherPath.startNodeId());
        if (x == 0) {
            int i = 0;
            int thisSize = size();
            int otherSize = otherPath.size();
            int commonLength = Math.min(thisSize, otherSize);
            var relIterator = path.relationships().iterator();
            while (x == 0 && i < commonLength) {
                long relationshipId = relIterator.next().getId();
                long otherRelationshipId = otherPath.relationshipId(i);
                x = Long.compare(relationshipId, otherRelationshipId);
                ++i;
            }
            if (x == 0) {
                x = Integer.compare(thisSize, otherSize);
            }
        }
        return x;
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (!(other instanceof VirtualPathValue that)) {
            return false;
        }
        if (size() == that.size() && startNodeId() == that.startNodeId()) {
            if (other instanceof PathWrappingPathValue thatPathWrappingPathValue) {
                return relationshipIdsEquals(thatPathWrappingPathValue);
            } else {
                return relationshipIdsEquals(that);
            }
        }
        return false;
    }

    @Override
    protected int computeHashToMemoize() {
        int result = path.startNode().hashCode();
        for (Relationship relationship : path.relationships()) {
            result += HASH_CONSTANT * (result + relationship.hashCode());
        }
        return result;
    }

    private boolean relationshipIdsEquals(PathWrappingPathValue other) {
        // NOTE: Only safe to be called after sizes have already been compared to be equal
        var relIterator = path.relationships().iterator();
        var otherRelIterator = other.path.relationships().iterator();
        while (relIterator.hasNext()) {
            var rel = relIterator.next();
            var otherRel = otherRelIterator.next();
            if (rel.getId() != otherRel.getId()) {
                return false;
            }
        }
        return true;
    }

    private boolean relationshipIdsEquals(VirtualPathValue other) {
        // NOTE: Only safe to be called after sizes have already been compared to be equal
        int i = 0;
        for (Relationship relationship : path.relationships()) {
            if (relationship.getId() != other.relationshipId(i)) {
                return false;
            }
            i++;
        }
        return true;
    }

    @Override
    public final long relationshipId(int index) {
        // NOTE: Unless relationships is a List of type RandomAccess, this code is inefficient and should be avoided
        //       in higher-order methods like equals() and unsafeCompareTo()
        var relationships = path.relationships();
        if (relationships instanceof List) {
            return ((List<Relationship>) relationships).get(index).getId();
        }
        int i = 0;
        for (Relationship relationship : relationships) {
            if (i == index) {
                return relationship.getId();
            }
            i++;
        }
        throw new IndexOutOfBoundsException();
    }

    public Path path() {
        return path;
    }

    @Override
    public long estimatedHeapUsage() {
        int length = path.length();

        // There are many different implementations of Path, so here we are left guessing.
        // We calculate some size for each node and relationship, but that will not include any potentially cached
        // properties, labels, etc.
        return SHALLOW_SIZE
                + length * RelationshipEntityWrappingValue.SHALLOW_SIZE
                + (length + 1) * NodeEntityWrappingNodeValue.SHALLOW_SIZE;
    }
}
