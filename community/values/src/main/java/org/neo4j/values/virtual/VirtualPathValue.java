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

import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;

public abstract class VirtualPathValue extends VirtualValue {
    public abstract long startNodeId();

    public abstract long endNodeId();

    public abstract long[] nodeIds();

    public abstract long[] relationshipIds();

    public abstract ListValue relationshipsAsList();

    public abstract int size();

    public abstract ListValue asList();

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.PATH;
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        VirtualPathValue otherPath = (VirtualPathValue) other;
        long[] nodes = nodeIds();
        long[] relationships = relationshipIds();
        long[] otherNodes = otherPath.nodeIds();
        long[] otherRelationships = otherPath.relationshipIds();

        int x = Long.compare(nodes[0], otherNodes[0]);
        if (x == 0) {
            int i = 0;
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
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        return Comparison.from(unsafeCompareTo(other, comparator));
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (!(other instanceof VirtualPathValue that)) {
            return false;
        }
        return size() == that.size()
                && Arrays.equals(nodeIds(), that.nodeIds())
                && Arrays.equals(relationshipIds(), that.relationshipIds());
    }

    @Override
    protected int computeHashToMemoize() {
        long[] nodes = nodeIds();
        long[] relationships = relationshipIds();
        int result = Long.hashCode(nodes[0]);
        for (int i = 1; i < nodes.length; i++) {
            result += HASH_CONSTANT * (result + Long.hashCode(relationships[i - 1]));
            result += HASH_CONSTANT * (result + Long.hashCode(nodes[i]));
        }
        return result;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapPath(this);
    }

    @Override
    public String getTypeName() {
        return "Path";
    }
}
