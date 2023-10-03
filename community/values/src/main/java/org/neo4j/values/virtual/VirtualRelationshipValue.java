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

import java.util.Comparator;
import java.util.function.Consumer;
import org.neo4j.exceptions.IncomparableValuesException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;

public abstract class VirtualRelationshipValue extends VirtualValue {
    public abstract long id();

    public abstract long startNodeId(Consumer<RelationshipVisitor> consumer);

    public abstract long endNodeId(Consumer<RelationshipVisitor> consumer);

    public final long otherNodeId(long node, Consumer<RelationshipVisitor> consumer) {
        long startNodeId = startNodeId(consumer);
        return node == startNodeId ? endNodeId(consumer) : startNodeId;
    }

    public abstract int relationshipTypeId(Consumer<RelationshipVisitor> consumer);

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        VirtualRelationshipValue otherNode = (VirtualRelationshipValue) other;
        return Long.compare(id(), otherNode.id());
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        return Comparison.from(unsafeCompareTo(other, comparator));
    }

    @Override
    protected int computeHashToMemoize() {
        return Long.hashCode(id());
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapRelationship(this);
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (!(other instanceof VirtualRelationshipValue that)) {
            return false;
        } else if (!(this instanceof RelationshipValue) && other instanceof CompositeDatabaseValue) {
            /*
             * If we get here, it means we try to compare a composite relationship with a relationship that does not have an element id.
             * It is not possible to compare those values as they might OR might not refer to the same element.
             *
             * We should never get here, as we always work with either only composite or only non-composite values.
             */
            throw new IncomparableValuesException(
                    this.getClass().getSimpleName(), other.getClass().getSimpleName());
        }
        return id() == that.id();
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.EDGE;
    }
}
