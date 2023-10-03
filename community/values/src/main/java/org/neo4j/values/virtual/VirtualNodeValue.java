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
import org.neo4j.exceptions.IncomparableValuesException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;

public abstract class VirtualNodeValue extends VirtualValue {
    public abstract long id();

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        VirtualNodeValue otherNode = (VirtualNodeValue) other;
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
        return mapper.mapNode(this);
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (!(other instanceof VirtualNodeValue that)) {
            return false;
        } else if (!(this instanceof VirtualNodeReference) && other instanceof CompositeDatabaseValue) {
            /*
             * If we get here, it means we try to compare a composite node with a node that does not have an element id.
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
        return VirtualValueGroup.NODE;
    }
}
