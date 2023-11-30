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
    public final boolean equals(VirtualValue other) {
        if (!(other instanceof VirtualNodeValue that)) {
            return false;
        }

        return that.equals(this);
    }

    protected boolean equals(VirtualNodeValue other) {
        return id() == other.id();
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.NODE;
    }
}
