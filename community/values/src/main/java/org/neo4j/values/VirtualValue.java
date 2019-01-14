/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.values;

import java.util.Comparator;

import org.neo4j.values.virtual.VirtualValueGroup;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Value that can exist transiently during computations, but that cannot be stored as a property value. A Virtual
 * Value could be a NodeReference for example.
 */
public abstract class VirtualValue extends AnyValue
{
    @Override
    public final boolean eq( Object other )
    {
        if ( other == null )
        {
            return false;
        }

        if ( other instanceof SequenceValue && this.isSequenceValue() )
        {
            return ((SequenceValue) this).equals( (SequenceValue) other );
        }
        return other instanceof VirtualValue && equals( (VirtualValue) other );
    }

    public abstract boolean equals( VirtualValue other );

    @Override
    public Boolean ternaryEquals( AnyValue other )
    {
        if ( other == null || other == NO_VALUE )
        {
            return null;
        }
        if ( other instanceof SequenceValue && this.isSequenceValue() )
        {
            return ((SequenceValue) this).ternaryEquality( (SequenceValue) other );
        }
        if ( other instanceof VirtualValue && ((VirtualValue) other).valueGroup() == valueGroup() )
        {
            return equals( (VirtualValue) other );
        }
        return Boolean.FALSE;
    }

    public abstract VirtualValueGroup valueGroup();

    public abstract int compareTo( VirtualValue other, Comparator<AnyValue> comparator );
}
