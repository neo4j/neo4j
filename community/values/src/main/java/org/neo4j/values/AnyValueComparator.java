/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueComparator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValueGroup;

/**
 * Comparator for any values.
 */
class AnyValueComparator implements TernaryComparator<AnyValue>
{
    private final Comparator<VirtualValueGroup> virtualValueGroupComparator;
    private final ValueComparator valueComparator;

    AnyValueComparator( ValueComparator valueComparator, Comparator<VirtualValueGroup> virtualValueGroupComparator )
    {
        this.virtualValueGroupComparator = virtualValueGroupComparator;
        this.valueComparator = valueComparator;
    }

    @SuppressWarnings( "ConstantConditions" )
    @Override
    public int compare( AnyValue v1, AnyValue v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        // NO_VALUE is bigger than all other values, need to check for that up
        // front
        if ( v1 == v2 )
        {
            return 0;
        }
        if ( v1 == Values.NO_VALUE )
        {
            return 1;
        }
        if ( v2 == Values.NO_VALUE )
        {
            return -1;
        }

        // We must handle sequences as a special case, as they can be both storable and virtual
        boolean isSequence1 = v1.isSequenceValue();
        boolean isSequence2 = v2.isSequenceValue();

        if ( isSequence1 && isSequence2 )
        {
            return ((SequenceValue) v1).compareToSequence( (SequenceValue) v2, this );
        }
        else if ( isSequence1 )
        {
            return compareSequenceAndNonSequence( v2 );
        }
        else if ( isSequence2 )
        {
            return -compareSequenceAndNonSequence( v1 );
        }

        // Handle remaining AnyValues
        boolean isValue1 = v1 instanceof Value;
        boolean isValue2 = v2 instanceof Value;

        int x = Boolean.compare( isValue1, isValue2 );

        if ( x == 0 )
        {
            // Do not turn this into ?-operator
            if ( isValue1 )
            {
                return valueComparator.compare( (Value) v1, (Value) v2 );
            }
            else
            {
                // This returns int
                return compareVirtualValues( (VirtualValue) v1, (VirtualValue) v2 );
            }

        }
        return x;
    }

    @Override
    public Comparison ternaryCompare( AnyValue v1, AnyValue v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        if ( v1 == Values.NO_VALUE || v2 == Values.NO_VALUE )
        {
            return Comparison.UNDEFINED;
        }

        // We must handle sequences as a special case, as they can be both storable and virtual
        boolean isSequence1 = v1.isSequenceValue();
        boolean isSequence2 = v2.isSequenceValue();

        if ( isSequence1 && isSequence2 )
        {
            return ((SequenceValue) v1).ternaryCompareToSequence( (SequenceValue) v2, this );
        }
        else if ( isSequence1 || isSequence2 )
        {
            return Comparison.UNDEFINED;
        }

        // Handle remaining AnyValues
        boolean isValue1 = v1 instanceof Value;
        boolean isValue2 = v2 instanceof Value;

        if ( isValue1 && isValue2 )
        {
            return valueComparator.ternaryCompare( (Value) v1, (Value) v2 );
        }
        else if ( !isValue1 && !isValue2 )
        {
            return ternaryCompareVirtualValues( (VirtualValue) v1, (VirtualValue) v2 );
        }
        else
        {
            return Comparison.UNDEFINED;
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof AnyValueComparator;
    }

    @Override
    public int hashCode()
    {
        return 1;
    }

    private int compareVirtualValues( VirtualValue v1, VirtualValue v2 )
    {
        VirtualValueGroup id1 = v1.valueGroup();
        VirtualValueGroup id2 = v2.valueGroup();

        int x = virtualValueGroupComparator.compare( id1, id2 );

        if ( x == 0 )
        {
            return v1.unsafeCompareTo( v2, this );
        }
        return x;
    }

    private Comparison ternaryCompareVirtualValues( VirtualValue v1, VirtualValue v2 )
    {
        VirtualValueGroup id1 = v1.valueGroup();
        VirtualValueGroup id2 = v2.valueGroup();

        if ( id1 == id2 )
        {
            return v1.unsafeTernaryCompareTo( v2, this );
        }
        else
        {
            return Comparison.UNDEFINED;
        }
    }

    private int compareSequenceAndNonSequence( AnyValue v )
    {
        boolean isValue2 = v instanceof Value;
        if ( isValue2 )
        {
            return -1;
        }
        else
        {
            return virtualValueGroupComparator.compare( VirtualValueGroup.LIST, ((VirtualValue) v).valueGroup() );
        }
    }
}
