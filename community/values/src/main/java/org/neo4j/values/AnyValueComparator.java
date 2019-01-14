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

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueComparator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValueGroup;

/**
 * Comparator for any values.
 */
class AnyValueComparator implements Comparator<AnyValue>, TernaryComparator<AnyValue>
{
    private final Comparator<VirtualValueGroup> virtualValueGroupComparator;
    private final ValueComparator valueComparator;

    AnyValueComparator( ValueComparator valueComparator, Comparator<VirtualValueGroup> virtualValueGroupComparator )
    {
        this.virtualValueGroupComparator = virtualValueGroupComparator;
        this.valueComparator = valueComparator;
    }

    private Comparison cmp( AnyValue v1, AnyValue v2, boolean ternary )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        // NO_VALUE is bigger than all other values, need to check for that up
        // front
        if ( v1 == v2 )
        {
            return Comparison.EQUAL;
        }
        if ( v1 == Values.NO_VALUE )
        {
            return Comparison.GREATER_THAN;
        }
        if ( v2 == Values.NO_VALUE )
        {
            return Comparison.SMALLER_THAN;
        }

        // We must handle sequences as a special case, as they can be both storable and virtual
        boolean isSequence1 = v1.isSequenceValue();
        boolean isSequence2 = v2.isSequenceValue();

        if ( isSequence1 && isSequence2 )
        {
            return Comparison.from( compareSequences( (SequenceValue) v1, (SequenceValue) v2 ) );
        }
        else if ( isSequence1 )
        {
            return Comparison.from( compareSequenceAndNonSequence( (SequenceValue) v1, v2 ) );
        }
        else if ( isSequence2 )
        {
            return Comparison.from( -compareSequenceAndNonSequence( (SequenceValue) v2, v1 ) );
        }

        // Handle remaining AnyValues
        boolean isValue1 = v1 instanceof Value;
        boolean isValue2 = v2 instanceof Value;

        int x = Boolean.compare( isValue1, isValue2 );

        if ( x == 0 )
        {
            //noinspection ConstantConditions
            // Do not turn this into ?-operator
            if ( isValue1 )
            {
                if ( ternary )
                {
                    return valueComparator.ternaryCompare( (Value) v1, (Value) v2 );
                }
                else
                {
                    return Comparison.from( valueComparator.compare( (Value) v1, (Value) v2 ) );
                }
            }
            else
            {
                // This returns int
                return Comparison.from( compareVirtualValues( (VirtualValue) v1, (VirtualValue) v2 ) );
            }

        }
        return Comparison.from( x );
    }

    @Override
    public int compare( AnyValue v1, AnyValue v2 )
    {
        return cmp( v1, v2, false ).value();
    }

    @Override
    public Comparison ternaryCompare( AnyValue v1, AnyValue v2 )
    {
        return cmp( v1, v2, true );
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
            return v1.compareTo( v2, this );
        }
        return x;
    }

    private int compareSequenceAndNonSequence( SequenceValue v1, AnyValue v2 )
    {
        boolean isValue2 = v2 instanceof Value;
        if ( isValue2 )
        {
            return -1;
        }
        else
        {
            return virtualValueGroupComparator.compare( VirtualValueGroup.LIST, ((VirtualValue) v2).valueGroup() );
        }
    }

    private int compareSequences( SequenceValue v1, SequenceValue v2 )
    {
        return v1.compareToSequence( v2, this );
    }
}
