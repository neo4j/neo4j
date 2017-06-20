/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

/**
 * Comparator for any values.
 */
class AnyValueComparator implements Comparator<AnyValue>
{
    private final Comparator<Value> valueComparator;
    private final Comparator<VirtualValueGroup> virtualValueGroupComparator;

    AnyValueComparator( Comparator<Value> valueComparator,
            Comparator<VirtualValueGroup> virtualValueGroupComparator )
    {
        this.valueComparator = valueComparator;
        this.virtualValueGroupComparator = virtualValueGroupComparator;
    }

    @Override
    public int compare( AnyValue v1, AnyValue v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        boolean isValue1 = v1 instanceof Value;
        boolean isValue2 = v2 instanceof Value;

        int x = -Boolean.compare( isValue1, isValue2 );

        if ( x == 0 )
        {
            //noinspection ConstantConditions
            return isValue1 ? valueComparator.compare( (Value)v1, (Value)v2 ) :
                   compareVirtualValues( (VirtualValue)v1, (VirtualValue)v2 );
        }
        return x;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof AnyValueComparator;
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
}
