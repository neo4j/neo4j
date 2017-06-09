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
package org.neo4j.values.virtual;

import java.util.Arrays;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

import static org.neo4j.values.virtual.ArrayHelpers.hasNullOrNoValue;
import static org.neo4j.values.virtual.ArrayHelpers.isSortedSet;

final class MapValue extends VirtualValue
{
    private final int[] keys;
    private final AnyValue[] values;

    MapValue( int[] keys, AnyValue[] values )
    {
        assert keys != null;
        assert values != null;
        assert keys.length == values.length;
        assert isSortedSet( keys );
        assert !hasNullOrNoValue( values );

        this.keys = keys;
        this.values = values;
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof MapValue) )
        {
            return false;
        }
        MapValue that = (MapValue) other;
        return size() == that.size() &&
                Arrays.equals( keys, that.keys ) &&
                Arrays.equals( values, that.values );
    }

    @Override
    public int hash()
    {
        int result = 0;
        for ( int i = 0; i < keys.length; i++ )
        {
            result += 31 * ( result + keys[i] );
            result += 31 * ( result + values[i].hashCode() );
        }
        return result;
    }

    @Override
    public void writeTo( AnyValueWriter writer )
    {
        writer.beginMap( keys.length );
        for ( int i = 0; i < keys.length; i++ )
        {
            writer.writeKeyId( keys[i] );
            values[i].writeTo( writer );
        }
        writer.endMap();
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.MAP;
    }

    public int size()
    {
        return keys.length;
    }

    public int propertyKeyId( int offset )
    {
        return keys[offset];
    }

    public AnyValue value( int offset )
    {
        return values[offset];
    }
}
