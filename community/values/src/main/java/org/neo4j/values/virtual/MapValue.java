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
import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

import static org.neo4j.values.virtual.ArrayHelpers.hasNullOrNoValue;
import static org.neo4j.values.virtual.ArrayHelpers.isSortedSet;

public final class MapValue extends VirtualValue
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
        if ( other == null || other.getClass() != MapValue.class )
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
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
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

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( other == null || other.getClass() != MapValue.class )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        MapValue otherMap = (MapValue) other;
        int x = Integer.compare( this.size(), otherMap.size() );

        if ( x == 0 )
        {
            for ( int i = 0; i < keys.length; i++ )
            {
                x = Integer.compare( this.keys[i], otherMap.keys[i] );
                if ( x != 0 )
                {
                    return x;
                }
            }
            for ( int i = 0; i < values.length; i++ )
            {
                x = comparator.compare( this.values[i], otherMap.values[i] );
                if ( x != 0 )
                {
                    return x;
                }
            }
        }
        return x;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "Map{" );
        int i = 0;
        for ( ; i < keys.length - 1; i++ )
        {
            sb.append( keys[i] );
            sb.append( " -> " );
            sb.append( values[i] );
            sb.append( ", " );
        }
        if ( keys.length > 0 )
        {
            sb.append( keys[i] );
            sb.append( " -> " );
            sb.append( values[i] );
        }
        sb.append( '}' );
        return sb.toString();
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
