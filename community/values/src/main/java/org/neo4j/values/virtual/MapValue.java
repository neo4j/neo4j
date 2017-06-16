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
import java.util.Map;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

public final class MapValue extends VirtualValue
{
    private final Map<String,AnyValue> map;

    MapValue( Map<String,AnyValue> map )
    {
        this.map = map;
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || other.getClass() != MapValue.class )
        {
            return false;
        }
        MapValue that = (MapValue) other;
        return map.equals( that.map );
    }

    @Override
    public int hash()
    {
        return map.hashCode();
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.beginMap( map.size() );
        for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
        {
            writer.writeString( entry.getKey() );
            entry.getValue().writeTo( writer );
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
        Map<String,AnyValue> otherMap = ((MapValue) other).map;
        int size = map.size();
        int compare = Integer.compare( size(), otherMap.size() );
        if ( compare == 0 )
        {

            String[] thisKeys = map.keySet().toArray( new String[size] );
            Arrays.sort( thisKeys, String::compareTo );
            String[] thatKeys = otherMap.keySet().toArray( new String[size] );
            Arrays.sort( thatKeys, String::compareTo );
            for ( int i = 0; i < size; i++ )
            {
                compare = thisKeys[i].compareTo( thatKeys[i] );
                if ( compare != 0 )
                {
                    return compare;
                }
            }

            for ( int i = 0; i < size; i++ )
            {
                String key = thisKeys[i];
                compare = comparator.compare( map.get( key ), otherMap.get( key ) );
                if ( compare != 0 )
                {
                    return compare;
                }
            }
        }
        return compare;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "Map{" );
        String sep = "";
        for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
        {
            sb.append( sep );
            sb.append( entry.getKey() );
            sb.append( " -> " );
            sb.append( entry.getValue() );
            sep = ", ";
        }

        sb.append( '}' );
        return sb.toString();
    }

    public int size()
    {
        return map.size();
    }
}
