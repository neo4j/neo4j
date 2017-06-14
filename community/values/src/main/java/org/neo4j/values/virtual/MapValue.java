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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

public final class MapValue extends VirtualValue
{
    private final HashMap<String,AnyValue> map;

    MapValue( HashMap<String,AnyValue> map )
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
        HashMap<String,AnyValue> otherMap = ((MapValue) other).map;
        int compare = Integer.compare( map.size(), otherMap.size() );
        if ( compare == 0 )
        {
            Iterator<String> thisKeys = map.keySet().iterator();
            Iterator<String> thatKeys = otherMap.keySet().iterator();
            while ( thisKeys.hasNext() && thatKeys.hasNext() )
            {
                String key1 = thisKeys.next();
                String key2 = thatKeys.next();
                compare = key1.compareTo( key2 );
                if ( compare != 0 )
                {
                    return compare;
                }
            }

            Iterator<AnyValue> thisValues = map.values().iterator();
            Iterator<AnyValue> thatValues = otherMap.values().iterator();
            while ( thisValues.hasNext() && thatValues.hasNext() )
            {
                AnyValue value1 = thisValues.next();
                AnyValue value2 = thatValues.next();
                compare = comparator.compare( value1, value2 );
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
