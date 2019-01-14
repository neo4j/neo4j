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
package org.neo4j.values.virtual;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

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
    public int computeHash()
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

    public ListValue keys()
    {
        String[] strings = keySet().toArray( new String[map.size()] );
        return VirtualValues.fromArray( Values.stringArray( strings ) );
    }

    public Set<String> keySet()
    {
        return map.keySet();
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
            String[] thisKeys = keySet().toArray( new String[size] );
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
    public Boolean ternaryEquals( AnyValue other )
    {
        if ( other == null || other == NO_VALUE )
        {
            return null;
        }
        else if ( !(other instanceof MapValue) )
        {
            return Boolean.FALSE;
        }
        Map<String,AnyValue> otherMap = ((MapValue) other).map;
        int size = map.size();
        if ( size != otherMap.size() )
        {
            return Boolean.FALSE;
        }
        String[] thisKeys = keySet().toArray( new String[size] );
        Arrays.sort( thisKeys, String::compareTo );
        String[] thatKeys = otherMap.keySet().toArray( new String[size] );
        Arrays.sort( thatKeys, String::compareTo );
        for ( int i = 0; i < size; i++ )
        {
            if ( thisKeys[i].compareTo( thatKeys[i] ) != 0 )
            {
                return Boolean.FALSE;
            }
        }
        Boolean equalityResult = Boolean.TRUE;

        for ( int i = 0; i < size; i++ )
        {
            String key = thisKeys[i];
            Boolean s = map.get( key ).ternaryEquals( otherMap.get( key ) );
            if ( s == null )
            {
                equalityResult = null;
            }
            else if ( !s )
            {
                return Boolean.FALSE;
            }
        }
        return equalityResult;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapMap( this );
    }

    @Override
    public String getTypeName()
    {
        return "Map";
    }

    public void foreach( BiConsumer<String,AnyValue> f )
    {
        map.forEach( f );
    }

    public Set<Map.Entry<String,AnyValue>> entrySet()
    {
        return map.entrySet();
    }

    public boolean containsKey( String key )
    {
        return map.containsKey( key );
    }

    public AnyValue get( String key )
    {
      return map.getOrDefault( key, NO_VALUE );
    }

    public Map<String,AnyValue> getMapCopy()
    {
        return new HashMap<>( map );
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( getTypeName() + "{" );
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
