/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

public abstract class MapValue extends VirtualValue
{
    final static class MapWrappingMapValue extends MapValue
    {
        private final Map<String,AnyValue> map;

        MapWrappingMapValue( Map<String,AnyValue> map )
        {
            this.map = map;
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

        public int size()
        {
            return map.size();
        }
    }

    @Override
    public int computeHash()
    {
        int h = 0;
        for ( Map.Entry<String,AnyValue> entry : entrySet() )
        {
            h += entry.getKey().hashCode() ^ entry.getValue().hashCode();
        }
        return h;
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( !(other instanceof MapValue) )
        {
            return false;
        }
        MapValue that = (MapValue) other;
        int size = size();
        if ( size != that.size() )
        {
            return false;
        }

        Set<String> keys = keySet();
        for ( String key : keys )
        {
            if ( get( key ).equals( that.get( key ) ) )
            {
                return false;
            }
        }

        return false;
    }

    public abstract ListValue keys();

    public abstract Set<String> keySet();


    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.MAP;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof MapValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        MapValue otherMap = (MapValue) other;
        int size = size();
        int compare = Integer.compare( size, otherMap.size() );
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
                compare = comparator.compare( get( key ), otherMap.get( key ) );
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
        MapValue otherMap = (MapValue) other;
        int size = size();
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
            Boolean s = get( key ).ternaryEquals( otherMap.get( key ) );
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

    public abstract void foreach( BiConsumer<String,AnyValue> f );


    //TODO remove??
    public abstract Set<Map.Entry<String,AnyValue>> entrySet();

    public abstract boolean containsKey( String key );

    public abstract AnyValue get( String key );

    //TODO remove
    public Map<String,AnyValue> getMapCopy()
    {

        HashMap<String,AnyValue> copy = new HashMap<>( size() );
        for ( Map.Entry<String,AnyValue> entry : entrySet() )
        {
            copy.put( entry.getKey(), entry.getValue() );
        }
        return copy;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "Map{" );
        String sep = "";
        for ( Map.Entry<String,AnyValue> entry : entrySet() )
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

    public abstract int size();
}
