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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

public abstract class MapValue extends VirtualValue
{
    public static MapValue EMPTY = new MapValue()
    {
        @Override
        public Iterable<String> keySet()
        {
            return Collections.emptyList();
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f )
        {
            //do nothing
        }

        @Override
        public boolean containsKey( String key )
        {
            return false;
        }

        @Override
        public AnyValue get( String key )
        {
            return NO_VALUE;
        }

        @Override
        public int size()
        {
            return 0;
        }
    };

    static final class MapWrappingMapValue extends MapValue
    {
        private final Map<String,AnyValue> map;

        MapWrappingMapValue( Map<String,AnyValue> map )
        {
            this.map = map;
        }

        @Override
        public Iterable<String> keySet()
        {
            return map.keySet();
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
        {
            for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
            {
                f.accept( entry.getKey(), entry.getValue() );
            }
        }

        @Override
        public boolean containsKey( String key )
        {
            return map.containsKey( key );
        }

        @Override
        public AnyValue get( String key )
        {
            return map.getOrDefault( key, NO_VALUE );
        }

        @Override
        public int size()
        {
            return map.size();
        }
    }

    private static final class FilteringMapValue extends MapValue
    {
        private final MapValue map;
        private final BiFunction<String,AnyValue,Boolean> filter;
        private int size = -1;

        FilteringMapValue( MapValue map,
                BiFunction<String,AnyValue,Boolean> filter )
        {
            this.map = map;
            this.filter = filter;
        }

        @Override
        public Iterable<String> keySet()
        {
            List<String> keys = size >= 0 ? new ArrayList<>( size ) : new ArrayList<>();
            foreach( ( key, value ) -> {
                if ( filter.apply( key, value ) )
                {
                    keys.add( key );
                }
            } );

            return keys;
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
        {
            map.foreach( ( s, anyValue ) -> {
                if ( filter.apply( s, anyValue ) )
                {
                    f.accept( s, anyValue );
                }
            } );
        }

        @Override
        public boolean containsKey( String key )
        {
            AnyValue value = map.get( key );
            if ( value == NO_VALUE )
            {
                return false;
            }
            else
            {
                return filter.apply( key, value );
            }
        }

        @Override
        public AnyValue get( String key )
        {
            AnyValue value = map.get( key );
            if ( value == NO_VALUE )
            {
                return NO_VALUE;
            }
            else if ( filter.apply( key, value ) )
            {
                return value;
            }
            else
            {
                return NO_VALUE;
            }
        }

        @Override
        public int size()
        {
            if ( size < 0 )
            {
                size = 0;
                foreach( ( k, v ) -> {
                    if ( filter.apply( k, v ) )
                    {
                        size++;
                    }
                } );
            }
            return size;
        }
    }

    private static final class MappedMapValue extends MapValue
    {
        private final MapValue map;
        private final BiFunction<String,AnyValue,AnyValue> mapFunction;

        MappedMapValue( MapValue map,
                BiFunction<String,AnyValue,AnyValue> mapFunction )
        {
            this.map = map;
            this.mapFunction = mapFunction;
        }

        @Override
        public ListValue keys()
        {
            return map.keys();
        }

        @Override
        public Iterable<String> keySet()
        {
            return map.keySet();
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
        {
            map.foreach( ( s, anyValue ) -> f.accept( s, mapFunction.apply( s, anyValue ) ) );
        }

        @Override
        public boolean containsKey( String key )
        {
            return map.containsKey( key );
        }

        @Override
        public AnyValue get( String key )
        {
            return mapFunction.apply( key, map.get( key ) );
        }

        @Override
        public int size()
        {
            return map.size();
        }
    }

    private static final class UpdatedMapValue extends MapValue
    {
        private final MapValue map;
        private final String[] updatedKeys;
        private final AnyValue[] updatedValues;

        UpdatedMapValue( MapValue map, String[] updatedKeys, AnyValue[] updatedValues )
        {
            assert updatedKeys.length == updatedValues.length;
            assert !overlaps( map, updatedKeys );
            this.map = map;
            this.updatedKeys = updatedKeys;
            this.updatedValues = updatedValues;
        }

        private static boolean overlaps( MapValue map, String[] updatedKeys )
        {
            for ( String key : updatedKeys )
            {
                if ( map.containsKey( key ) )
                {
                    return true;
                }
            }

            return false;
        }

        @Override
        public ListValue keys()
        {
            return VirtualValues.concat( map.keys(), VirtualValues.fromArray( Values.stringArray( updatedKeys ) ) );
        }

        @Override
        public Iterable<String> keySet()
        {
            return () -> new Iterator<String>()
            {
                private Iterator<String> internal = map.keySet().iterator();
                private int index;

                @Override
                public boolean hasNext()
                {
                    if ( internal.hasNext() )
                    {
                        return true;
                    }
                    else
                    {
                        return index < updatedKeys.length;
                    }
                }

                @Override
                public String next()
                {
                    if ( internal.hasNext() )
                    {
                        return internal.next();
                    }
                    else if ( index < updatedKeys.length )
                    {
                        return updatedKeys[index++];
                    }
                    else
                    {
                        throw new NoSuchElementException();
                    }
                }
            };
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
        {
            map.foreach( f );
            for ( int i = 0; i < updatedKeys.length; i++ )
            {
                f.accept( updatedKeys[i], updatedValues[i] );
            }
        }

        @Override
        public boolean containsKey( String key )
        {
            for ( String updatedKey : updatedKeys )
            {
                if ( updatedKey.equals( key ) )
                {
                    return true;
                }
            }

            return map.containsKey( key );
        }

        @Override
        public AnyValue get( String key )
        {
            for ( int i = 0; i < updatedKeys.length; i++ )
            {
                if ( updatedKeys[i].equals( key ) )
                {
                    return updatedValues[i];
                }
            }
            return map.get( key );
        }

        @Override
        public int size()
        {
            return map.size() + updatedKeys.length;
        }
    }

    private static final class CombinedMapValue extends MapValue
    {
        private final MapValue[] maps;

        CombinedMapValue( MapValue... mapValues )
        {
            this.maps = mapValues;
        }

        @Override
        public Iterable<String> keySet()
        {
           return () -> new PrefetchingIterator<String>()
           {
               private int mapIndex;
               private Iterator<String> internal;
               private HashSet<String> seen = new HashSet<>();

               @Override
               protected String fetchNextOrNull()
               {
                   while ( mapIndex < maps.length || internal != null && internal.hasNext() )
                   {
                       if ( internal == null || !internal.hasNext() )
                       {
                           internal = maps[mapIndex++].keySet().iterator();
                       }

                       while ( internal.hasNext() )
                       {
                           String key = internal.next();
                           if ( seen.add( key ) )
                           {
                               return key;
                           }
                       }
                   }
                   return null;
               }
           };
        }

        @Override
        public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
        {
            HashSet<String> seen = new HashSet<>();
            ThrowingBiConsumer<String,AnyValue,E> consume = ( key, value ) ->
            {
                if ( seen.add( key ) )
                {
                    f.accept( key, value );
                }
            };
            for ( int i = maps.length - 1; i >= 0; i-- )
            {
                maps[i].foreach( consume );
            }
        }

        @Override
        public boolean containsKey( String key )
        {
            for ( MapValue map : maps )
            {
                if ( map.containsKey( key ) )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public AnyValue get( String key )
        {
            for ( int i = maps.length - 1; i >= 0; i-- )
            {
                AnyValue value = maps[i].get( key );
                if ( value != NO_VALUE )
                {
                    return value;
                }
            }
            return NO_VALUE;
        }

        @Override
        public int size()
        {
            int[] size = {0};
            HashSet<String> seen = new HashSet<>();
            ThrowingBiConsumer<String,AnyValue,RuntimeException> consume = ( key, value ) ->
            {
                if ( seen.add( key ) )
                {
                    size[0]++;
                }
            };
            for ( int i = maps.length - 1; i >= 0; i-- )
            {
                maps[i].foreach( consume );
            }
            return size[0];
        }
    }

    @Override
    public int computeHash()
    {
        int[] h = new int[1];
        foreach( ( key, value ) -> h[0] += key.hashCode() ^ value.hashCode() );
        return h[0];
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.beginMap( size() );
        foreach( ( s, anyValue ) -> {
            writer.writeString( s );
            anyValue.writeTo( writer );
        } );
        writer.endMap();
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

        Iterable<String> keys = keySet();
        for ( String key : keys )
        {
            if ( !get( key ).equals( that.get( key ) ) )
            {
                return false;
            }
        }

        return true;
    }

    public abstract Iterable<String> keySet();

    public ListValue keys()
    {
        String[] keys = new String[size()];
        int i = 0;
        for ( String key : keySet() )
        {
            keys[i++] = key;
        }
        return VirtualValues.fromArray( Values.stringArray( keys ) );
    }

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
            String[] thisKeys = StreamSupport.stream( keySet().spliterator(), false).toArray( String[]::new  );
            Arrays.sort( thisKeys, String::compareTo );
            String[] thatKeys = StreamSupport.stream( otherMap.keySet().spliterator(), false).toArray( String[]::new  );
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
        String[] thisKeys = StreamSupport.stream( keySet().spliterator(), false ).toArray( String[]::new );
        Arrays.sort( thisKeys, String::compareTo );
        String[] thatKeys = StreamSupport.stream( otherMap.keySet().spliterator(), false ).toArray( String[]::new );
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

    public abstract <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E;

    public abstract boolean containsKey( String key );

    public abstract AnyValue get( String key );

    public MapValue filter( BiFunction<String,AnyValue,Boolean> filterFunction )
    {
        return new FilteringMapValue( this, filterFunction );
    }

    public MapValue updatedWith( String key, AnyValue value )
    {
        AnyValue current = get( key );
        if ( current.equals( value ) )
        {
            return this;
        }
        else if ( current == NO_VALUE )
        {
            return new UpdatedMapValue( this, new String[]{key}, new AnyValue[]{value} );
        }
        else
        {
            return new MappedMapValue( this, ( k, v ) -> {
                if ( k.equals( key ) )
                {
                    return value;
                }
                else
                {
                    return v;
                }
            } );
        }
    }

    public MapValue updatedWith(  MapValue other )
    {
        return new CombinedMapValue( this, other );
    }

    public String getTypeName()
    {
        return "Map";
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( getTypeName() + "{" );
        final String[] sep = new String[]{""};
        foreach( ( key, value ) ->
        {
            sb.append( sep[0] );
            sb.append( key );
            sb.append( " -> " );
            sb.append( value );
            sep[0] = ", ";
        } );
        sb.append( '}' );
        return sb.toString();
    }

    public abstract int size();
}
