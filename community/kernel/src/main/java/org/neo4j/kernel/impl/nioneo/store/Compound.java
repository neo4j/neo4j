/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.*;

/**
 * @author Alexander Yastrebov <yastrebov.alex@gmail.com>
 */
public final class Compound
{
    protected final Map<Integer, Object> properties;

    protected Compound()
    {
        properties = new HashMap<Integer, Object>();
    }

    protected Compound( int capacity )
    {
        properties = new HashMap<Integer, Object>( capacity );
    }

    /**
     * Returns a map representation for this compound.
     *
     * @return The map representation for this compound
     */
    public static Map<String, Object> toMap( Compound c, Map<Integer, String> nameMap )
    {
        Map<String, Object> result = new HashMap<String, Object>( c.properties.size() );
        for ( Map.Entry<Integer,Object> e : c.properties.entrySet() )
        {
            String name = nameMap.get( e.getKey() );
            Object v = e.getValue();
            if ( v instanceof Compound )
                result.put( name, toMap( (Compound)v, nameMap ) );
            else
                result.put( name, v );
        }
        return result;
    }

    /**
     * Returns a compound representation for map.
     *
     * @return The compound representation for map
     */
    public static Compound create( Map<String, Object> map, Map<String, Integer> keyIndexes )
    {
        Compound result = new Compound( map.size() );
        for ( Map.Entry<String, Object> e : map.entrySet() )
        {
            String key = e.getKey();
            Object value = e.getValue();

            ensureKeyNotNull( key, value );
            if( value == null )
                continue;

            Integer index = keyIndexes.get( key );
            if ( value instanceof Map )
                result.properties.put( index, create( (Map<String, Object>)value, keyIndexes ) );
            else
                result.properties.put( index, value );
        }
        return result;
    }

    public static Set<Integer> collectPropertyKeys( Compound c )
    {
        Set<Integer> keys = new HashSet<Integer>();

        doCollectPropertyKeys( c, keys );

        return keys;
    }

    public static Set<String> collectPropertyNames( Map<String, Object> map )
    {
        IdentityHashMap<Object, Object> cycleDetect = new IdentityHashMap<Object, Object>();
        Set<String> names = new HashSet<String>();

        cycleDetect.put( map, map );
        doCollectPropertyNames( map, names, cycleDetect );

        return names;
    }

    private static void doCollectPropertyNames( Map<String, Object> map, Set<String> result, IdentityHashMap<Object, Object> cycleDetect )
    {
        for( Map.Entry<String, Object> e : map.entrySet() )
        {
            String key = e.getKey();
            Object value = e.getValue();

            ensureKeyNotNull( key, value );
            if( value == null )
                continue;

            result.add( key );
            if ( value instanceof Map )
            {
                Map m = (Map<String, Object>)value;
                if ( cycleDetect.containsKey( m ) )
                    throw new IllegalArgumentException( "Cycle detected on compound property value" );

                cycleDetect.put( m, m );
                doCollectPropertyNames( m, result, cycleDetect );
                cycleDetect.remove( m );
            }
        }
    }

    private static void doCollectPropertyKeys( Compound c, Set<Integer> result )
    {
        for ( Map.Entry<Integer, Object> e : c.properties.entrySet() )
        {
            result.add( e.getKey() );
            if ( e.getValue() instanceof Compound )
                doCollectPropertyKeys( (Compound)e.getValue(), result );
        }
    }

    private static void ensureKeyNotNull( String key, Object value )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Null key found for value '" + value + "'");
        }
    }

    @Override
    public String toString()
    {
        return properties.toString();
    }
}