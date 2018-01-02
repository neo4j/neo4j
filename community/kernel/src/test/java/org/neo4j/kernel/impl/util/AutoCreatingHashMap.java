/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.function.Factory;

/**
 * {@link HashMap} that automatically creates missing values on {@link #get(Object)}, using a supplied {@link Factory}.
 * Merely for convenience. For example code like:
 *
 * <pre>
 * Map<String,Map<String,Map<String,AtomicLong>>> data = new HashMap<>();
 * ...
 * for ( String a : ... )
 * {
 *    Map<String,Map<String,AtomicLong>> levelOne = data.get( a );
 *    if ( levelOne == null )
 *    {
 *        levelOne = new HashMap<>();
 *        data.put( a, levelOne );
 *    }
 *
 *    for ( String b : ... )
 *    {
 *        Map<String,AtomicLong> levelTwo = levelOne.get( b );
 *        if ( levelTwo == null )
 *        {
 *            levelTwo = new HashMap<>();
 *            levelOne.put( b, levelTwo );
 *        }
 *
 *        for ( String c : ... )
 *        {
 *            AtomicLong count = levelTwo.get( c );
 *            if ( count == null )
 *            {
 *                count = new AtomicLong();
 *                levelTwo.put( c, count );
 *            }
 *            count.incrementAndGet();
 *        }
 *    }
 * }
 * </pre>
 *
 * Can be replaced with:
 *
 * <pre>
 * Map<String,Map<String,Map<String,AtomicLong>>> data = new AutoCreatingHashMap<>(
 *     nested( String.class, nested( String.class, values( AtomicLong.class ) ) ) );
 * ...
 * for ( String a : ... )
 * {
 *     Map<String,Map<String,AtomicLong>> levelOne = data.get( a );
 *     for ( String b : ... )
 *     {
 *         Map<String,AtomicLong> levelTwo = levelOne.get( b );
 *         for ( String c : ... )
 *         {
 *             levelTwo.get( c ).incrementAndGet();
 *         }
 *     }
 * }
 * </pre>
 *
 * An enormous improvement in readability. The only reflection used is in the {@link #values()} {@link Factory},
 * however that's just a convenience as well. Any {@link Factory} can be supplied instead.
 *
 * @author Mattias Persson
 */
public class AutoCreatingHashMap<K,V> extends HashMap<K,V>
{
    private final Factory<V> valueCreator;

    public AutoCreatingHashMap( Factory<V> valueCreator )
    {
        super();
        this.valueCreator = valueCreator;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public V get( Object key )
    {
        if ( !super.containsKey( key ) )
        {
            // Since this is just a test class, we can force all users of it to call get with K instances.
            put( (K) key, valueCreator.newInstance() );
        }
        return super.get( key );
    }

    /**
     * @return a {@link Factory} that via reflection instantiates objects of the supplied {@code valueType},
     * assuming zero-argument constructor.
     */
    public static <V> Factory<V> values( final Class<V> valueType )
    {
        return new Factory<V>()
        {
            @Override
            public V newInstance()
            {
                try
                {
                    return valueType.newInstance();
                }
                catch ( InstantiationException | IllegalAccessException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    /**
     * @return a {@link Factory} that creates {@link AutoCreatingHashMap} instances as values, and where the
     * created maps have the supplied {@code nested} {@link Factory} as value factory.
     */
    public static <K,V> Factory<Map<K,V>> nested( Class<K> keyClass, final Factory<V> nested )
    {
        return new Factory<Map<K,V>>()
        {
            @Override
            public Map<K,V> newInstance()
            {
                return new AutoCreatingHashMap<>( nested );
            }
        };
    }

    public static <V> Factory<V> dontCreate()
    {
        return new Factory<V>()
        {
            @Override
            public V newInstance()
            {
                return null;
            }
        };
    }

    public static <V> Factory<Set<V>> valuesOfTypeHashSet()
    {
        return new Factory<Set<V>>()
        {
            @Override
            public Set<V> newInstance()
            {
                return new HashSet<>();
            }
        };
    }
}
