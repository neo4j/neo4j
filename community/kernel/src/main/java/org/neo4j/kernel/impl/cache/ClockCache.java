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
package org.neo4j.kernel.impl.cache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ClockCache<K, V>
{
    private final Queue<Page<V>> clock = new ConcurrentLinkedQueue<>();
    private final Map<K, Page<V>> cache = new ConcurrentHashMap<>();
    private final int maxSize;
    private final AtomicInteger currentSize = new AtomicInteger( 0 );
    private final String name;

    public ClockCache( String name, int size )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "name cannot be null" );
        }
        if ( size <= 0 )
        {
            throw new IllegalArgumentException( size + " is not > 0" );
        }
        this.name = name;
        this.maxSize = size;
    }

    public void put( K key, V value )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key not allowed" );
        }
        if ( value == null )
        {
            throw new IllegalArgumentException( "null value not allowed" );
        }
        Page<V> theValue = cache.get( key );
        if ( theValue == null )
        {
            theValue = new Page<>();
            cache.put( key, theValue );
            clock.offer( theValue );
        }
        if ( theValue.value == null )
        {
            currentSize.incrementAndGet();
        }
        theValue.flag = true;
        theValue.value = value;
        checkSize();
    }

    public V get( K key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "cannot get null key" );
        }
        Page<V> theElement = cache.get( key );
        if ( theElement == null || theElement.value == null )
        {
            return null;
        }
        theElement.flag = true;
        return theElement.value;
    }

    private void checkSize()
    {
        while ( currentSize.get() > maxSize )
        {
            evict();
        }
    }

    private void evict()
    {
        Page<V> theElement = null;
        while ( ( theElement = clock.poll() ) != null )
        {
            try
            {
                if ( theElement.flag )
                {
                    theElement.flag = false;
                }
                else
                {
                    V valueCleaned = theElement.value;
                    elementCleaned( valueCleaned );
                    theElement.value = null;
                    currentSize.decrementAndGet();
                    return;
                }
            }
            finally
            {
                clock.offer( theElement );
            }
        }
    }

    protected void elementCleaned( V element )
    {
        // to be overridden as required
    }

    public Collection<V> values()
    {
        Set<V> toReturn = new HashSet<>();
        for ( Page<V> page : cache.values() )
        {
            if ( page.value != null )
            {
                toReturn.add( page.value );
            }
        }
        return toReturn;
    }

    public V remove( K key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "cannot remove null key" );
        }
        Page<V> toRemove = cache.remove( key );
        if ( toRemove == null || toRemove.value == null )
        {
            return null;
        }
        currentSize.decrementAndGet();
        V toReturn = toRemove.value;
        toRemove.value = null;
        toRemove.flag = false;
        return toReturn;
    }

    public String getName()
    {
        return name;
    }

    public void clear()
    {
        cache.clear();
        clock.clear();
        currentSize.set( 0 );
    }

    public int size()
    {
        return currentSize.get();
    }

    private static class Page<E>
    {
        volatile boolean flag = true;
        volatile E value;

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( !( obj instanceof Page ) )
            {
                return false;
            }
            Page<?> other = (Page) obj;
            if ( value == null )
            {
                return other.value == null;
            }
            return value.equals( other.value );
        }

        @Override
        public int hashCode()
        {
            return value == null ? 0 : value.hashCode();
        }
    }
}