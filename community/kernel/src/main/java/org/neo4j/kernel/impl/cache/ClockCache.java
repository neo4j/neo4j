/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a barebones implementation of a clock cache that tries to approximate
 * LRU. The primary benefit is less synchronization required (none on get) and
 * less write operations needed for putting stuff in. The tradeoff is reduced
 * predictability on the contents at any given time, though the targeted
 * behaviour is close to LRU. Keys are always kept, so this is useful only
 * for highly utilized caches or very restricted key spaces.
 *
 * The basic structure is a ConcurrentHashMap that points to Pages which are
 * also present in a lock free queue and they contain a boolean flag and a field
 * which holds the value. Eviction happens on a clock fashion, meaning that the
 * head of the queue is popped and if the flag is false then the value is
 * evicted, otherwise the flag is set to false and the page re-inserted at the
 * end of the queue. This only happens on put, so it is synchronized. get() does
 * not lock but because of that it is possible that a value which was just
 * retrieved will not be present even if older values exist in the cache. This
 * should happen very rarely though and that's ok.
 */
public class ClockCache<K, V>
{
    private final Queue<Page<V>> clock = new ConcurrentLinkedQueue<Page<V>>();
    private final ConcurrentHashMap<K, Page<V>> cache = new ConcurrentHashMap<K, Page<V>>();
    private final int maxSize;
    private final AtomicInteger currentSize = new AtomicInteger( 0 );
    private final String name;

    /**
     * Constructs a Clock cache with the given value and size.
     *
     * @param name The name of the cache, must be non-null
     * @param size The size of the cache (the number of values at most present).
     *            Must be positive.
     */
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

    /**
     * Puts the given key-value pair in the cache. This method is synchronized
     * and calls evict if this put would make the cache exceed capacity.
     *
     * @param key The key, cannot be null
     * @param value The value, cannot be null
     */
    public synchronized void put( K key, V value )
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
            theValue = new Page<V>();
            if ( cache.putIfAbsent( key, theValue ) == null )
            {
                clock.offer( theValue );
            }
            else
            {
                System.out.println( "Ouch, for key " + key );
            }
        }
        V myValue = theValue.value.get();
        theValue.flag.set( true );
        theValue.value.set( value );
        if ( myValue == null )
        {
            if ( currentSize.incrementAndGet() > maxSize )
            {
                evict();
            }
            assert currentSize.get() <= maxSize : "put: " + currentSize.get();
        }
    }

    /**
     * Returns the value for this key. The result will be null for keys that
     * have no value present. There is no inherent differentiation between
     * values that were never present and values that were inserted but are now
     * evicted.
     *
     * @param key The key for which the value should be retrieved. Cannot be
     *            null
     * @return The value associated with the key or null if no such value could
     *         be found
     */
    public V get( K key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "cannot get null key" );
        }
        Page<V> theElement = cache.get( key );
        if ( theElement == null || theElement.value.get() == null )
        {
            return null;
        }
        V theValue = theElement.value.get();
        theElement.flag.set( true );
        if ( theValue == null )
        {
            theElement.flag.set( false );
        }
        return theValue;
    }

    private void evict()
    {
        /*
         * This is an inverse clock. Instead of moving a pointer around an array,
         * we move the array around the pointer. And what better way to do that than
         * a queue? The head of the queue is the pointer and we rotate around that until
         * we meet something that has the flag set to false - then evict that. What changes
         * under our nose is get()s, which may change a flag to true after we change it to false.
         * Note that this is supposed to be run ONLY FROM put(), which is synchronized.
         */
        Page<V> theElement = null;
        while ( ( theElement = clock.poll() ) != null && currentSize.get() > maxSize )
        {
            if ( !theElement.flag.compareAndSet( true, false ) )
            {
                V valueCleaned = theElement.value.get();
                if ( valueCleaned == null)
                {
                    theElement.flag.set( false );
                }
                else if ( theElement.value.compareAndSet( valueCleaned, null ) )
                {
                    elementCleaned( valueCleaned );
                    currentSize.decrementAndGet();
                }
            }
            clock.offer( theElement );
        }
        if ( theElement != null )
        {
            clock.offer( theElement );
        }
    }

    /**
     * Called whenever an element is evicted from the cache. This is expected to
     * be overridden by subclassing implementations expressly written for the
     * purpose of managing evicted elements. The default implementation does
     * nothing.
     *
     * <b>NOTE:</b> This is called during {@link #put(Object, Object)} which is
     * synchronized. It should be assumed that this method is also synchronized
     * on this and take care to avoid deadlocks.
     *
     * @param element The element that was evicted. When the method is called
     *            there is no reference in the cache to that element so unless
     *            one is kept outside of the cache it will be garbage collected
     *            on the next GC cycle.
     */
    protected void elementCleaned( V element )
    {
    }

    /**
     * Returns a collection of values present in the cache. This method is not
     * synchronized, so what it returns may not be an accurate view of the
     * contents of the cache at any point in time. For example, it is possible
     * that putting value A may evict value B but a call to values() in between
     * the put and the evict will have both A and B.
     *
     * @return A possibly inaccurate collection of values contained in the
     *         cache. The collection returned is not backed by the cache.
     */
    public Collection<V> values()
    {
        Set<V> toReturn = new HashSet<V>();
        for ( Page<V> page : cache.values() )
        {
            if ( page.value.get() != null )
            {
                toReturn.add( page.value.get() );
            }
        }
        return toReturn;
    }

    /**
     * Returns a collection of key/value pairs present in the cache. This method
     * is not synchronized, so what it returns may not be an accurate view of
     * the contents of the cache at any point in time. For example, it is
     * possible that putting value A may evict value B but a call to entrySet()
     * in between the put and the evict will have both A and B.
     *
     * @return A possibly inaccurate collection of key/value pairs contained in
     *         the cache. The collection returned is not backed by the cache.
     */
    public synchronized Set<Map.Entry<K, V>> entrySet()
    {
        Map<K, V> temp = new HashMap<K, V>();
        for ( K key : cache.keySet() )
        {
            Page<V> value = cache.get( key );
            if ( value.value.get() != null )
            {
                temp.put( key, value.value.get() );
            }
        }
        return temp.entrySet();
    }

    /**
     * Removes the value mapped to this key. If the mapping does not exist
     * nothing happens.
     *
     * @param key The key for which to remove the mapping. Cannot be null.
     * @return The value that was associated with this key, possibly null.
     */
    public synchronized V remove( K key )
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
        V toReturn = toRemove.value.get();
        toRemove.value.compareAndSet( toReturn, null );
        toRemove.flag.set( false );
        return toReturn;
    }

    /**
     * @return The name of the cache.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Clears the cache of all contents. After this any call to
     * {@link #get(Object)} will return null and {@link #size()} will return
     * null
     */
    public synchronized void clear()
    {
        cache.clear();
        clock.clear();
        currentSize.set( 0 );
    }

    /**
     * Returns the size of the cache, specifically the number of values present.
     * It is synchronized, so it is expected to always return a value larger
     * than 0 and at most equals to the size argument passed in the constructor.
     *
     * @return The number of values currently present in the cache.
     */
    public synchronized int size()
    {
        return currentSize.get();
    }

    private static class Page<E>
    {
        final AtomicBoolean flag = new AtomicBoolean( true );
        final AtomicReference<E> value = new AtomicReference<E>();

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
            Page<?> other = (Page<?>) obj;
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

        @Override
        public String toString()
        {
            return ( value.get() != null ? "->" : "" ) + "[Flag: " + flag + ", value: " + value.get() + "]";
        }
    }
}
