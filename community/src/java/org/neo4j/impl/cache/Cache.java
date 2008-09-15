/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.cache;

/**
 * Simple cache interface with add, remove, get, clear and size methods. If null
 * is passed as parameter an {@link IllegalArgumentException} is thrown.
 * <p>
 * If the cache cleans it self (for example a LIFO cache with maximum size) the
 * <CODE>elementCleaned</CODE> method is invoked. Override the default
 * implementation (that does nothing) if needed.
 * <p>
 * TODO: Create a pluggable, scalable, configurable, self analyzing/adaptive,
 * statistics/reportable cache architecture. Emil will code that in four hours
 * when he has time.
 */
public abstract class Cache<K,E>
{
    /**
     * Returns the name of the cache.
     * 
     * @return name of the cache
     */
    public abstract String getName();

    /**
     * Adds <CODE>element</CODE> to cache.
     * 
     * @param key
     *            the key for the element
     * @param element
     *            the element to cache
     */
    public abstract void add( K key, E element );

    /**
     * Removes the element for <CODE>key</CODE> from cache and returns it. If
     * the no element for <CODE>key</CODE> exists <CODE>null</CODE> is
     * returned.
     * 
     * @param key
     *            the key for the element
     * @return the removed element or <CODE>null</CODE> if element didn't
     *         exist
     */
    public abstract E remove( K key );

    /**
     * Returns the cached element for <CODE>key</CODE>. If the element isn't
     * in cache <CODE>null</CODE> is returned.
     * 
     * @param key
     *            the key for the element
     * @return the cached element or <CODE>null</CODE> if element didn't exist
     */
    public abstract E get( K key );

    /**
     * Removing all cached elements.
     */
    public abstract void clear();

    /**
     * Returns the cache size.
     * 
     * @return cache size
     */
    public abstract int size();

    /**
     * If cache is self cleaning this method will be invoked with the element
     * cleaned. Override this implementation (that does nothing) if needed.
     * 
     * @param element
     *            cache element that has been removed
     */
    protected void elementCleaned( E element )
    {
        // do nothing
    }

    public abstract int maxSize();

    public abstract void resize( int newSize );

    private boolean isAdaptive = false;

    boolean isAdaptive()
    {
        return isAdaptive;
    }

    void setAdaptiveStatus( boolean status )
    {
        isAdaptive = status;
    }
}
