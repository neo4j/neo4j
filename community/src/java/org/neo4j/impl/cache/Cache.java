/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
public interface Cache<K,V>
{
    /**
     * Returns the name of the cache.
     * 
     * @return name of the cache
     */
    public String getName();

    /**
     * Adds <CODE>element</CODE> to cache.
     * 
     * @param key
     *            the key for the element
     * @param element
     *            the element to cache
     */
    public void put( K key, V value );

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
    public V remove( K key );

    /**
     * Returns the cached element for <CODE>key</CODE>. If the element isn't
     * in cache <CODE>null</CODE> is returned.
     * 
     * @param key
     *            the key for the element
     * @return the cached element or <CODE>null</CODE> if element didn't exist
     */
    public V get( K key );

    /**
     * Removing all cached elements.
     */
    public void clear();

    /**
     * Returns the cache size.
     * 
     * @return cache size
     */
    public int size();

    void elementCleaned( V value );
    
    public int maxSize();

    public void resize( int newSize );

    public boolean isAdaptive();

    public void setAdaptiveStatus( boolean status );
}
