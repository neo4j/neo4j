/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

public interface Cache<E extends EntityWithSizeObject>
{
    /**
     * Returns the name of the cache.
     *
     * @return name of the cache
     */
    String getName();

    /**
     * Adds {@code element} to the cache. This operation is atomic and will not put the element into the cache
     * if there were a previous element with the same {@link EntityWithSizeObject#getId() id}, but instead
     * then return that element.
     *
     * @param value the element to cache.
     * @param force put the element even if there's already a value for that key in the cache.
     */
    E put( E value, boolean force );
    
    E put( E value );

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
    E remove( long key );

    /**
     * Returns the cached element for <CODE>key</CODE>. If the element isn't
     * in cache <CODE>null</CODE> is returned.
     *
     * @param key
     *            the key for the element
     * @return the cached element or <CODE>null</CODE> if element didn't exist
     */
    E get( long key );

    /**
     * Removing all cached elements.
     */
    void clear();

    /**
     * Returns the cache size. This number means different depending on cache type.
     *
     * @return cache size
     */
    long size();

    void putAll( Collection<E> values );

    long hitCount();

    long missCount();

    void updateSize( E entity, int newSize );

    void printStatistics();
    
    public static abstract class Adapter<E extends EntityWithSizeObject> implements Cache<E>
    {
        @Override
        public E put( E value )
        {
            return put( value, false );
        }
    }
}
