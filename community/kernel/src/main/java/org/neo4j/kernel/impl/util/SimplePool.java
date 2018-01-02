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

import org.neo4j.collection.pool.Pool;

/**
 * Just a little GC free pool with a fixed max size.
 * Calls to {@link #acquire()} will wait for {@link #release(Object) freed} instances if full.
 */
public class SimplePool<T> implements Pool<T>
{
    private final T[] items;
    private final boolean[] acquiredMarkers;

    public SimplePool( T[] items )
    {
        this.items = items;
        this.acquiredMarkers = new boolean[items.length];
    }

    @Override
    public synchronized T acquire()
    {
        int availableItemIndex;
        while ( (availableItemIndex = firstAvailableItemIndex()) == -1 )
        {
            try
            {
                wait( 10 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        acquiredMarkers[availableItemIndex] = true;
        return items[availableItemIndex];
    }

    @Override
    public synchronized void release( T item )
    {
        for ( int i = 0; i < items.length; i++ )
        {
            if ( items[i] == item )
            {
                acquiredMarkers[i] = false;
                notifyAll();
                return;
            }
        }
        throw new IllegalArgumentException( "Item " + item + " does not belong to the pool" );
    }

    private int firstAvailableItemIndex()
    {
        for ( int i = 0; i < acquiredMarkers.length; i++ )
        {
            boolean itemIsAcquired = acquiredMarkers[i];
            if ( !itemIsAcquired )
            {
                return i;
            }
        }
        return -1;
    }
}
