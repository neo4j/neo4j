/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import org.neo4j.helpers.Factory;

/**
 * Just a little GC free queue with a fixed max size. Calls to {@link #next()} will wait for {@link #free(Object) freed}
 * instances if full. It's assumed that instances are freed in the same order they are retrieved.
 */
public class Ring<T>
{
    private final Object[] ring;
    // number of active instances == nextCursor - freeCursor
    private int nextCursor, freeCursor;

    public Ring( int size, Factory<T> factory )
    {
        this.ring = new Object[size];
        for ( int i = 0; i < size; i++ )
        {
            ring[i] = factory.newInstance();
        }
    }

    @SuppressWarnings( "unchecked" )
    public synchronized T next()
    {
        while ( nextCursor-freeCursor >= ring.length )
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
        assert nextCursor-freeCursor < ring.length;

        return (T) ring[nextCursor++%ring.length];
    }

    public synchronized void free( T item )
    {
        // They have to come back in the order they were gotten
        int index = freeCursor++%ring.length;
        assert ring[index] == item;
        notifyAll();
    }
}
