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
package org.neo4j.kernel.impl.util.collection;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

/**
 * A basic bitset.
 *
 * Space usage: 20bytes + 1 bit per key.
 *
 * Performance:
 *  * put, remove, contains, size: O(1)
 *  * clear: O(size/64)
 *
 * Concurrency semantics:
 *  * Concurrent writes may lead to data loss.
 *  * Concurrent reads is fine
 *  * Concurrent reads during write is allowed, but bulk put/remove is not atomic.
 */
public class SimpleBitSet implements PrimitiveIntIterable
{
    private long[] data;

    public SimpleBitSet( int size )
    {
        int initialCapacity = size / 64;
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        data = new long[capacity];
    }

    public boolean contains(int key)
    {
        int idx = key >>> 6;
        return data.length > idx && (data[idx] & ((1l << (key & 63)))) != 0;
    }

    public void put( int key )
    {
        int idx = key >>> 6;
        ensureCapacity(idx);
        data[idx] = data[idx] | (1l<< (key & 63));
    }

    public void put( SimpleBitSet other )
    {
        ensureCapacity( other.data.length - 1 );
        for(int i=0;i<data.length && i<other.data.length;i++)
            data[i] = data[i] | other.data[i];
    }

    public void remove( int key )
    {
        int idx = key >>> 6;
        if(data.length > idx)
        {
            data[idx] = data[idx] & ~(1l<< (key & 63));
        }
    }

    public void remove( SimpleBitSet other )
    {
        for(int i=0;i<data.length;i++)
            data[i] = data[i] & ~other.data[i];
    }

    public void clear()
    {
        for(int i=0;i<data.length;i++)
            data[i] = 0l;
    }

    public int size()
    {
        int size = 0;
        for(int i=0;i<data.length;i++)
            size += Long.bitCount( data[i] );
        return size;
    }

    private void ensureCapacity( int arrayIndex )
    {
        while(data.length <= arrayIndex)
            data = Arrays.copyOf(data, data.length * 2);
    }

    //
    // Views
    //

    @Override
    public PrimitiveIntIterator iterator()
    {
        return new PrimitiveIntIterator()
        {
            private int next = 0;
            private final int size = data.length * 64;

            {
                // Prefetch first
                while( next < size && !contains( next ) )
                    next++;
            }

            @Override
            public boolean hasNext()
            {
                return next < size;
            }

            @Override
            public int next()
            {
                int current = next;
                next++;
                while( next < size && !contains( next ) ) next++;
                return current;
            }
        };
    }
}
