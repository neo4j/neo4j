/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.concurrent.locks.StampedLock;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

/**
 * A basic bitset.
 * <p>
 * Represented using an automatically expanding long array, with one bit per key.
 * <p>
 * Performance:
 * * put, remove, contains, size: O(1)
 * * clear: O(size/64)
 * <p>
 * Concurrency semantics:
 * * Concurrent writes synchronise and is thread-safe
 * * Concurrent reads are thread-safe and will not observe torn writes, but may become
 * out of date as soon as the operation returns
 * * Concurrent reads during write is thread-safe
 * * Bulk operations appear atomic to concurrent readers
 * * Only caveat being that the iterator is not thread-safe
 */
public class SimpleBitSet extends StampedLock implements PrimitiveIntIterable
{
    private long lastCheckPointKey;
    private long[] data;

    public SimpleBitSet( int size )
    {
        int initialCapacity = size / 64;
        int capacity = 1;
        while ( capacity < initialCapacity )
        {
            capacity <<= 1;
        }
        long stamp = writeLock();
        data = new long[capacity];
        unlockWrite( stamp );
    }

    public boolean contains( int key )
    {
        int idx = key >>> 6;
        boolean result;
        long stamp;
        do
        {
            stamp = tryOptimisticRead();
            result = data.length > idx && (data[idx] & ((1L << (key & 63)))) != 0;
        }
        while ( !validate( stamp ) );
        return result;
    }

    public void put( int key )
    {
        long stamp = writeLock();
        int idx = key >>> 6;
        ensureCapacity( idx );
        data[idx] = data[idx] | (1L << (key & 63));
        unlockWrite( stamp );
    }

    public void put( SimpleBitSet other )
    {
        long stamp = writeLock();
        ensureCapacity( other.data.length - 1 );
        for ( int i = 0; i < data.length && i < other.data.length; i++ )
        {
            data[i] = data[i] | other.data[i];
        }
        unlockWrite( stamp );
    }

    public void remove( int key )
    {
        long stamp = writeLock();
        int idx = key >>> 6;
        if ( data.length > idx )
        {
            data[idx] = data[idx] & ~(1L << (key & 63));
        }
        unlockWrite( stamp );
    }

    public void remove( SimpleBitSet other )
    {
        long stamp = writeLock();
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = data[i] & ~other.data[i];
        }
        unlockWrite( stamp );
    }

    public long checkPointAndPut( long checkPoint, int key )
    {
        // We only need to clear the bit set if it was modified since the last check point
        if ( !validate( checkPoint ) || key != lastCheckPointKey )
        {
            long stamp = writeLock();
            int idx = key >>> 6;
            if ( idx < data.length )
            {
                Arrays.fill( data, 0 );
            }
            else
            {
                int len = data.length;
                len = findNewLength( idx, len );
                data = new long[len];
            }
            data[idx] = data[idx] | (1L << (key & 63));
            lastCheckPointKey = key;
            checkPoint = tryConvertToOptimisticRead( stamp );
        }
        return checkPoint;
    }

    private int findNewLength( int idx, int len )
    {
        while ( len <= idx )
        {
            len *= 2;
        }
        return len;
    }

    public int size()
    {
        int size = 0;
        for ( long s : data )
        {
            size += Long.bitCount( s );
        }
        return size;
    }

    private void ensureCapacity( int arrayIndex )
    {
        data = Arrays.copyOf( data, findNewLength( arrayIndex, data.length ) );
    }

    //
    // Views
    //

    @Override
    public PrimitiveIntIterator iterator()
    {
        return new PrimitiveIntIterator()
        {
            private int next;
            private final int size = data.length * 64;

            {
                // Prefetch first
                while ( next < size && !contains( next ) )
                {
                    next++;
                }
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
                while ( next < size && !contains( next ) )
                {
                    next++;
                }
                return current;
            }
        };
    }
}
