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
package org.neo4j.internal.collector;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Implementation of {@link RecentBuffer} using ring buffer.
 */
public class RingRecentBuffer<T> implements RecentBuffer<T>
{
    private final int size;
    private final int mask;
    private final VolatileRef<T>[] data;

    private final AtomicLong produceCount;
    private final AtomicLong consumeCount;

    public RingRecentBuffer( int bitSize )
    {
        size = 1 << bitSize;
        mask = size - 1;

        //noinspection unchecked
        data = new VolatileRef[size];
        for ( int i = 0; i < size; i++ )
        {
            data[i] = new VolatileRef<>();
        }

        produceCount = new AtomicLong( 0 );
        consumeCount = new AtomicLong( 0 );
    }

    /* ---- many producers ---- */

    @Override
    public void produce( T t )
    {
        long produceNumber = produceCount.getAndIncrement();
        int offset = (int) (produceNumber & mask);
        VolatileRef<T> volatileRef = data[offset];
        volatileRef.ref = t;
        volatileRef.produceNumber = produceNumber;
    }

    /* ---- single consumer ---- */

    @Override
    public void clear()
    {
        for ( VolatileRef<T> volatileRef : data )
        {
            volatileRef.ref = null;
        }
        long snapshotProduce = produceCount.get();
        consumeCount.set( snapshotProduce );
    }

    @Override
    public void foreach( Consumer<T> consumer )
    {
        long snapshotProduce = produceCount.get();
        long snapshotConsume = Math.max( consumeCount.get(), snapshotProduce - size );
        for ( long i = snapshotConsume; i < snapshotProduce; i++ )
        {
            int offset = (int) (i & mask);
            VolatileRef<T> volatileRef = data[offset];
            if ( volatileRef.produceNumber < i )
            {
                return;
            }
            consumer.accept( volatileRef.ref );
        }
    }

    private static class VolatileRef<T>
    {
        private volatile T ref;
        private volatile long produceNumber;
    }
}
