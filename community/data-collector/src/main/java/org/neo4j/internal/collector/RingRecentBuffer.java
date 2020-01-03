/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.util.Preconditions;

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
    private final AtomicLong dropEvents;

    public RingRecentBuffer( int size )
    {
        if ( size > 0 )
        {
            Preconditions.requirePowerOfTwo( size );
        }

        this.size = size;
        mask = size - 1;

        //noinspection unchecked
        data = new VolatileRef[size];
        for ( int i = 0; i < size; i++ )
        {
            data[i] = new VolatileRef<>();
            data[i].produceNumber = i - size;
        }

        produceCount = new AtomicLong( 0 );
        consumeCount = new AtomicLong( 0 );
        dropEvents = new AtomicLong( 0 );
    }

    long numSilentQueryDrops()
    {
        return dropEvents.get();
    }

    /* ---- many producers ---- */

    @Override
    public void produce( T t )
    {
        if ( size == 0 )
        {
            return;
        }

        long produceNumber = produceCount.getAndIncrement();
        int offset = (int) (produceNumber & mask);
        VolatileRef<T> volatileRef = data[offset];
        if ( assertPreviousCompleted( produceNumber, volatileRef ) )
        {
            volatileRef.ref = t;
            volatileRef.produceNumber = produceNumber;
        }
        else
        {
            // If we don't manage to wait for the previous produce to complete even after
            // all the yields in `assertPreviousCompleted`, we drop `t` to avoid causing
            // a problem in db operation. We increment dropEvents to so the RecentBuffer
            // consumer can detect that there has been a drop.
            dropEvents.incrementAndGet();
        }
    }

    private boolean assertPreviousCompleted( long produceNumber, VolatileRef<T> volatileRef )
    {
        int attempts = 100;
        long prevProduceNumber = volatileRef.produceNumber;
        while ( prevProduceNumber != produceNumber - size && attempts > 0 )
        {
            // Coming in here is expected to be very rare, because it means that producers have
            // circled around the ring buffer, and the producer `size` elements ago hasn't finished
            // writing to the buffer. We yield and hope the previous produce is done when we get back.
            try
            {
                Thread.sleep(0, 1000);
            }
            catch ( InterruptedException e )
            {
                // continue
            }
            prevProduceNumber = volatileRef.produceNumber;
            attempts--;
        }
        return attempts > 0;
    }

    /* ---- single consumer ---- */

    @Override
    public void clear()
    {
        if ( size == 0 )
        {
            return;
        }

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
        if ( size == 0 )
        {
            return;
        }

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
