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
package org.neo4j.internal.id.indexed;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

/**
 * Single Producer Multiple Consumers FIFO queue.
 */
class SpmcLongQueue implements ConcurrentLongQueue
{
    private final long idxMask;
    private final AtomicLongArray array;
    private final AtomicLong readSeq = new AtomicLong();
    private final AtomicLong writeSeq = new AtomicLong();

    SpmcLongQueue( int capacity )
    {
        requirePowerOfTwo( capacity );
        this.idxMask = capacity - 1;
        this.array = new AtomicLongArray( capacity );
    }

    @Override
    public boolean offer( long value )
    {
        final long currentWriteSeq = writeSeq.get();
        final long currentReadSeq = readSeq.get();
        final int writeIdx = idx( currentWriteSeq );
        final int readIdx = idx( currentReadSeq );
        if ( writeIdx == readIdx && currentWriteSeq != currentReadSeq )
        {
            return false;
        }
        array.set( writeIdx, value );
        writeSeq.incrementAndGet();
        return true;
    }

    @Override
    public long takeOrDefault( long defaultValue )
    {
        long currentReadSeq;
        long currentWriteSeq;
        long value;
        do
        {
            currentReadSeq = readSeq.get();
            currentWriteSeq = writeSeq.get();
            if ( currentReadSeq == currentWriteSeq )
            {
                return defaultValue;
            }
            value = array.get( idx( currentReadSeq ) );
        }
        while ( !readSeq.compareAndSet( currentReadSeq, currentReadSeq + 1 ) );
        return value;
    }

    @Override
    public int capacity()
    {
        return array.length();
    }

    @Override
    public int size()
    {
        // Why do we need max on this value? Well the size being returned is a rough estimate since we're reading two atomic longs un-atomically.
        // We may end up in a scenario where writeSeq is read and then both writeSeq as well as readSeq moves along so that when later
        // reading readSeq it will be bigger than writeSeq. This is fine, but would look strange on the receiving end, so let it be 0 instead.
        return toIntExact( max( 0, writeSeq.get() - readSeq.get() ) );
    }

    /**
     * This call is not thread-safe w/ concurrent calls to {@link #offer(long)} so external synchronization is required.
     */
    @Override
    public void clear()
    {
        readSeq.set( writeSeq.get() );
    }

    private int idx( long seq )
    {
        return toIntExact( seq & idxMask );
    }
}
