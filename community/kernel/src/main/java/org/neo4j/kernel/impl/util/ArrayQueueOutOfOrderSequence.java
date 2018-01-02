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

import static java.lang.String.format;

/**
 * A crude, synchronized implementation of OutOfOrderSequence. Please implement a faster one if need be.
 */
public class ArrayQueueOutOfOrderSequence implements OutOfOrderSequence
{
    // odd means updating, even means no one is updating
    private volatile int version;
    // These don't need to be volatile, reading them is "guarded" by version access
    private long highestGapFreeNumber;
    private long[] highestGapFreeMeta;
    private final SequenceArray outOfOrderQueue;
    private long[] metaArray;

    public ArrayQueueOutOfOrderSequence( long startingNumber, int initialArraySize, long[] initialMeta )
    {
        this.highestGapFreeNumber = startingNumber;
        this.highestGapFreeMeta = this.metaArray = initialMeta;
        this.outOfOrderQueue = new SequenceArray( initialMeta.length + 1, initialArraySize );
    }

    @Override
    public synchronized boolean offer( long number, long[] meta )
    {
        if ( highestGapFreeNumber + 1 == number )
        {
            version++;
            highestGapFreeNumber = outOfOrderQueue.pollHighestGapFree( number, metaArray );
            highestGapFreeMeta = highestGapFreeNumber == number ? meta : metaArray;
            version++;
            return true;
        }

        outOfOrderQueue.offer( highestGapFreeNumber, number, pack( meta ) );
        return false;
    }

    private long[] pack( long[] meta )
    {
        metaArray = meta;
        return metaArray;
    }

    @Override
    public long[] get()
    {
        long number; long[] meta;
        while ( true )
        {
            int versionBefore = version;
            if ( (versionBefore & 1) == 1 )
            {   // Someone else is updating those values as we speak, go another round
                continue;
            }

            number = highestGapFreeNumber;
            meta = highestGapFreeMeta;
            if ( version == versionBefore )
            {   // We read a consistent version of these two values
                break;
            }
        }

        return createResult( number, meta );
    }

    private long[] createResult( long number, long[] meta )
    {
        long[] result = new long[meta.length + 1];
        result[0] = number;
        System.arraycopy( meta, 0, result, 1, meta.length );
        return result;
    }

    @Override
    public long getHighestGapFreeNumber()
    {
        return highestGapFreeNumber;
    }

    @Override
    public synchronized boolean seen( long number, long[] meta )
    {
        if ( number < highestGapFreeNumber )
        {
            //assume meta data correct since they are gone
            return true;
        }

        if ( number == highestGapFreeNumber )
        {
            return highestGapFreeMeta == meta;
        }

        return outOfOrderQueue.seen( highestGapFreeNumber, number, meta );

    }

    @Override
    public synchronized void set( long number, long[] meta )
    {
        highestGapFreeNumber = number;
        highestGapFreeMeta = meta;
        outOfOrderQueue.clear();
    }

    @Override
    public synchronized String toString()
    {
        return format( "out-of-order-sequence:%d [%s]", highestGapFreeNumber, outOfOrderQueue );
    }
}
