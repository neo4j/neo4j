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
package org.neo4j.memory;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.kernel.api.exceptions.Status.General.MemoryPoolOutOfMemoryError;
import static org.neo4j.util.Preconditions.requirePositive;

/**
 * A pool of memory that can be limited. The implementation is thread-safe.
 */
class MemoryPoolImpl implements MemoryPool
{
    private final AtomicLong maxMemory = new AtomicLong();
    private final AtomicLong usedHeapBytes = new AtomicLong();
    private final AtomicLong usedNativeBytes = new AtomicLong();
    private final boolean strict;
    private final String limitSettingName;

    /**
     * @param limit of the pool, passing 0 will result in an unbounded pool
     * @param strict if true enforce limit by throwing exception
     */
    MemoryPoolImpl( long limit, boolean strict, String limitSettingName )
    {
        this.limitSettingName = limitSettingName;
        this.maxMemory.set( validateSize( limit ) );
        this.strict = strict;
    }

    @Override
    public long usedHeap()
    {
        return usedHeapBytes.get();
    }

    @Override
    public long usedNative()
    {
        return usedNativeBytes.get();
    }

    @Override
    public long free()
    {
        return Math.max( 0, totalSize() - totalUsed() );
    }

    @Override
    public void releaseHeap( long bytes )
    {
        usedHeapBytes.addAndGet( -bytes );
    }

    @Override
    public void releaseNative( long bytes )
    {
        usedNativeBytes.addAndGet( -bytes );
    }

    @Override
    public void reserveHeap( long bytes )
    {
        reserveMemory( bytes, usedHeapBytes );
    }

    @Override
    public void reserveNative( long bytes )
    {
        reserveMemory( bytes, usedNativeBytes );
    }

    private void reserveMemory( long bytes, AtomicLong counter )
    {
        long max;
        long usedMemoryBefore;
        do
        {
            max = maxMemory.get();
            usedMemoryBefore = counter.get();
            if ( strict && totalUsed() + bytes > max )
            {
                throw new MemoryLimitExceededException( bytes, max, totalUsed(), MemoryPoolOutOfMemoryError, limitSettingName );
            }
        }
        while ( !counter.weakCompareAndSetVolatile( usedMemoryBefore, usedMemoryBefore + bytes ) );
    }

    @Override
    public long totalSize()
    {
        return maxMemory.get();
    }

    @Override
    public void setSize( long size )
    {
        maxMemory.set( validateSize( size ) );
    }

    private static long validateSize( long size )
    {
        if ( size == 0 )
        {
            return Long.MAX_VALUE;
        }
        return requirePositive( size );
    }
}
