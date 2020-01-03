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
package org.neo4j.kernel.impl.util.collection;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.memory.MemoryAllocationTracker;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.requirePositive;

public class CapacityLimitingBlockAllocatorDecorator implements OffHeapBlockAllocator
{
    private final OffHeapBlockAllocator impl;
    private final long maxMemory;
    private final AtomicLong usedMemory = new AtomicLong();

    public CapacityLimitingBlockAllocatorDecorator( OffHeapBlockAllocator impl, long maxMemory )
    {
        this.impl = requireNonNull( impl );
        this.maxMemory = requirePositive( maxMemory );
    }

    @Override
    public MemoryBlock allocate( long size, MemoryAllocationTracker tracker )
    {
        while ( true )
        {
            final long usedMemoryBefore = usedMemory.get();
            final long usedMemoryAfter = usedMemoryBefore + size;
            if ( usedMemoryAfter > maxMemory )
            {
                throw new RuntimeException(
                        format( "Can't allocate %d bytes due to exceeding memory limit; used=%d, max=%d", size, usedMemoryBefore, maxMemory ) );
            }
            if ( usedMemory.compareAndSet( usedMemoryBefore, usedMemoryAfter ) )
            {
                break;
            }
        }
        try
        {
            return impl.allocate( size, tracker );
        }
        catch ( Throwable t )
        {
            usedMemory.addAndGet( -size );
            throw t;
        }
    }

    @Override
    public void free( MemoryBlock block, MemoryAllocationTracker tracker )
    {
        try
        {
            impl.free( block, tracker );
        }
        finally
        {
            usedMemory.addAndGet( -block.size );
        }
    }

    @Override
    public void release()
    {
        try
        {
            impl.release();
        }
        finally
        {
            usedMemory.set( 0 );
        }
    }
}
