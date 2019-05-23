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
package org.neo4j.memory;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Long.max;

/**
 * A {@link MemoryAllocationTracker} which is thread-safe, forwards allocations and deallocations to another {@link MemoryAllocationTracker}
 * and will register peak memory usage during its lifetime.
 */
public class ThreadSafePeakMemoryAllocationTracker implements MemoryAllocationTracker
{
    // Why AtomicLong instead of LongAdder? AtomicLong fits this use case due to:
    // - Having much faster "sum", this is used in every call to allocate/deallocate
    // - Convenient and accurate sum when making allocations to correctly register peak memory usage
    private final AtomicLong allocated = new AtomicLong();
    private final AtomicLong peak = new AtomicLong();
    private final MemoryAllocationTracker alsoReportTo;

    public ThreadSafePeakMemoryAllocationTracker( MemoryAllocationTracker alsoReportTo )
    {
        this.alsoReportTo = alsoReportTo;
    }

    @Override
    public void allocated( long bytes )
    {
        // Update allocated
        long total = allocated.addAndGet( bytes );

        // Update peak
        long currentPeak;
        long updatedPeak;
        do
        {
            currentPeak = peak.get();
            if ( currentPeak >= total )
            {
                break;
            }
            updatedPeak = max( currentPeak, total );
        }
        while ( !peak.compareAndSet( currentPeak, updatedPeak ) );

        alsoReportTo.allocated( bytes );
    }

    @Override
    public void deallocated( long bytes )
    {
        allocated.addAndGet( -bytes );
        alsoReportTo.deallocated( bytes );
    }

    @Override
    public long usedDirectMemory()
    {
        return allocated.get();
    }

    public long peakMemoryUsage()
    {
        return peak.get();
    }
}
