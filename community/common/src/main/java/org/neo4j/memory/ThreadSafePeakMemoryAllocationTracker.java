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
import java.util.concurrent.atomic.LongAccumulator;

/**
 * A {@link MemoryAllocationTracker} which is thread-safe and will register peak memory usage during its lifetime.
 * Note that thread-safe and accurate is not the same thing, since we don't enforce the memory ordering peak memory is not exact, but a good enough estimate.
 */
public class ThreadSafePeakMemoryAllocationTracker implements MemoryAllocationTracker
{
    private final AtomicLong allocated = new AtomicLong();
    private final LongAccumulator peak = new LongAccumulator( Long::max, 0 );

    @Override
    public void allocated( long bytes )
    {
        // Update allocated
        long total = allocated.addAndGet( bytes );
        peak.accumulate( total );
    }

    @Override
    public void deallocated( long bytes )
    {
        allocated.addAndGet( -bytes );
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
