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

import java.util.concurrent.atomic.LongAdder;

/**
 * Global memory tracker that can be used in a global multi threaded context to record
 * allocation and de-allocation of native memory.
 * @see org.neo4j.memory.MemoryAllocationTracker
 * @see MemoryTracker
 */
public class GlobalMemoryTracker implements MemoryAllocationTracker
{
    public static final GlobalMemoryTracker INSTANCE = new GlobalMemoryTracker();

    private final LongAdder allocatedBytes = new LongAdder();

    private GlobalMemoryTracker()
    {
    }

    @Override
    public long usedDirectMemory()
    {
        return allocatedBytes.sum();
    }

    @Override
    public void allocated( long bytes )
    {
        allocatedBytes.add( bytes );
    }

    @Override
    public void deallocated( long bytes )
    {
        allocatedBytes.add( -bytes );
    }
}
