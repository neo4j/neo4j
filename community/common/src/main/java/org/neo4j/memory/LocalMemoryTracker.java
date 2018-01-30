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
package org.neo4j.memory;

/**
 * Memory allocation tracker that can be used in local context that required
 * tracking of memory that is independent from global.
 * All allocations/de-allocation reported to this trackers also will be reported to global tracker transparently.
 */
public class LocalMemoryTracker implements MemoryTracker, MemoryAllocationTracker
{
    private long allocatedBytes;
    private GlobalMemoryTracker globalTracker = GlobalMemoryTracker.INSTANCE;

    @Override
    public void allocate( long allocatedBytes )
    {
        globalTracker.allocate( allocatedBytes );
        this.allocatedBytes += allocatedBytes;
    }

    @Override
    public void deallocate( long deallocatedBytes )
    {
        globalTracker.deallocate( deallocatedBytes );
        this.allocatedBytes -= deallocatedBytes;
    }

    /**
     * @return number of locally used bytes.
     */
    @Override
    public long usedDirectMemory()
    {
        return allocatedBytes;
    }
}
