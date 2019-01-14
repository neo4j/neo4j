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

/**
 * Memory allocation tracker that can be used in local context that required
 * tracking of memory that is independent from global.
 * All allocations/de-allocation reported to this trackers also will be reported to global tracker transparently.
 */
public class LocalMemoryTracker implements MemoryAllocationTracker
{
    private long allocatedBytes;

    @Override
    public void allocated( long bytes )
    {
        GlobalMemoryTracker.INSTANCE.allocated( bytes );
        this.allocatedBytes += bytes;
    }

    @Override
    public void deallocated( long bytes )
    {
        GlobalMemoryTracker.INSTANCE.deallocated( bytes );
        this.allocatedBytes -= bytes;
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
