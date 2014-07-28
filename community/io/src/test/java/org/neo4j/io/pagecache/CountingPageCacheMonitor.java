/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingPageCacheMonitor implements PageCacheMonitor
{
    private final AtomicInteger faults = new AtomicInteger();
    private final AtomicInteger evictions = new AtomicInteger();
    private final AtomicInteger pins = new AtomicInteger();
    private final AtomicInteger unpins = new AtomicInteger();
    private final AtomicInteger takenExclusiveLocks = new AtomicInteger();
    private final AtomicInteger takenSharedLocks = new AtomicInteger();
    private final AtomicInteger releasedExclusiveLocks = new AtomicInteger();
    private final AtomicInteger releasedSharedLocks = new AtomicInteger();

    @Override
    public void pageFault( long filePageId, PageSwapper swapper )
    {
        faults.getAndIncrement();
    }

    @Override
    public void evict( long filePageId, PageSwapper swapper )
    {
        evictions.getAndIncrement();
    }

    @Override
    public void pin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        pins.getAndIncrement();
        if ( exclusiveLock )
        {
            takenExclusiveLocks.getAndIncrement();
        }
        else
        {
            takenSharedLocks.getAndIncrement();
        }
    }

    @Override
    public void unpin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        unpins.getAndIncrement();
        if ( exclusiveLock )
        {
            releasedExclusiveLocks.getAndIncrement();
        }
        else
        {
            releasedSharedLocks.getAndIncrement();
        }
    }

    public int countFaults()
    {
        return faults.get();
    }

    public int countEvictions()
    {
        return evictions.get();
    }

    public int countPins()
    {
        return pins.get();
    }

    public int countUnpins()
    {
        return unpins.get();
    }

    public int countTakenExclusiveLocks()
    {
        return takenExclusiveLocks.get();
    }

    public int countTakenSharedLocks()
    {
        return takenSharedLocks.get();
    }

    public int countReleasedExclusiveLocks()
    {
        return releasedExclusiveLocks.get();
    }

    public int countReleasedSharedLocks()
    {
        return releasedSharedLocks.get();
    }
}
