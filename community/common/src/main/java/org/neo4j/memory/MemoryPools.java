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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class MemoryPools
{
    public static final ScopedMemoryPool NO_TRACKING = new NoTrackingMemoryPool();
    private final List<ScopedMemoryPool> pools = new CopyOnWriteArrayList<>();

    public GlobalMemoryGroupTracker pool( MemoryGroup group, long limit )
    {
        return this.pool( group, limit, true );
    }

    public GlobalMemoryGroupTracker pool( MemoryGroup group, long limit, boolean strict )
    {
        var pool = new GlobalMemoryGroupTracker( this, group, limit, strict );
        pools.add( pool );
        return pool;
    }

    public void registerPool( ScopedMemoryPool pool )
    {
        pools.add( pool );
    }

    public boolean unregisterPool( ScopedMemoryPool pool )
    {
        return pools.remove( pool );
    }

    public List<ScopedMemoryPool> getPools()
    {
        return new ArrayList<>( pools );
    }

    void releasePool( GlobalMemoryGroupTracker globalMemoryGroupTracker )
    {
        pools.remove( globalMemoryGroupTracker );
    }

    private static class NoTrackingMemoryPool implements ScopedMemoryPool
    {
        @Override
        public MemoryGroup group()
        {
            return MemoryGroup.NO_TRACKING;
        }

        @Override
        public void reserveHeap( long bytes )
        {
        }

        @Override
        public void reserveNative( long bytes )
        {
        }

        @Override
        public void releaseHeap( long bytes )
        {
        }

        @Override
        public void releaseNative( long bytes )
        {
        }

        @Override
        public long totalSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public long usedHeap()
        {
            return 0;
        }

        @Override
        public long usedNative()
        {
            return 0;
        }

        @Override
        public long totalUsed()
        {
            return 0;
        }

        @Override
        public long free()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void setSize( long size )
        {
        }
    }
}
