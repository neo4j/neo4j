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
    public static final MemoryPool NO_TRACKING = new NoTrackingMemoryPool();
    private final List<NamedMemoryPool> pools = new CopyOnWriteArrayList<>();

    public NamedMemoryPool pool( MemoryGroup group, String name, long limit )
    {
        return this.pool( group, name, limit, true );
    }

    public NamedMemoryPool pool( MemoryGroup group, String name, long limit, boolean strict )
    {
        var pool = new MemoryGroupTracker( this, group, name, limit, strict );
        pools.add( pool );
        return pool;
    }

    public void releasePool( NamedMemoryPool pool )
    {
        pools.remove( pool );
    }

    public List<NamedMemoryPool> getPools()
    {
        return new ArrayList<>( pools );
    }

    /**
     * Constructs a new memory pool.
     *
     * @param limit of the pool, passing 0 will result in an unbounded pool
     * @param strict true if pool should restrict allocation to the provided limit
     * @return a new memory pool with the specified limit
     */
    static MemoryPool fromLimit( long limit, boolean strict )
    {
        if ( limit == 0 )
        {
            return new MemoryPoolImpl.UnboundedMemoryPool();
        }
        return new MemoryPoolImpl.BoundedMemoryPool( limit, strict );
    }

    private static class NoTrackingMemoryPool implements NamedMemoryPool
    {
        private static final String NO_TRACKING_POOL_NAME = "No tracking";

        @Override
        public MemoryGroup group()
        {
            return MemoryGroup.NO_TRACKING;
        }

        @Override
        public String name()
        {
            return NO_TRACKING_POOL_NAME;
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
    }
}
