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

public final class MemoryPools
{
    public static final MemoryPool NO_TRACKING = new NoTrackingMemoryPool();

    private MemoryPools()
    {
    }

    /**
     * Constructs a new memory pool.
     *
     * @param limit of the pool, passing 0 will result in an unbounded pool
     * @return a new memory pool with the specified limit
     */
    public static MemoryPool fromLimit( long limit )
    {
        if ( limit == 0 )
        {
            return new MemoryPoolImpl.UnboundedMemoryPool();
        }
        return new MemoryPoolImpl.BoundedMemoryPool( limit );
    }

    private static class NoTrackingMemoryPool implements MemoryPool
    {
        @Override
        public void reserve( long bytes )
        {
        }

        @Override
        public void release( long bytes )
        {
        }

        @Override
        public long totalSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public long used()
        {
            return 0;
        }

        @Override
        public long free()
        {
            return Long.MAX_VALUE;
        }
    }
}
