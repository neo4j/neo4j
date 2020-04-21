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

import static org.neo4j.util.Preconditions.checkState;

class TopMemoryGroupTracker extends DelegatingMemoryPool implements NamedMemoryPool
{
    private final MemoryPools pools;
    private final MemoryGroup group;

    private final List<SubMemoryGroupTracker> subPools = new CopyOnWriteArrayList<>();

    TopMemoryGroupTracker( MemoryPools pools, MemoryGroup group, long limit, boolean strict )
    {
        super( new MemoryPoolImpl( limit, strict ) );
        this.pools = pools;
        this.group = group;
    }

    void releasePool( SubMemoryGroupTracker subMemoryGroupTracker )
    {
        subPools.remove( subMemoryGroupTracker );
    }

    @Override
    public MemoryGroup group()
    {
        return group;
    }

    @Override
    public String name()
    {
        return "*";
    }

    @Override
    public void close()
    {
        checkState( subPools.isEmpty(), "All sub pools must be closed before closing top pool" );
        pools.releasePool( this );
    }

    @Override
    public NamedMemoryPool newSubPool( String name, long limit, boolean strict )
    {
        SubMemoryGroupTracker subTracker = new SubMemoryGroupTracker( this, name, limit, strict );
        subPools.add( subTracker );
        return subTracker;
    }

    @Override
    public List<NamedMemoryPool> getSubPools()
    {
        return new ArrayList<>( subPools );
    }
}
