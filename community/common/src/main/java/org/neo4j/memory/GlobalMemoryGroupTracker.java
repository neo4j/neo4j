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

public class GlobalMemoryGroupTracker extends DelegatingMemoryPool implements ScopedMemoryPool
{
    private final MemoryPools pools;
    private final MemoryGroup group;

    private final List<DatabaseMemoryGroupTracker> databasePools = new CopyOnWriteArrayList<>();
    private final MemoryTracker memoryTracker;
    private final boolean trackingEnabled;

    public GlobalMemoryGroupTracker( MemoryPools pools, MemoryGroup group, long limit, boolean strict, boolean trackingEnabled, String limitSettingName )
    {
        super( new MemoryPoolImpl( limit, strict, limitSettingName ) );
        this.pools = pools;
        this.group = group;
        this.trackingEnabled = trackingEnabled;
        this.memoryTracker = trackingEnabled ? new MemoryPoolTracker( this ) : EmptyMemoryTracker.INSTANCE;
    }

    void releasePool( DatabaseMemoryGroupTracker databaseMemoryGroupTracker )
    {
        databasePools.remove( databaseMemoryGroupTracker );
    }

    @Override
    public MemoryGroup group()
    {
        return group;
    }

    @Override
    public void close()
    {
        checkState( databasePools.isEmpty(), "All sub pools must be closed before closing top pool" );
        pools.releasePool( this );
        memoryTracker.close();
    }

    public ScopedMemoryPool newDatabasePool( String name, long limit, String limitSettingName )
    {
        DatabaseMemoryGroupTracker subTracker = new DatabaseMemoryGroupTracker( this, name, limit, true, trackingEnabled, limitSettingName );
        databasePools.add( subTracker );
        return subTracker;
    }

    public MemoryTracker getPoolMemoryTracker()
    {
        return memoryTracker;
    }

    public List<ScopedMemoryPool> getDatabasePools()
    {
        return new ArrayList<>( databasePools );
    }
}
