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

public class DatabaseMemoryGroupTracker extends DelegatingMemoryPool implements ScopedMemoryPool
{
    private final GlobalMemoryGroupTracker parent;
    private final String name;
    private final MemoryTracker memoryTracker;

    DatabaseMemoryGroupTracker( GlobalMemoryGroupTracker parent, String name, long limit, boolean strict, boolean trackingEnabled,
            String limitSettingName )
    {
        super( new MemoryPoolImpl( limit, strict, limitSettingName ) );
        this.parent = parent;
        this.name = name;
        this.memoryTracker = trackingEnabled ? new MemoryPoolTracker( this ) : EmptyMemoryTracker.INSTANCE;
    }

    @Override
    public MemoryGroup group()
    {
        return parent.group();
    }

    @Override
    public String databaseName()
    {
        return name;
    }

    @Override
    public void close()
    {
        parent.releasePool( this );
        parent.releaseNative( usedNative() );
        parent.releaseHeap( usedHeap() );
        memoryTracker.close();
    }

    @Override
    public MemoryTracker getPoolMemoryTracker()
    {
        return memoryTracker;
    }

    @Override
    public void reserveHeap( long bytes )
    {
        parent.reserveHeap( bytes );
        try
        {
            super.reserveHeap( bytes );
        }
        catch ( MemoryLimitExceededException e )
        {
            parent.releaseHeap( bytes );
            throw e;
        }
    }

    @Override
    public void releaseHeap( long bytes )
    {
        super.releaseHeap( bytes );
        parent.releaseHeap( bytes );
    }

    @Override
    public void reserveNative( long bytes )
    {
        parent.reserveNative( bytes );
        try
        {
            super.reserveNative( bytes );
        }
        catch ( MemoryLimitExceededException e )
        {
            parent.releaseNative( bytes );
            throw e;
        }
    }

    @Override
    public void releaseNative( long bytes )
    {
        super.releaseNative( bytes );
        parent.releaseNative( bytes );
    }
}
