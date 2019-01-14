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
package org.neo4j.kernel.impl.locking;

import java.util.stream.Stream;

import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

public class NoOpClient implements Locks.Client
{
    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        return false;
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        return false;
    }

    @Override
    public boolean reEnterShared( ResourceType resourceType, long resourceId )
    {
        return false;
    }

    @Override
    public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
    {
        return false;
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
    }

    @Override
    public void prepare()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public int getLockSessionId()
    {
        return -1;
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return Stream.empty();
    }

    @Override
    public long activeLockCount()
    {
        return 0;
    }
}
