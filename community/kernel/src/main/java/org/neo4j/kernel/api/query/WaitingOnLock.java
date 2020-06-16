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
package org.neo4j.kernel.api.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

class WaitingOnLock extends ExecutingQueryStatus
{
    private final LockType lockType;
    private final ResourceType resourceType;
    private final long transactionId;
    private final long[] resourceIds;
    private final long startTimeNanos;

    WaitingOnLock( LockType lockType, ResourceType resourceType, long transactionId, long[] resourceIds, long startTimeNanos )
    {
        this.lockType = lockType;
        this.resourceType = resourceType;
        this.transactionId = transactionId;
        this.resourceIds = resourceIds;
        this.startTimeNanos = startTimeNanos;
    }

    @Override
    long waitTimeNanos( long currentTimeNanos )
    {
        return currentTimeNanos - startTimeNanos;
    }

    @Override
    Map<String,Object> toMap( long currentTimeNanos )
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "lockMode", lockType.getDescription() );
        map.put( "waitTimeMillis", TimeUnit.NANOSECONDS.toMillis( waitTimeNanos( currentTimeNanos ) ) );
        map.put( "resourceType", resourceType.toString() );
        map.put( "resourceIds", resourceIds );
        map.put( "transactionId", transactionId );
        return map;
    }

    @Override
    String name()
    {
        return WAITING_STATE;
    }

    @Override
    boolean isWaitingOnLocks()
    {
        return true;
    }

    @Override
    List<ActiveLock> waitingOnLocks()
    {
        List<ActiveLock> locks = new ArrayList<>( resourceIds.length );
        for ( long resourceId : resourceIds )
        {
            locks.add( new ActiveLock( resourceType, lockType, transactionId, resourceId ) );
        }
        return locks;
    }
}
