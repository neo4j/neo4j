/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking.community;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.Transaction;

import org.neo4j.kernel.impl.locking.Locks;

public class CommunityLockClient implements Locks.Client
{
    private final LockManagerImpl manager;
    private Transaction tx = LockTransaction.NO_TRANSACTION;

    private final Map<Locks.ResourceType, Map<Long, LockResource>> sharedLocks = new HashMap<>();
    private final Map<Locks.ResourceType, Map<Long, LockResource>> exclusiveLocks = new HashMap<>();

    public CommunityLockClient( LockManagerImpl manager )
    {
        this.manager = manager;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            manager.getReadLock( resource, tx );
            localLocks.put(resourceId, resource);
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            manager.getWriteLock( resource, tx );
            localLocks.put(resourceId, resource);
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            if(manager.tryWriteLock( resource, tx ))
            {
                localLocks.put(resourceId, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            if(manager.tryReadLock( resource, tx ))
            {
                localLocks.put(resourceId, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseReadLock( new LockResource( resourceType, resourceId ), tx );
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseWriteLock( new LockResource( resourceType, resourceId ), tx );
        }
    }

    @Override
    public void releaseAllShared()
    {
        for ( Map<Long, LockResource> map : sharedLocks.values() )
        {
            for ( LockResource resource : map.values() )
            {
                manager.releaseReadLock( resource, tx );
            }
        }
        sharedLocks.clear();
    }

    @Override
    public void releaseAllExclusive()
    {
        for ( Map<Long, LockResource> map : exclusiveLocks.values() )
        {
            for ( LockResource resource : map.values() )
            {
                manager.releaseWriteLock( resource, tx );
            }
        }
        exclusiveLocks.clear();
    }

    @Override
    public void releaseAll()
    {
        releaseAllExclusive();
        releaseAllShared();
    }

    @Override
    public void setTx( Transaction tx )
    {
        this.tx = tx;
    }

    @Override
    public void close()
    {
        releaseAll();
        setTx( LockTransaction.NO_TRANSACTION );
    }

    private Map<Long, LockResource> localShared( Locks.ResourceType resourceType )
    {
        Map<Long, LockResource> map = sharedLocks.get( resourceType );
        if(map == null)
        {
            map = new HashMap<>();
            sharedLocks.put( resourceType, map );
        }
        return map;
    }

    private Map<Long, LockResource> localExclusive( Locks.ResourceType resourceType )
    {
        Map<Long, LockResource> map = exclusiveLocks.get( resourceType );
        if(map == null)
        {
            map = new HashMap<>();
            exclusiveLocks.put( resourceType, map );
        }
        return map;
    }
}
