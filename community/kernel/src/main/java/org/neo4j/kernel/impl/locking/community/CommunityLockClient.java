/*
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.kernel.impl.locking.LockClientAlreadyClosedException;
import org.neo4j.kernel.impl.locking.LockClientStateHolder;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.lang.String.format;


// Please note. Except separate test cases for particular classes related to community locking
// see also org.neo4j.kernel.impl.locking.community.CommunityLocksCompatibility test suite

public class CommunityLockClient implements Locks.Client
{
    private final LockManagerImpl manager;
    private final LockTransaction lockTransaction = new LockTransaction();

    private final PrimitiveIntObjectMap<PrimitiveLongObjectMap<LockResource>> sharedLocks = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<PrimitiveLongObjectMap<LockResource>> exclusiveLocks = Primitive.intObjectMap();
    private final PrimitiveLongObjectVisitor<LockResource,RuntimeException> readReleaser;
    private final PrimitiveLongObjectVisitor<LockResource,RuntimeException> writeReleaser;
    private final PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>,RuntimeException> typeReadReleaser;
    private final PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>,RuntimeException> typeWriteReleaser;

    // To be able to close Locks.Client instance properly we should be able to do couple of things:
    //  - have a possibility to prevent new clients to come
    //  - wake up all the waiters and let them go
    //  - have a possibility to see how many clients are still using us and wait for them to finish
    // We need to do all of that to prevent a situation when a closing client will get a lock that will never be
    // closed and eventually will block other clients.
    private final LockClientStateHolder stateHolder = new LockClientStateHolder();

    public CommunityLockClient( LockManagerImpl manager )
    {
        this.manager = manager;

        readReleaser = ( long key, LockResource lockResource ) ->
        {
            manager.releaseReadLock( lockResource, lockTransaction );
            return false;
        };

        writeReleaser = ( long key, LockResource lockResource ) ->
        {
            manager.releaseWriteLock( lockResource, lockTransaction );
            return false;
        };

        typeReadReleaser = ( int key, PrimitiveLongObjectMap<LockResource> value ) ->
        {
            value.visitEntries( readReleaser );
            return false;
        };

        typeWriteReleaser = ( int key, PrimitiveLongObjectMap<LockResource> value ) ->
        {
            value.visitEntries( writeReleaser );
            return false;
        };
    }

    @Override
    public void acquireShared( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource != null )
            {
                resource.acquireReference();
            }
            else
            {
                resource = new LockResource( resourceType, resourceId );
                if ( manager.getReadLock( resource, lockTransaction ) )
                {
                    localLocks.put( resourceId, resource );
                }
                else
                {
                    throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void acquireExclusive( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource != null )
            {
                resource.acquireReference();
            }
            else
            {
                resource = new LockResource( resourceType, resourceId );
                if ( manager.getWriteLock( resource, lockTransaction ) )
                {
                    localLocks.put( resourceId, resource );
                }
                else
                {
                    throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            return false;
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource != null )
            {
                resource.acquireReference();
                return true;
            }
            else
            {
                resource = new LockResource( resourceType, resourceId );
                if ( manager.tryWriteLock( resource, lockTransaction ) )
                {
                    localLocks.put( resourceId, resource );
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            return false;
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource != null )
            {
                resource.acquireReference();
                return true;
            }
            else
            {
                resource = new LockResource( resourceType, resourceId );
                if ( manager.tryReadLock( resource, lockTransaction ) )
                {
                    localLocks.put( resourceId, resource );
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseShared( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource.releaseReference() != 0 )
            {
                return;
            }

            localLocks.remove( resourceId );

            manager.releaseReadLock( new LockResource( resourceType, resourceId ), lockTransaction );
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long resourceId )
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
        }
        try
        {
            PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
            LockResource resource = localLocks.get( resourceId );
            if ( resource.releaseReference() != 0 )
            {
                return;
            }
            localLocks.remove( resourceId );

            manager.releaseWriteLock( new LockResource( resourceType, resourceId ), lockTransaction );
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseAll()
    {
        if ( !stateHolder.incrementActiveClients() )
        {
            throw new LockClientAlreadyClosedException( String.format( "%s is already closed", this ) );
        }
        try
        {
            releaseLocks();
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void stop()
    {
        // closing client to prevent any new client to come
        stateHolder.stopClient();
        // wake up and terminate waiters
        terminateAllWaiters();
        // wait for all active clients to go and terminate latecomers
        while ( stateHolder.hasActiveClients() )
        {
            terminateAllWaiters();
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 20 ) );
        }
    }

    @Override
    public void close()
    {
        stop();
        // now we are only one who operate on this client
        // safe to release all the locks
        releaseLocks();
    }

    private void releaseLocks()
    {
        exclusiveLocks.visitEntries( typeWriteReleaser );
        sharedLocks.visitEntries( typeReadReleaser );
        exclusiveLocks.clear();
        sharedLocks.clear();
    }

    // waking up and terminate all waiters that were waiting for any lock for current client
    private void terminateAllWaiters()
    {
        manager.accept( lock -> {
            lock.terminateLockRequestsForLockTransaction( lockTransaction );
            return false;
        } );
    }

    @Override
    public int getLockSessionId()
    {
        return lockTransaction.getId();
    }

    private PrimitiveLongObjectMap<LockResource> localShared( ResourceType resourceType )
    {
        PrimitiveLongObjectMap<LockResource> map = sharedLocks.get( resourceType.typeId() );
        if ( map == null )
        {
            map = Primitive.longObjectMap();
            sharedLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    private PrimitiveLongObjectMap<LockResource> localExclusive( ResourceType resourceType )
    {
        PrimitiveLongObjectMap<LockResource> map = exclusiveLocks.get( resourceType.typeId() );
        if ( map == null )
        {
            map = Primitive.longObjectMap();
            exclusiveLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    @Override
    public String toString()
    {
        return format( "%s[%d]", getClass().getSimpleName(), getLockSessionId() );
    }
}
