/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.neo4j.kernel.impl.locking.LockType.READ;
import static org.neo4j.kernel.impl.locking.LockType.WRITE;

/**
 * The slave locks client is responsible for managing locks on behalf of some actor on a slave machine. An actor
 * could be a transaction or some other job that runs in the database.
 *
 * The client maintains a local "real" lock client, backed by some regular Locks implementation, but it also coordinates
 * with the master for certain types of locks. If you grab a lock on a node, for instance, this class will grab a
 * cluster-global lock by talking to the master machine, and then grab that same lock locally before returning.
 */
class SlaveLocksClient implements Locks.Client
{
    private final Master master;
    private final Locks.Client client;
    private final Locks localLockManager;
    private final RequestContextFactory requestContextFactory;
    private final AvailabilityGuard availabilityGuard;
    private final SlaveLockManager.Configuration config;

    // Using atomic ints to avoid creating garbage through boxing.
    private final Map<Locks.ResourceType, Map<Long, AtomicInteger>> sharedLocks;
    private final Map<Locks.ResourceType, Map<Long, AtomicInteger>> exclusiveLocks;
    private boolean initialized = false;

    public SlaveLocksClient(
            Master master,
            Locks.Client local,
            Locks localLockManager,
            RequestContextFactory requestContextFactory,
            AvailabilityGuard availabilityGuard,
            SlaveLockManager.Configuration config )
    {
        this.master = master;
        this.client = local;
        this.localLockManager = localLockManager;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.config = config;
        sharedLocks = new HashMap<>();
        exclusiveLocks = new HashMap<>();
    }

    private Map<Long, AtomicInteger> getLockMap(
            Map<Locks.ResourceType, Map<Long, AtomicInteger>> resourceMap,
            Locks.ResourceType resourceType )
    {
        Map<Long, AtomicInteger> lockMap = resourceMap.get( resourceType );
        if ( lockMap == null )
        {
            lockMap = new HashMap<>();
            resourceMap.put( resourceType, lockMap );
        }
        return lockMap;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        Map<Long, AtomicInteger> lockMap = getLockMap( sharedLocks, resourceType );
        long[] untakenIds = incrementAndRemoveAlreadyTakenLocks( lockMap, resourceIds );
        if ( untakenIds.length > 0 && getReadLockOnMaster( resourceType, untakenIds ) )
        {
            if ( client.trySharedLock( resourceType, untakenIds ) )
            {
                for ( int i = 0; i < untakenIds.length; i++ )
                {
                    lockMap.put( untakenIds[i], new AtomicInteger( 1 ) );
                }
            }
            else
            {
                throw new LocalDeadlockDetectedException( client, localLockManager, resourceType, resourceIds, READ );

            }
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws
            AcquireLockTimeoutException
    {
        Map<Long, AtomicInteger> lockMap = getLockMap( exclusiveLocks, resourceType );
        long[] untakenIds = incrementAndRemoveAlreadyTakenLocks( lockMap, resourceIds );
        if ( untakenIds.length > 0 && acquireExclusiveOnMaster( resourceType, untakenIds ) )
        {
            if ( client.tryExclusiveLock( resourceType, untakenIds ) )
            {
                for ( int i = 0; i < untakenIds.length; i++ )
                {
                    lockMap.put( untakenIds[i], new AtomicInteger( 1 ) );
                }
            }
            else
            {
                throw new LocalDeadlockDetectedException( client, localLockManager, resourceType, resourceIds, WRITE );
            }
        }
    }

    private long[] incrementAndRemoveAlreadyTakenLocks(
            Map<Long, AtomicInteger> takenLocks,
            long[] resourceIds )
    {
        ArrayList<Long> untakenIds = new ArrayList<>();
        for ( int i = 0; i < resourceIds.length; i++ )
        {
            long id = resourceIds[i];
            AtomicInteger counter = takenLocks.get( id );
            if ( counter != null )
            {
                counter.incrementAndGet();
            }
            else
            {
                untakenIds.add( id );
            }
        }
        long[] untaken = new long[untakenIds.size()];
        for ( int i = 0; i < untaken.length; i++ )
        {
            long id = untakenIds.get( i );
            untaken[i] = id;
        }
        return untaken;
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, AtomicInteger> lockMap = getLockMap( sharedLocks, resourceType );
        for ( long resourceId : resourceIds )
        {
            AtomicInteger counter = lockMap.get( resourceId );
            if(counter == null)
            {
                throw new IllegalStateException( this + " cannot release lock it does not hold: EXCLUSIVE " +
                        resourceType + "[" + resourceId + "]" );
            }
            if(counter.decrementAndGet() == 0)
            {
                lockMap.remove( resourceId );
                client.releaseShared( resourceType, resourceId );
            }
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<Long, AtomicInteger> lockMap = getLockMap( exclusiveLocks, resourceType );
        for ( long resourceId : resourceIds )
        {
            AtomicInteger counter = lockMap.get( resourceId );
            if(counter == null)
            {
                throw new IllegalStateException( this + " cannot release lock it does not hold: EXCLUSIVE " +
                        resourceType + "[" + resourceId + "]" );
            }
            if(counter.decrementAndGet() == 0)
            {
                lockMap.remove( resourceId );
                client.releaseExclusive( resourceType, resourceId );
            }
        }
    }

    @Override
    public void releaseAllShared()
    {
        sharedLocks.clear();
        client.releaseAllShared();
    }

    @Override
    public void releaseAllExclusive()
    {
        exclusiveLocks.clear();
        client.releaseAllExclusive();
    }

    @Override
    public void releaseAll()
    {
        sharedLocks.clear();
        exclusiveLocks.clear();
        if ( initialized )
        {
            try ( Response<Void> ignored = master.endLockSession( newRequestContextFor( client ), true ) )
            {
                // Lock session is closed on master at this point
            }
            initialized = false;
        }
        client.releaseAll();
    }

    @Override
    public void close()
    {
        sharedLocks.clear();
        exclusiveLocks.clear();
        if ( initialized )
        {
            try ( Response<Void> ignored = master.endLockSession( newRequestContextFor( client ), true ) )
            {
                // Lock session is closed on master at this point
            }
        }
        client.close();
    }

    @Override
    public int getLockSessionId()
    {
        return initialized ? client.getLockSessionId() : -1;
    }

    private boolean getReadLockOnMaster( Locks.ResourceType resourceType, long ... resourceId )
    {
        if ( resourceType == ResourceTypes.NODE
            || resourceType == ResourceTypes.RELATIONSHIP
            || resourceType == ResourceTypes.GRAPH_PROPS
            || resourceType == ResourceTypes.LEGACY_INDEX )
        {
            makeSureTxHasBeenInitialized();

            RequestContext requestContext = newRequestContextFor( this );
            try ( Response<LockResult> response = master.acquireSharedLock( requestContext, resourceType, resourceId ) )
            {
                return receiveLockResponse( response );
            }
        }
        else
        {
            return true;
        }
    }

    private boolean acquireExclusiveOnMaster( Locks.ResourceType resourceType, long... resourceId )
    {
        makeSureTxHasBeenInitialized();
        RequestContext requestContext = newRequestContextFor( this );
        try ( Response<LockResult> response = master.acquireExclusiveLock( requestContext, resourceType, resourceId ) )
        {
            return receiveLockResponse( response );
        }
    }

    private boolean receiveLockResponse( Response<LockResult> response )
    {
        LockResult result = response.response();

        switch ( result.getStatus() )
        {
            case DEAD_LOCKED:
                throw new DeadlockDetectedException( result.getDeadlockMessage() );
            case NOT_LOCKED:
                throw new UnsupportedOperationException();
            case OK_LOCKED:
                break;
            default:
                throw new UnsupportedOperationException( result.toString() );
        }

        return true;
    }

    private void makeSureTxHasBeenInitialized()
    {
        availabilityGuard.checkAvailability( config.getAvailabilityTimeout(), RuntimeException.class );
        if ( !initialized )
        {
            try ( Response<Void> ignored = master.newLockSession( newRequestContextFor( client ) ) )
            {
                // Lock session is initialized on master at this point
            }
            catch ( TransactionFailureException e )
            {
                // Temporary wrapping, we should review the exception structure of the Locks API to allow this to
                // not use runtime exceptions here.
                throw new org.neo4j.graphdb.TransactionFailureException( "Failed to acquire lock in cluster: " + e.getMessage(), e );
            }
            initialized = true;
        }
    }

    private RequestContext newRequestContextFor( Locks.Client client )
    {
        return requestContextFactory.newRequestContext( client.getLockSessionId() );
    }

    private UnsupportedOperationException newUnsupportedDirectTryLockUsageException()
    {
        return new UnsupportedOperationException( "At the time of adding \"try lock\" semantics there was no usage of " +
                getClass().getSimpleName() + " calling it directly. It was designed to be called on a local " +
                LockManager.class.getSimpleName() + " delegated to from within the waiting version" );
    }
}
