/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.AvailabilityGuard.UnavailableException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.locking.LockType.READ;
import static org.neo4j.kernel.impl.locking.LockType.WRITE;

/**
 * The slave locks client is responsible for managing locks on behalf of some actor on a slave machine. An actor
 * could be a transaction or some other job that runs in the database.
 * <p/>
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

    // Using atomic ints to avoid creating garbage through boxing.
    private final Map<Locks.ResourceType, Map<Long, AtomicInteger>> sharedLocks;
    private final Map<Locks.ResourceType, Map<Long, AtomicInteger>> exclusiveLocks;
    private final Log log;
    private final boolean txTerminationAwareLocks;
    private boolean initialized;
    private volatile boolean stopped;

    public SlaveLocksClient(
            Master master,
            Locks.Client local,
            Locks localLockManager,
            RequestContextFactory requestContextFactory,
            AvailabilityGuard availabilityGuard,
            LogProvider logProvider,
            boolean txTerminationAwareLocks )
    {
        this.master = master;
        this.client = local;
        this.localLockManager = localLockManager;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog( getClass() );
        this.txTerminationAwareLocks = txTerminationAwareLocks;
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
        assertNotStopped();

        Map<Long, AtomicInteger> lockMap = getLockMap( sharedLocks, resourceType );
        long[] newResourceIds = onlyFirstTimeLocks( lockMap, resourceIds );
        if ( newResourceIds.length > 0 )
        {
            acquireSharedOnMaster( resourceType, newResourceIds );
            for ( long resourceId : newResourceIds )
            {
                if ( client.trySharedLock( resourceType, resourceId ) )
                {
                    lockMap.put( resourceId, new AtomicInteger( 1 ) );
                }
                else
                {
                    throw new LocalDeadlockDetectedException(
                            client, localLockManager, resourceType, resourceId, READ );
                }
            }
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws
            AcquireLockTimeoutException
    {
        assertNotStopped();

        Map<Long, AtomicInteger> lockMap = getLockMap( exclusiveLocks, resourceType );
        long[] newResourceIds = onlyFirstTimeLocks( lockMap, resourceIds );
        if ( newResourceIds.length > 0 )
        {
            acquireExclusiveOnMaster( resourceType, newResourceIds );
            for ( long resourceId : newResourceIds )
            {
                if ( client.tryExclusiveLock( resourceType, resourceId ) )
                {
                    lockMap.put( resourceId, new AtomicInteger( 1 ) );
                }
                else
                {
                    throw new LocalDeadlockDetectedException(
                            client, localLockManager, resourceType, resourceId, WRITE );
                }
            }
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long resourceId )
    {
        assertNotStopped();

        Map<Long, AtomicInteger> lockMap = getLockMap( sharedLocks, resourceType );
        AtomicInteger counter = lockMap.get( resourceId );
        if ( counter == null )
        {
            throw new IllegalStateException( this + " cannot release lock it does not hold: SHARED " +
                    resourceType + "[" + resourceId + "]" );
        }
        if ( counter.decrementAndGet() == 0 )
        {
            lockMap.remove( resourceId );
            client.releaseShared( resourceType, resourceId );
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long resourceId )
    {
        assertNotStopped();

        Map<Long, AtomicInteger> lockMap = getLockMap( exclusiveLocks, resourceType );
        AtomicInteger counter = lockMap.get( resourceId );
        if ( counter == null )
        {
            throw new IllegalStateException( this + " cannot release lock it does not hold: EXCLUSIVE " +
                    resourceType + "[" + resourceId + "]" );
        }
        if ( counter.decrementAndGet() == 0 )
        {
            lockMap.remove( resourceId );
            client.releaseExclusive( resourceType, resourceId );
        }
    }

    @Override
    public void stop()
    {
        if ( txTerminationAwareLocks )
        {
            client.stop();
            stopLockSessionOnMaster();
            stopped = true;
        }
    }

    @Override
    public void close()
    {
        client.close();
        sharedLocks.clear();
        exclusiveLocks.clear();
        if ( initialized )
        {
            if ( !stopped )
            {
                closeLockSessionOnMaster();
                stopped = true;
            }
            initialized = false;
        }
    }

    @Override
    public int getLockSessionId()
    {
        assertNotStopped();
        return initialized ? client.getLockSessionId() : -1;
    }

    private void stopLockSessionOnMaster()
    {
        try
        {
            endLockSessionOnMaster( false );
        }
        catch ( Throwable t )
        {
            log.warn( "Unable to stop lock session on master", t );
        }
    }

    private void closeLockSessionOnMaster()
    {
        endLockSessionOnMaster( true );
    }

    private void endLockSessionOnMaster( boolean success )
    {
        try ( Response<Void> ignored = master.endLockSession( newRequestContextFor( client ), success ) )
        {
            // Lock session is closed on master at this point
        }
        catch ( ComException e )
        {
            throw new DistributedLockFailureException(
                    "Failed to end the lock session on the master (which implies releasing all held locks)",
                    master, e );
        }
    }

    private long[] onlyFirstTimeLocks( Map<Long,AtomicInteger> lockMap, long[] resourceIds )
    {
        int cursor = 0;
        for ( int i = 0; i < resourceIds.length; i++ )
        {
            AtomicInteger preExistingLock = lockMap.get( resourceIds[i] );
            if ( preExistingLock != null )
            {
                // We already hold this lock, just increment the local reference count
                preExistingLock.incrementAndGet();
            }
            else
            {
                resourceIds[cursor++] = resourceIds[i];
            }
        }
        if ( cursor == 0 )
        {
            return PrimitiveLongCollections.EMPTY_LONG_ARRAY;
        }
        return cursor == resourceIds.length ? resourceIds : Arrays.copyOf( resourceIds, cursor );
    }

    private void acquireSharedOnMaster( Locks.ResourceType resourceType, long... resourceIds )
    {
        if ( resourceType == ResourceTypes.NODE
                || resourceType == ResourceTypes.RELATIONSHIP
                || resourceType == ResourceTypes.GRAPH_PROPS
                || resourceType == ResourceTypes.LEGACY_INDEX )
        {
            makeSureTxHasBeenInitialized();

            RequestContext requestContext = newRequestContextFor( this );
            try ( Response<LockResult> response = master.acquireSharedLock( requestContext, resourceType, resourceIds ) )
            {
                receiveLockResponse( response );
            }
            catch ( ComException e )
            {
                throw new DistributedLockFailureException( "Cannot get shared lock(s) on master", master, e );
            }
        }
    }

    private void acquireExclusiveOnMaster( Locks.ResourceType resourceType, long... resourceIds )
    {
        makeSureTxHasBeenInitialized();
        RequestContext requestContext = newRequestContextFor( this );
        try ( Response<LockResult> response = master.acquireExclusiveLock( requestContext, resourceType, resourceIds ) )
        {
            receiveLockResponse( response );
        }
        catch ( ComException e )
        {
            throw new DistributedLockFailureException( "Cannot get exclusive lock(s) on master", master, e );
        }
    }

    private void receiveLockResponse( Response<LockResult> response )
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
    }

    private void makeSureTxHasBeenInitialized()
    {
        try
        {
            availabilityGuard.checkAvailable();
        }
        catch ( UnavailableException e )
        {
            throw new TransientDatabaseFailureException( "Database not available", e );
        }

        if ( !initialized )
        {
            try ( Response<Void> ignored = master.newLockSession( newRequestContextFor( client ) ) )
            {
                // Lock session is initialized on master at this point
            }
            catch ( Exception exception )
            {
                // Temporary wrapping, we should review the exception structure of the Locks API to allow this to
                // not use runtime exceptions here.
                ComException e;
                if ( exception instanceof ComException )
                {
                    e = (ComException) exception;
                }
                else
                {
                    e = new ComException( exception );
                }
                throw new DistributedLockFailureException( "Failed to start a new lock session on master", master, e );
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
        return new UnsupportedOperationException(
                "Distributed tryLocks are not supported. They only work with local lock managers." );
    }

    private void assertNotStopped()
    {
        if ( stopped )
        {
            throw new LockClientStoppedException( this );
        }
    }
}
