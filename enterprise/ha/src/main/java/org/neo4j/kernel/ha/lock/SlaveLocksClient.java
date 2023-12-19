/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.lock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongList;
import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.AvailabilityGuard.UnavailableException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

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
    private final Log log;
    private boolean initialized;
    private volatile boolean stopped;

    SlaveLocksClient( Master master, Locks.Client local, Locks localLockManager,
            RequestContextFactory requestContextFactory, AvailabilityGuard availabilityGuard, LogProvider logProvider )
    {
        this.master = master;
        this.client = local;
        this.localLockManager = localLockManager;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        assertNotStopped();

        long[] newResourceIds = firstTimeSharedLocks( resourceType, resourceIds );
        if ( newResourceIds.length > 0 )
        {
            try
            {
                acquireSharedOnMasterFiltered( tracer, resourceType, newResourceIds );
            }
            catch ( Throwable failure )
            {
                if ( resourceIds != newResourceIds )
                {
                    releaseShared( resourceType, resourceIds, newResourceIds );
                }
                throw failure;
            }
            for ( long resourceId : newResourceIds )
            {
                if ( !client.trySharedLock( resourceType, resourceId ) )
                {
                    throw new LocalDeadlockDetectedException(
                            client, localLockManager, resourceType, resourceId, READ );
                }
            }
        }
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws
            AcquireLockTimeoutException
    {
        assertNotStopped();

        long[] newResourceIds = firstTimeExclusiveLocks( resourceType, resourceIds );
        if ( newResourceIds.length > 0 )
        {
            try ( LockWaitEvent event = tracer.waitForLock( true, resourceType, newResourceIds ) )
            {
                acquireExclusiveOnMaster( resourceType, newResourceIds );
            }
            catch ( Throwable failure )
            {
                if ( resourceIds != newResourceIds )
                {
                    releaseExclusive( resourceType, resourceIds, newResourceIds );
                }
                throw failure;
            }
            for ( long resourceId : newResourceIds )
            {
                if ( !client.tryExclusiveLock( resourceType, resourceId ) )
                {
                    throw new LocalDeadlockDetectedException(
                            client, localLockManager, resourceType, resourceId, WRITE );
                }
            }
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        throw newUnsupportedDirectTryLockUsageException();
    }

    @Override
    public boolean reEnterShared( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();

        client.releaseShared( resourceType, resourceIds );
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();

        client.releaseExclusive( resourceType, resourceIds );
    }

    @Override
    public void prepare()
    {
        client.prepare();
    }

    @Override
    public void stop()
    {
        client.stop();
        stopLockSessionOnMaster();
        stopped = true;
    }

    @Override
    public void close()
    {
        client.close();
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

    @Override
    public Stream<? extends ActiveLock> activeLocks()
    {
        return client.activeLocks();
    }

    @Override
    public long activeLockCount()
    {
        return client.activeLockCount();
    }

    /**
     * In order to prevent various indexes collisions on master during transaction commit that originate on one of the
     * slaves we need to grab same locks on {@link ResourceTypes#LABEL} and {@link ResourceTypes#RELATIONSHIP_TYPE}
     * that
     * where obtained on origin. To be able to do that and also prevent shared locks to be propagates to master in cases
     * of
     * read only transactions we need to postpone obtaining them till we know that we participating in a
     * transaction that performs modifications.
     *
     * @param tracer lock tracer
     */
    void acquireDeferredSharedLocks( LockTracer tracer )
    {
        assertNotStopped();
        Map<ResourceType,PrimitiveLongList> deferredLocksMap = new HashMap<>();
        List<? extends ActiveLock> activeLocks = client.activeLocks()
                        .filter( activeLock -> ActiveLock.SHARED_MODE.equals( activeLock.mode() ) )
                        .filter( this::isLabelOrRelationshipType ).collect( Collectors.toList() );
        for ( ActiveLock activeLock : activeLocks )
        {
            Function<ResourceType,PrimitiveLongList> listCreator = resourceType -> Primitive.longList();
            deferredLocksMap.computeIfAbsent( activeLock.resourceType(), listCreator )
                            .add( activeLock.resourceId() );
        }
        deferredLocksMap.forEach( ( type, ids ) -> lockResourcesOnMaster( tracer, type, ids ) );
    }

    private void lockResourcesOnMaster( LockTracer tracer, ResourceType type, PrimitiveLongList ids )
    {
        long[] resourceIds = ids.toArray();
        try ( LockWaitEvent event = tracer.waitForLock( false, type, resourceIds ) )
        {
            acquireSharedOnMaster( type, resourceIds );
        }
    }

    private boolean isLabelOrRelationshipType( ActiveLock activeLock )
    {
        return (activeLock.resourceType() == ResourceTypes.LABEL) ||
                (activeLock.resourceType() == ResourceTypes.RELATIONSHIP_TYPE);
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

    private long[] firstTimeSharedLocks( ResourceType resourceType, long[] resourceIds )
    {
        int cursor = 0;
        for ( int i = 0; i < resourceIds.length; i++ )
        {
            if ( !client.reEnterShared( resourceType, resourceIds[i] ) )
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

    private long[] firstTimeExclusiveLocks( ResourceType resourceType, long[] resourceIds )
    {
        int cursor = 0;
        for ( int i = 0; i < resourceIds.length; i++ )
        {
            if ( !client.reEnterExclusive( resourceType, resourceIds[i] ) )
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

    private void releaseShared( ResourceType resourceType, long[] resourceIds, long[] excludedIds )
    {
        for ( int i = 0, j = 0; i < resourceIds.length; i++ )
        {
            if ( resourceIds[i] == excludedIds[j] )
            {
                j++;
            }
            else
            {
                client.releaseShared( resourceType, resourceIds[i] );
            }
        }
    }

    private void releaseExclusive( ResourceType resourceType, long[] resourceIds, long[] excludedIds )
    {
        for ( int i = 0, j = 0; i < resourceIds.length; i++ )
        {
            if ( resourceIds[i] == excludedIds[j] )
            {
                j++;
            }
            else
            {
                client.releaseShared( resourceType, resourceIds[i] );
            }
        }
    }

    private void acquireSharedOnMasterFiltered( LockTracer lockTracer, ResourceType resourceType, long... resourceIds )
    {
        if ( (resourceType == ResourceTypes.INDEX_ENTRY) ||
             (resourceType == ResourceTypes.LABEL) ||
             (resourceType == ResourceTypes.RELATIONSHIP_TYPE) )
        {
            return;
        }
        try ( LockWaitEvent event = lockTracer.waitForLock( false, resourceType, resourceIds ) )
        {
            acquireSharedOnMaster( resourceType, resourceIds );
        }
    }

    private void acquireSharedOnMaster( ResourceType resourceType, long[] resourceIds )
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

    private void acquireExclusiveOnMaster( ResourceType resourceType, long... resourceIds )
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
                throw new DeadlockDetectedException( result.getMessage() );
            case NOT_LOCKED:
                throw new UnsupportedOperationException( result.toString() );
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

    private void assertNotStopped()
    {
        if ( stopped )
        {
            throw new LockClientStoppedException( this );
        }
    }

    private UnsupportedOperationException newUnsupportedDirectTryLockUsageException()
    {
        return new UnsupportedOperationException(
                "Distributed tryLocks are not supported. They only work with local lock managers." );
    }
}
