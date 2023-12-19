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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.neo4j.collection.pool.Pool;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager.DeadlockResolutionStrategy;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.LockClientStateHolder;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.String.format;

// Please note. Except separate test cases for particular classes related to community locking
// see also LockingCompatibilityTestSuite test suite

/**
 * These clients act as agents against the lock manager. The clients hold and release locks.
 * <p/>
 * The Forseti client tracks which locks it already holds, and will only communicate with the global lock manager if
 * necessary. Grabbing the same lock multiple times will honor re-entrancy et cetera, but the client will track in
 * local fields how many times the lock has been grabbed, such that it will only grab and release the lock once from
 * the
 * global lock manager.
 */
public class ForsetiClient implements Locks.Client
{
    /** Id for this client */
    private final int clientId;

    /** resourceType -> lock map. These are the global lock maps, shared across all clients. */
    private final ConcurrentMap<Long,ForsetiLockManager.Lock>[] lockMaps;

    /** resourceType -> wait strategy */
    private final WaitStrategy<AcquireLockTimeoutException>[] waitStrategies;

    /** How to resolve deadlocks. */
    private final DeadlockResolutionStrategy deadlockResolutionStrategy;

    /** Handle to return client to pool when closed. */
    private final Pool<ForsetiClient> clientPool;

    /** Look up a client by id */
    private final IntFunction<ForsetiClient> clientById;

    /**
     * The client uses this to track which locks it holds. It is solely an optimization to ensure we don't need to
     * coordinate if we grab the same lock multiple times.
     * <p/>
     * The data structure looks like:
     * Array[ resourceType -> Map( resourceId -> num locks ) ]
     */
    private final PrimitiveLongIntMap[] sharedLockCounts;

    /** @see #sharedLockCounts */
    private final PrimitiveLongIntMap[] exclusiveLockCounts;

    /**
     * Time within which any particular lock should be acquired.
     *
     * @see GraphDatabaseSettings#lock_acquisition_timeout
     */
    private final long lockAcquisitionTimeoutMillis;
    private final Clock clock;

    /** List of other clients this client is waiting for. */
    private final SimpleBitSet waitList = new SimpleBitSet( 64 );
    private long waitListCheckPoint;

    // To be able to close Locks.Client instance properly we should be able to do couple of things:
    //  - have a possibility to prevent new clients to come
    //  - wake up all the waiters and let them go
    //  - have a possibility to see how many clients are still using us and wait for them to finish
    // We need to do all of that to prevent a situation when a closing client will get a lock that will never be
    // closed and eventually will block other clients.
    private final LockClientStateHolder stateHolder = new LockClientStateHolder();

    /**
     * For exclusive locks, we only need a single re-usable one per client. We simply CAS this lock into whatever slots
     * we want to hold in the global lock map.
     */
    private final ExclusiveLock myExclusiveLock = new ExclusiveLock( this );

    private volatile boolean hasLocks;

    private final ReleaseExclusiveLocksAndClearSharedVisitor releaseExclusiveAndClearSharedVisitor =
            new ReleaseExclusiveLocksAndClearSharedVisitor();
    private final ReleaseSharedDontCheckExclusiveVisitor releaseSharedDontCheckExclusiveVisitor =
            new ReleaseSharedDontCheckExclusiveVisitor();

    /**
     * When we *wait* for a specific lock to be released to us, we assign it to this field. This helps us during the
     * secondary deadlock verification process, where we traverse the waiter/lock-owner dependency graph.
     */
    private volatile ForsetiLockManager.Lock waitingForLock;

    public ForsetiClient( int id, ConcurrentMap<Long,ForsetiLockManager.Lock>[] lockMaps,
                          WaitStrategy<AcquireLockTimeoutException>[] waitStrategies, Pool<ForsetiClient> clientPool,
                          DeadlockResolutionStrategy deadlockResolutionStrategy, IntFunction<ForsetiClient> clientById,
                          long lockAcquisitionTimeoutMillis, Clock clock )
    {
        this.clientId = id;
        this.lockMaps = lockMaps;
        this.waitStrategies = waitStrategies;
        this.deadlockResolutionStrategy = deadlockResolutionStrategy;
        this.clientPool = clientPool;
        this.clientById = clientById;
        this.sharedLockCounts = new PrimitiveLongIntMap[lockMaps.length];
        this.exclusiveLockCounts = new PrimitiveLongIntMap[lockMaps.length];
        this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
        this.clock = clock;

        for ( int i = 0; i < sharedLockCounts.length; i++ )
        {
            sharedLockCounts[i] = Primitive.longIntMap();
            exclusiveLockCounts[i] = Primitive.longIntMap();
        }
    }

    /**
     * Reset current client state. Make it ready for next bunch of operations.
     * Should be used before factory release client to public usage.
     */
    public void reset()
    {
        stateHolder.reset();
    }

    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        hasLocks = true;
        stateHolder.incrementActiveClients( this );
        LockWaitEvent waitEvent = null;

        try
        {
            // Grab the global lock map we will be using
            ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];

            // And grab our local lock maps
            PrimitiveLongIntMap heldShareLocks = sharedLockCounts[resourceType.typeId()];
            PrimitiveLongIntMap heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

            for ( long resourceId : resourceIds )
            {
                // First, check if we already hold this as a shared lock
                int heldCount = heldShareLocks.get( resourceId );
                if ( heldCount != -1 )
                {
                    // We already have a lock on this, just increment our local reference counter.
                    heldShareLocks.put( resourceId, Math.incrementExact( heldCount ) );
                    continue;
                }

                // Second, check if we hold it as an exclusive lock
                if ( heldExclusiveLocks.containsKey( resourceId ) )
                {
                    // We already have an exclusive lock, so just leave that in place.
                    // When the exclusive lock is released, it will be automatically downgraded to a shared lock,
                    // since we bumped the share lock reference count.
                    heldShareLocks.put( resourceId, 1 );
                    continue;
                }

                // We don't hold the lock, so we need to grab it via the global lock map
                int tries = 0;
                SharedLock mySharedLock = null;
                long waitStartMillis = clock.millis();

                // Retry loop
                while ( true )
                {
                    assertValid( waitStartMillis, resourceType, resourceId );

                    // Check if there is a lock for this entity in the map
                    ForsetiLockManager.Lock existingLock = lockMap.get( resourceId );

                    // No lock
                    if ( existingLock == null )
                    {
                        // Try to create a new shared lock
                        if ( mySharedLock == null )
                        {
                            mySharedLock = new SharedLock( this );
                        }

                        if ( lockMap.putIfAbsent( resourceId, mySharedLock ) == null )
                        {
                            // Success, we now hold the shared lock.
                            break;
                        }
                        else
                        {
                            continue;
                        }
                    }

                    // Someone holds shared lock on this entity, try and get in on that action
                    else if ( existingLock instanceof SharedLock )
                    {
                        if ( ((SharedLock) existingLock).acquire( this ) )
                        {
                            // Success!
                            break;
                        }
                    }

                    // Someone holds an exclusive lock on this entity
                    else if ( existingLock instanceof ExclusiveLock )
                    {
                        // We need to wait, just let the loop run.
                    }
                    else
                    {
                        throw new UnsupportedOperationException( "Unknown lock type: " + existingLock );
                    }

                    if ( waitEvent == null )
                    {
                        waitEvent = tracer.waitForLock( false, resourceType, resourceId );
                    }
                    // And take note of who we are waiting for. This is used for deadlock detection.
                    waitFor( existingLock, resourceType, resourceId, tries++ );
                }

                // Make a local note about the fact that we now hold this lock
                heldShareLocks.put( resourceId, 1 );
            }
        }
        finally
        {
            if ( waitEvent != null )
            {
                waitEvent.close();
            }
            clearWaitList();
            waitingForLock = null;
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        hasLocks = true;
        stateHolder.incrementActiveClients( this );
        LockWaitEvent waitEvent = null;

        try
        {
            ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            PrimitiveLongIntMap heldLocks = exclusiveLockCounts[resourceType.typeId()];

            for ( long resourceId : resourceIds )
            {
                int heldCount = heldLocks.get( resourceId );
                if ( heldCount != -1 )
                {
                    // We already have a lock on this, just increment our local reference counter.
                    heldLocks.put( resourceId, Math.incrementExact( heldCount ) );
                    continue;
                }

                // Grab the global lock
                ForsetiLockManager.Lock existingLock;
                int tries = 0;
                long waitStartMillis = clock.millis();
                while ( (existingLock = lockMap.putIfAbsent( resourceId, myExclusiveLock )) != null )
                {
                    assertValid( waitStartMillis, resourceType, resourceId );

                    // If this is a shared lock:
                    // Given a grace period of tries (to try and not starve readers), grab an update lock and wait
                    // for it to convert to an exclusive lock.
                    if ( tries > 50 && existingLock instanceof SharedLock )
                    {
                        // Then we should upgrade that lock
                        SharedLock sharedLock = (SharedLock) existingLock;
                        if ( tryUpgradeSharedToExclusive( tracer, waitEvent, resourceType, lockMap, resourceId,
                                sharedLock,
                                waitStartMillis ) )
                        {
                            break;
                        }
                    }

                    if ( waitEvent == null )
                    {
                        waitEvent = tracer.waitForLock( true, resourceType, resourceId );
                    }
                    waitFor( existingLock, resourceType, resourceId, tries++ );
                }

                heldLocks.put( resourceId, 1 );
            }
        }
        finally
        {
            if ( waitEvent != null )
            {
                waitEvent.close();
            }
            clearWaitList();
            waitingForLock = null;
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        hasLocks = true;
        stateHolder.incrementActiveClients( this );

        try
        {
            ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            PrimitiveLongIntMap heldLocks = exclusiveLockCounts[resourceType.typeId()];

            int heldCount = heldLocks.get( resourceId );
            if ( heldCount != -1 )
            {
                // We already have a lock on this, just increment our local reference counter.
                heldLocks.put( resourceId, Math.incrementExact( heldCount ) );
                return true;
            }

            // Grab the global lock
            ForsetiLockManager.Lock lock;
            if ( (lock = lockMap.putIfAbsent( resourceId, myExclusiveLock )) != null )
            {
                if ( lock instanceof SharedLock && sharedLockCounts[resourceType.typeId()].containsKey( resourceId ) )
                {
                    SharedLock sharedLock = (SharedLock) lock;
                    if ( sharedLock.tryAcquireUpdateLock( this ) )
                    {
                        if ( sharedLock.numberOfHolders() == 1 )
                        {
                            heldLocks.put( resourceId, 1 );
                            return true;
                        }
                        else
                        {
                            sharedLock.releaseUpdateLock();
                            return false;
                        }
                    }
                }
                return false;
            }

            heldLocks.put( resourceId, 1 );
            return true;
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        hasLocks = true;
        stateHolder.incrementActiveClients( this );

        try
        {
            ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            PrimitiveLongIntMap heldShareLocks = sharedLockCounts[resourceType.typeId()];
            PrimitiveLongIntMap heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

            int heldCount = heldShareLocks.get( resourceId );
            if ( heldCount != -1 )
            {
                // We already have a lock on this, just increment our local reference counter.
                heldShareLocks.put( resourceId, Math.incrementExact( heldCount ) );
                return true;
            }

            if ( heldExclusiveLocks.containsKey( resourceId ) )
            {
                // We already have an exclusive lock, so just leave that in place. When the exclusive lock is released,
                // it will be automatically downgraded to a shared lock, since we bumped the share lock reference count.
                heldShareLocks.put( resourceId, 1 );
                return true;
            }

            long waitStartMillis = clock.millis();
            while ( true )
            {
                assertValid( waitStartMillis, resourceType, resourceId );

                ForsetiLockManager.Lock existingLock = lockMap.get( resourceId );
                if ( existingLock == null )
                {
                    // Try to create a new shared lock
                    if ( lockMap.putIfAbsent( resourceId, new SharedLock( this ) ) == null )
                    {
                        // Success!
                        break;
                    }
                }
                else if ( existingLock instanceof SharedLock )
                {
                    // Note that there is a "safe" race here where someone may be releasing the last reference to a lock
                    // and thus removing that lock instance (making it unacquirable). In this case, we allow retrying,
                    // even though this is a try-lock call.
                    if ( ((SharedLock) existingLock).acquire( this ) )
                    {
                        // Success!
                        break;
                    }
                    else if ( ((SharedLock) existingLock).isUpdateLock() )
                    {
                        return false;
                    }
                }
                else if ( existingLock instanceof ExclusiveLock )
                {
                    return false;
                }
                else
                {
                    throw new UnsupportedOperationException( "Unknown lock type: " + existingLock );
                }
            }
            heldShareLocks.put( resourceId, 1 );
            return true;
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean reEnterShared( ResourceType resourceType, long resourceId )
    {
        stateHolder.incrementActiveClients( this );
        try
        {
            PrimitiveLongIntMap heldShareLocks = sharedLockCounts[resourceType.typeId()];
            PrimitiveLongIntMap heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

            int heldCount = heldShareLocks.get( resourceId );
            if ( heldCount != -1 )
            {
                // We already have a lock on this, just increment our local reference counter.
                heldShareLocks.put( resourceId, Math.incrementExact( heldCount ) );
                return true;
            }

            if ( heldExclusiveLocks.containsKey( resourceId ) )
            {
                // We already have an exclusive lock, so just leave that in place. When the exclusive lock is released,
                // it will be automatically downgraded to a shared lock, since we bumped the share lock reference count.
                heldShareLocks.put( resourceId, 1 );
                return true;
            }

            // We didn't hold a lock already, so we cannot re-enter.
            return false;
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
    {
        stateHolder.incrementActiveClients( this );
        try
        {
            PrimitiveLongIntMap heldLocks = exclusiveLockCounts[resourceType.typeId()];

            int heldCount = heldLocks.get( resourceId );
            if ( heldCount != -1 )
            {
                // We already have a lock on this, just increment our local reference counter.
                heldLocks.put( resourceId, Math.incrementExact( heldCount ) );
                return true;
            }

            // We didn't hold a lock already, so we cannot re-enter.
            return false;
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        stateHolder.incrementActiveClients( this );

        try
        {
            PrimitiveLongIntMap sharedLocks = sharedLockCounts[resourceType.typeId()];
            PrimitiveLongIntMap exclusiveLocks = exclusiveLockCounts[resourceType.typeId()];
            ConcurrentMap<Long,ForsetiLockManager.Lock> resourceTypeLocks = lockMaps[resourceType.typeId()];
            for ( long resourceId : resourceIds )
            {
                if ( releaseLocalLock( resourceType, resourceId, sharedLocks ) )
                {
                    continue;
                }
                // Only release if we were not holding an exclusive lock as well
                if ( !exclusiveLocks.containsKey( resourceId ) )
                {
                    releaseGlobalLock( resourceTypeLocks, resourceId );
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        stateHolder.incrementActiveClients( this );

        try
        {
            PrimitiveLongIntMap exclusiveLocks = exclusiveLockCounts[resourceType.typeId()];
            ConcurrentMap<Long,ForsetiLockManager.Lock> resourceTypeLocks = lockMaps[resourceType.typeId()];
            PrimitiveLongIntMap sharedLocks = sharedLockCounts[resourceType.typeId()];
            for ( long resourceId : resourceIds )
            {
                if ( releaseLocalLock( resourceType, resourceId, exclusiveLocks ) )
                {
                    continue;
                }

                if ( sharedLocks.containsKey( resourceId ) )
                {
                    // We are still holding a shared lock, so we will release it to be reused
                    ForsetiLockManager.Lock lock = resourceTypeLocks.get( resourceId );
                    if ( lock instanceof SharedLock )
                    {
                        SharedLock sharedLock = (SharedLock) lock;
                        if ( sharedLock.isUpdateLock() )
                        {
                            sharedLock.releaseUpdateLock();
                        }
                        else
                        {
                            throw new IllegalStateException( "Incorrect state of exclusive lock. Lock should be updated " +
                                    "to exclusive before attempt to release it. Lock: " + this );
                        }
                    }
                    else
                    {
                        // in case if current lock is exclusive we swap it to new shared lock
                        SharedLock sharedLock = new SharedLock( this );
                        resourceTypeLocks.put( resourceId, sharedLock );
                    }
                }
                else
                {
                    // we do not hold shared lock so we just releasing it
                    releaseGlobalLock( resourceTypeLocks, resourceId );
                }
            }
        }
        finally
        {
            stateHolder.decrementActiveClients();
        }
    }

    private void releaseAllClientLocks()
    {
        // Force the release of all locks held.
        for ( int i = 0; i < exclusiveLockCounts.length; i++ )
        {
            PrimitiveLongIntMap exclusiveLocks = exclusiveLockCounts[i];
            PrimitiveLongIntMap sharedLocks = sharedLockCounts[i];

            // Begin releasing exclusive locks, as we may hold both exclusive and shared locks on the same resource,
            // and so releasing exclusive locks means we can "throw away" our shared lock (which would normally have
            // been re-instated after releasing the exclusive lock).
            if ( exclusiveLocks != null )
            {
                int size = exclusiveLocks.size();
                exclusiveLocks.visitKeys(
                        releaseExclusiveAndClearSharedVisitor.initialize( sharedLocks, lockMaps[i] ) );
                if ( size <= 32 )
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    if ( size > 0 )
                    {
                        exclusiveLocks.clear();
                    }
                }
                else
                {
                    exclusiveLockCounts[i] = Primitive.longIntMap();
                }
            }

            // Then release all remaining shared locks
            if ( sharedLocks != null )
            {
                int size = sharedLocks.size();
                sharedLocks.visitKeys( releaseSharedDontCheckExclusiveVisitor.initialize( lockMaps[i] ) );
                if ( size <= 32 )
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    if ( size > 0 )
                    {
                        sharedLocks.clear();
                    }
                }
                else
                {
                    sharedLockCounts[i] = Primitive.longIntMap();
                }
            }
        }
    }

    @Override
    public void prepare()
    {
        stateHolder.prepare( this );
    }

    @Override
    public void stop()
    {
        // marking client as closed
        if ( stateHolder.stopClient() )
        {
            // waiting for all operations to be completed
            waitForAllClientsToLeave();
            releaseAllLocks();
        }
    }

    private void waitForAllClientsToLeave()
    {
        while ( stateHolder.hasActiveClients() )
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    @Override
    public void close()
    {
        stateHolder.closeClient();
        waitForAllClientsToLeave();
        releaseAllLocks();
        clientPool.release( this );
    }

    private void releaseAllLocks()
    {
        if ( hasLocks )
        {
            releaseAllClientLocks();
            clearWaitList();
            hasLocks = false;
        }
    }

    @Override
    public int getLockSessionId()
    {
        return clientId;
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        List<ActiveLock> locks = new ArrayList<>();
        collectActiveLocks( exclusiveLockCounts, locks, ActiveLock.Factory.EXCLUSIVE_LOCK );
        collectActiveLocks( sharedLockCounts, locks, ActiveLock.Factory.SHARED_LOCK );
        return locks.stream();
    }

    @Override
    public long activeLockCount()
    {
        return countLocks( exclusiveLockCounts ) + countLocks( sharedLockCounts );
    }

    private static void collectActiveLocks(
            PrimitiveLongIntMap[] counts,
            List<ActiveLock> locks,
            ActiveLock.Factory activeLock )
    {
        for ( int typeId = 0; typeId < counts.length; typeId++ )
        {
            PrimitiveLongIntMap lockCounts = counts[typeId];
            if ( lockCounts != null )
            {
                ResourceType resourceType = ResourceTypes.fromId( typeId );
                lockCounts.visitEntries( ( resourceId, count ) ->
                {
                    locks.add( activeLock.create( resourceType, resourceId ) );
                    return false;
                } );
            }
        }
    }

    private long countLocks( PrimitiveLongIntMap[] lockCounts )
    {
        long count = 0;
        for ( PrimitiveLongIntMap lockCount : lockCounts )
        {
            if ( lockCount != null )
            {
                count += lockCount.size();
            }
        }
        return count;
    }

    int waitListSize()
    {
        return waitList.size();
    }

    void copyWaitListTo( SimpleBitSet other )
    {
        other.put( waitList );
    }

    boolean isWaitingFor( int clientId )
    {
        return clientId != this.clientId && waitList.contains( clientId );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ForsetiClient that = (ForsetiClient) o;

        return clientId == that.clientId;
    }

    @Override
    public int hashCode()
    {
        return clientId;
    }

    @Override
    public String toString()
    {
        return String.format( "ForsetiClient[%d]", clientId );
    }

    /** Release a lock from the global pool. */
    private void releaseGlobalLock( ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap, long resourceId )
    {
        ForsetiLockManager.Lock lock = lockMap.get( resourceId );
        if ( lock instanceof ExclusiveLock )
        {
            lockMap.remove( resourceId );
        }
        else if ( lock instanceof SharedLock && ((SharedLock) lock).release( this ) )
        {
            // We were the last to hold this lock, it is now dead and we should remove it.
            // Also cleaning updater referense that can hold lock in memory
            ((SharedLock) lock).cleanUpdateHolder();
            lockMap.remove( resourceId );
        }
    }

    /** Release a lock locally, and return true if we still hold more references to that lock. */
    private boolean releaseLocalLock( ResourceType type, long resourceId, PrimitiveLongIntMap localLocks )
    {
        int lockCount = localLocks.remove( resourceId );
        if ( lockCount == -1 )
        {
            throw new IllegalStateException( this + " cannot release lock that it does not hold: " +
                                             type + "[" + resourceId + "]." );
        }

        if ( lockCount > 1 )
        {
            localLocks.put( resourceId, lockCount - 1 );
            return true;
        }
        return false;
    }

    /**
     * Attempt to upgrade a share lock to an exclusive lock, grabbing the share lock if we don't hold it.
     **/
    private boolean tryUpgradeSharedToExclusive(
            LockTracer tracer,
            LockWaitEvent waitEvent,
            ResourceType resourceType,
            ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap,
            long resourceId,
            SharedLock sharedLock,
            long waitStartMillis )
            throws AcquireLockTimeoutException
    {
        int tries = 0;
        boolean holdsSharedLock = sharedLockCounts[resourceType.typeId()].containsKey( resourceId );
        if ( !holdsSharedLock )
        {
            // We don't hold the shared lock, we need to grab it to upgrade it to an exclusive one
            if ( !sharedLock.acquire( this ) )
            {
                return false;
            }

            try
            {
                if ( tryUpgradeToExclusiveWithShareLockHeld( tracer, waitEvent, resourceType, resourceId, sharedLock,
                        tries, waitStartMillis ) )
                {
                    return true;
                }
                else
                {
                    releaseGlobalLock( lockMap, resourceId );
                    return false;
                }
            }
            catch ( Throwable e )
            {
                releaseGlobalLock( lockMap, resourceId );
                throw e;
            }
        }
        else
        {
            // We do hold the shared lock, so no reason to deal with the complexity in the case above.
            return tryUpgradeToExclusiveWithShareLockHeld( tracer, waitEvent, resourceType, resourceId, sharedLock,
                    tries, waitStartMillis );
        }
    }

    /** Attempt to upgrade a share lock that we hold to an exclusive lock. */
    private boolean tryUpgradeToExclusiveWithShareLockHeld(
            LockTracer tracer, LockWaitEvent priorEvent, ResourceType resourceType, long resourceId,
            SharedLock sharedLock, int tries, long waitStartMillis ) throws AcquireLockTimeoutException
    {
        if ( sharedLock.tryAcquireUpdateLock( this ) )
        {
            LockWaitEvent waitEvent = null;
            try
            {
                // Now we just wait for all clients to release the the share lock
                while ( sharedLock.numberOfHolders() > 1 )
                {
                    assertValid( waitStartMillis, resourceType, resourceId );
                    if ( waitEvent == null && priorEvent == null )
                    {
                        waitEvent = tracer.waitForLock( true, resourceType, resourceId );
                    }
                    waitFor( sharedLock, resourceType, resourceId, tries++ );
                }

                return true;
            }
            catch ( Throwable e )
            {
                sharedLock.releaseUpdateLock();
                if ( e instanceof DeadlockDetectedException || e instanceof LockClientStoppedException )
                {
                    throw (RuntimeException) e;
                }
                throw new TransactionFailureException( "Failed to upgrade shared lock to exclusive: " + sharedLock, e );
            }
            finally
            {
                if ( waitEvent != null )
                {
                    waitEvent.close();
                }
                clearWaitList();
                waitingForLock = null;
            }
        }
        return false;
    }

    private void clearWaitList()
    {
        waitListCheckPoint = waitList.checkPointAndPut( waitListCheckPoint, clientId );
    }

    private void waitFor( ForsetiLockManager.Lock lock, ResourceType type, long resourceId, int tries )
    {
        waitingForLock = lock;
        clearWaitList();
        lock.copyHolderWaitListsInto( waitList );
        applyWaitStrategy( type, tries );

        int b = lock.detectDeadlock( id() );
        if ( b != -1 && deadlockResolutionStrategy.shouldAbort( this, clientById.apply( b ) ) )
        {
            // Force the operations below to happen after the reads we do for deadlock
            // detection in the lines above, as a way to cut down on false-positive deadlocks
            UnsafeUtil.loadFence();

            // Create message before we clear the wait-list, to lower the chance of the message being insane
            String message = this + " can't acquire " + lock + " on " + type + "(" + resourceId +
                             "), because holders of that lock " +
                             "are waiting for " + this + ".\n Wait list:" + lock.describeWaitList();

            // Minimize the risk of false positives by double-checking that the deadlock remains
            // after we've generated a description of it.
            if ( lock.detectDeadlock( id() ) != -1 )
            {
                // If the deadlock is real, then an owner of this lock must be (transitively) waiting on a lock that
                // we own. So to verify the deadlock, we traverse the lock owners and their `waitingForLock` fields,
                // to find a lock that has us among the owners.
                // We only act upon the result of this method if the `tries` count is above some threshold. The reason
                // is that the Lock.collectOwners, which is algorithm relies upon, is inherently racy, and so only
                // reduces the probably of a false positive, but does not eliminate them.
                if ( isDeadlockReal( lock, tries ) )
                {
                    // After checking several times, this really does look like a real deadlock.
                    throw new DeadlockDetectedException( message );
                }
            }
        }
    }

    private boolean isDeadlockReal( ForsetiLockManager.Lock lock, int tries )
    {
        Set<ForsetiLockManager.Lock> waitedUpon = new HashSet<>();
        Set<ForsetiClient> owners = new HashSet<>();
        Set<ForsetiLockManager.Lock> nextWaitedUpon = new HashSet<>();
        Set<ForsetiClient> nextOwners = new HashSet<>();
        lock.collectOwners( owners );

        do
        {
            waitedUpon.addAll( nextWaitedUpon );
            collectNextOwners( waitedUpon, owners, nextWaitedUpon, nextOwners );
            if ( nextOwners.contains( this ) && tries > 20 )
            {
                // Worrying... let's take a deep breath
                nextOwners.clear();
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
                // ... and check again
                collectNextOwners( waitedUpon, owners, nextWaitedUpon, nextOwners );
                if ( nextOwners.contains( this ) )
                {
                    // Yes, this deadlock looks real.
                    return true;
                }
            }
            owners.clear();
            Set<ForsetiClient> ownersTmp = owners;
            owners = nextOwners;
            nextOwners = ownersTmp;
        }
        while ( !nextWaitedUpon.isEmpty() );
        // Nope, we didn't find any real wait cycles.
        return false;
    }

    private void collectNextOwners( Set<ForsetiLockManager.Lock> waitedUpon, Set<ForsetiClient> owners,
                                    Set<ForsetiLockManager.Lock> nextWaitedUpon, Set<ForsetiClient> nextOwners )
    {
        nextWaitedUpon.clear();
        for ( ForsetiClient owner : owners )
        {
            ForsetiLockManager.Lock waitingForLock = owner.waitingForLock;
            if ( waitingForLock != null && !waitedUpon.contains( waitingForLock ) )
            {
                nextWaitedUpon.add( waitingForLock );
            }
        }
        for ( ForsetiLockManager.Lock lck : nextWaitedUpon )
        {
            lck.collectOwners( nextOwners );
        }
    }

    /**
     * @return an approximate (assuming data is concurrently being edited) count of the number of locks held by this
     * client.
     */
    int lockCount()
    {
        int count = 0;
        for ( PrimitiveLongIntMap sharedLockCount : sharedLockCounts )
        {
            count += sharedLockCount.size();
        }
        for ( PrimitiveLongIntMap exclusiveLockCount : exclusiveLockCounts )
        {
            count += exclusiveLockCount.size();
        }
        return count;
    }

    String describeWaitList()
    {
        StringBuilder sb = new StringBuilder( format( "%nClient[%d] waits for [", id() ) );
        PrimitiveIntIterator iter = waitList.iterator();
        for ( boolean first = true; iter.hasNext(); )
        {
            int next = iter.next();
            if ( next == clientId )
            {
                // Skip our own id from the wait list, that's an implementation detail
                continue;
            }
            sb.append( (!first) ? "," : "" ).append( next );
            first = false;
        }
        sb.append( "]" );
        return sb.toString();
    }

    public int id()
    {
        return clientId;
    }

    private void applyWaitStrategy( ResourceType resourceType, int tries )
    {
        WaitStrategy<AcquireLockTimeoutException> waitStrategy = waitStrategies[resourceType.typeId()];
        waitStrategy.apply( tries );
    }

    private void assertValid( long waitStartMillis, ResourceType resourceType, long resourceId )
    {
        assertNotStopped();
        assertNotExpired( waitStartMillis, resourceType, resourceId );
    }

    private void assertNotStopped()
    {
        if ( stateHolder.isStopped() )
        {
            throw new LockClientStoppedException( this );
        }
    }

    private void assertNotExpired( long waitStartMillis, ResourceType resourceType, long resourceId )
    {
        if ( lockAcquisitionTimeoutMillis > 0 )
        {
            if ( (lockAcquisitionTimeoutMillis + waitStartMillis) < clock.millis() )
            {
                throw new LockAcquisitionTimeoutException( resourceType, resourceId, lockAcquisitionTimeoutMillis );
            }
        }
    }

    // Visitors used for bulk ops on the lock maps (such as releasing all locks)

    /**
     * Release all shared locks, assuming that there will be no exclusive locks held by this client, such that there
     * is no need to check for those. It is used when releasing all locks.
     */
    private class ReleaseSharedDontCheckExclusiveVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap )
        {
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public boolean visited( long resourceId )
        {
            releaseGlobalLock( lockMap, resourceId );
            return false;
        }
    }

    /**
     * Release exclusive locks and remove any local reference to the shared lock.
     * This is an optimization used when releasing all locks.
     */
    private class ReleaseExclusiveLocksAndClearSharedVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private PrimitiveLongIntMap sharedLockCounts;
        private ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( PrimitiveLongIntMap sharedLockCounts,
                                                                   ConcurrentMap<Long,ForsetiLockManager.Lock> lockMap )
        {
            this.sharedLockCounts = sharedLockCounts;
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public boolean visited( long resourceId )
        {
            releaseGlobalLock( lockMap, resourceId );

            // If we hold this as a shared lock, we can throw that shared lock away directly, since we haven't
            // followed the down-grade protocol.
            if ( sharedLockCounts != null )
            {
                sharedLockCounts.remove( resourceId );
            }
            return false;
        }
    }
}
