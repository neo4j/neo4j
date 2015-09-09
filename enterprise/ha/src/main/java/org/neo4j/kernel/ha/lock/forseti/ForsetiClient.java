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
package org.neo4j.kernel.ha.lock.forseti;

import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.kernel.impl.util.concurrent.WaitStrategy;

import static java.lang.String.format;

/**
 * These clients act as agents against the lock manager. The clients hold and release locks.
 *
 * The Forseti client tracks which locks it already holds, and will only communicate with the global lock manager if
 * necessary. Grabbing the same lock multiple times will honor re-entrancy et cetera, but the client will track in
 * local fields how many times the lock has been grabbed, such that it will only grab and release the lock once from the
 * global lock manager.
 */
public class ForsetiClient implements Locks.Client
{
    /** Id for this client */
    private final int myId;

    /** resourceType -> lock map. These are the global lock maps, shared across all clients. */
    private final ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps;

    /** resourceType -> wait strategy */
    private final WaitStrategy<AcquireLockTimeoutException>[] waitStrategies;

    /** Handle to return client to pool when closed. */
    private final LinkedQueuePool<ForsetiClient> clientPool;

    /**
     * The client uses this to track which locks it holds. It is solely an optimization to ensure we don't need to
     * coordinate if we grab the same lock multiple times.
     *
     * The data structure looks like:
     * Array[ resourceType -> Map( resourceId -> num locks ) ]
     */
    private final PrimitiveLongIntMap[] sharedLockCounts;

    /** @see {@link #sharedLockCounts} */
    private final PrimitiveLongIntMap[] exclusiveLockCounts;

    /** List of other clients this client is waiting for. */
    private final SimpleBitSet waitList = new SimpleBitSet( 64 );

    /**
     * For exclusive locks, we only need a single re-usable one per client. We simply CAS this lock into whatever slots
     * we want to hold in the global lock map. */
    private final ExclusiveLock myExclusiveLock = new ExclusiveLock(this);

    public ForsetiClient( int id,
                          ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps,
                          WaitStrategy<AcquireLockTimeoutException>[] waitStrategies,
                          LinkedQueuePool<ForsetiClient> clientPool )
    {
        this.myId                = id;
        this.lockMaps            = lockMaps;
        this.waitStrategies      = waitStrategies;
        this.clientPool          = clientPool;
        this.sharedLockCounts    = new PrimitiveLongIntMap[lockMaps.length];
        this.exclusiveLockCounts = new PrimitiveLongIntMap[lockMaps.length];

        for ( int i = 0; i < sharedLockCounts.length; i++ )
        {
            sharedLockCounts[i] = Primitive.longIntMap();
            exclusiveLockCounts[i] = Primitive.longIntMap();
        }
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        // Grab the global lock map we will be using
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];

        // And grab our local lock maps
        PrimitiveLongIntMap heldShareLocks = sharedLockCounts[resourceType.typeId()];
        PrimitiveLongIntMap heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            // First, check if we already hold this as a shared lock
            int heldCount = heldShareLocks.get( resourceId );
            if(heldCount != -1)
            {
                // We already have a lock on this, just increment our local reference counter.
                heldShareLocks.put( resourceId, heldCount + 1 );
                continue;
            }

            // Second, check if we hold it as an exclusive lock
            if( heldExclusiveLocks.containsKey( resourceId ) )
            {
                // We already have an exclusive lock, so just leave that in place. When the exclusive lock is released,
                // it will be automatically downgraded to a shared lock, since we bumped the share lock reference count.
                heldShareLocks.put( resourceId, 1 );
                continue;
            }

            // We don't hold the lock, so we need to grab it via the global lock map
            int tries = 0;
            SharedLock mySharedLock = null;

            // Retry loop
            while(true)
            {
                // Check if there is a lock for this entity in the map
                ForsetiLockManager.Lock existingLock = lockMap.get( resourceId );

                // No lock
                if(existingLock == null)
                {
                    // Try to create a new shared lock
                    if(mySharedLock == null)
                    {
                        mySharedLock = new SharedLock( this );
                    }

                    if(lockMap.putIfAbsent( resourceId, mySharedLock ) == null)
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
                else if(existingLock instanceof SharedLock)
                {
                    if(((SharedLock)existingLock).acquire(this))
                    {
                        // Success!
                        break;
                    }
                }

                // Someone holds an exclusive lock on this entity
                else if(existingLock instanceof ExclusiveLock)
                {
                    // We need to wait, just let the loop run.
                }
                else
                {
                    throw new UnsupportedOperationException( "Unknown lock type: " + existingLock );
                }

                // Apply the designated wait strategy
                waitStrategies[resourceType.typeId()].apply( tries++ );

                // And take note of who we are waiting for. This is used for deadlock detection.
                markAsWaitingFor( existingLock, resourceType, resourceId );
            }

            // Got the lock, no longer waiting for anyone.
            clearWaitList();

            // Make a local note about the fact that we now hold this lock
            heldShareLocks.put( resourceId, 1 );
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        // For details on how this works, refer to the acquireShared method call, as the two are very similar

        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
        PrimitiveLongIntMap heldLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            int heldCount = heldLocks.get( resourceId );
            if(heldCount != -1)
            {
                // We already have a lock on this, just increment our local reference counter.
                heldLocks.put( resourceId, heldCount + 1 );
                continue;
            }

            // Grab the global lock
            ForsetiLockManager.Lock existingLock;
            int tries = 0;
            while( (existingLock = lockMap.putIfAbsent( resourceId, myExclusiveLock )) != null)
            {
                // If this is a shared lock:
                // Given a grace period of tries (to try and not starve readers), grab an update lock and wait for it
                // to convert to an exclusive lock.
                if( tries > 50 && existingLock instanceof SharedLock)
                {
                    // Then we should upgrade that lock
                    SharedLock sharedLock = (SharedLock) existingLock;
                    if ( tryUpgradeSharedToExclusive( resourceType, lockMap, resourceId, sharedLock ) )
                    {
                        break;
                    }
                }

                waitStrategies[resourceType.typeId()].apply( tries++ );
                markAsWaitingFor( existingLock, resourceType, resourceId );
            }

            clearWaitList();
            heldLocks.put( resourceId, 1 );
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
        PrimitiveLongIntMap heldLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            int heldCount = heldLocks.get( resourceId );
            if(heldCount != -1)
            {
                // We already have a lock on this, just increment our local reference counter.
                heldLocks.put( resourceId, heldCount + 1 );
                continue;
            }

            // Grab the global lock
            ForsetiLockManager.Lock lock;
            if((lock = lockMap.putIfAbsent( resourceId, myExclusiveLock )) != null)
            {
                if(lock instanceof SharedLock && sharedLockCounts[resourceType.typeId()].containsKey( resourceId ))
                {
                    SharedLock sharedLock = (SharedLock) lock;
                    if(sharedLock.tryAcquireUpdateLock( this ))
                    {
                        if(sharedLock.numberOfHolders() == 1)
                        {
                            lockMap.put( resourceId, myExclusiveLock );
                            heldLocks.put( resourceId, 1 );
                            continue;
                        }
                        else
                        {
                            sharedLock.releaseUpdateLock( this );
                            return false;
                        }
                    }
                }
                return false;
            }

            heldLocks.put( resourceId, 1 );
        }
        return true;
    }

    // TODO These kinds of variadic APIs look generally problematic to me.
    // TODO Say we're trying to grab the locks on [1, 2, 3]. Getting the lock on 1
    // TODO succeeds, but getting the lock on 2 fails. Then we return 'false', leave
    // TODO the lock on 1 held, and never try to grab the lock on 3.
    // TODO That sounds like a mess to me. Basically, if you try to grab more than
    // TODO one lock at a time, and methods like this one returns 'false', then you
    // TODO have no idea what locks you did or did not get.
    // TODO I think the API with batched lock-grabbing should be dropped completely.
    // TODO Especially considering the implementation of Forseti, or the general
    // TODO concept of lock managers as a whole, I don't think batched lock-grabbing
    // TODO will ever give any noticable performance benefit anyway.
    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap     = lockMaps[resourceType.typeId()];
        PrimitiveLongIntMap heldShareLocks = sharedLockCounts[resourceType.typeId()];
        PrimitiveLongIntMap heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            int heldCount = heldShareLocks.get( resourceId );
            if(heldCount != -1)
            {
                // We already have a lock on this, just increment our local reference counter.
                heldShareLocks.put( resourceId, heldCount + 1 );
                continue;
            }

            if( heldExclusiveLocks.containsKey( resourceId ) )
            {
                // We already have an exclusive lock, so just leave that in place. When the exclusive lock is released,
                // it will be automatically downgraded to a shared lock, since we bumped the share lock reference count.
                heldShareLocks.put( resourceId, 1 );
                continue;
            }

            while(true)
            {
                ForsetiLockManager.Lock existingLock = lockMap.get( resourceId );
                if(existingLock == null)
                {
                    // Try to create a new shared lock
                    if(lockMap.putIfAbsent( resourceId, new SharedLock(this) ) == null)
                    {
                        // Success!
                        break;
                    }
                }
                else if(existingLock instanceof SharedLock)
                {
                    // Note that there is a "safe" race here where someone may be releasing the last reference to a lock
                    // and thus removing that lock instance (making it unacquirable). In this case, we allow retrying,
                    // even though this is a try-lock call.
                    if(((SharedLock)existingLock).acquire(this))
                    {
                        // Success!
                        break;
                    }
                    else if( ((SharedLock) existingLock).isUpdateLock() )
                    {
                        return false;
                    }
                }
                else if(existingLock instanceof ExclusiveLock)
                {
                    return false;
                }
                else
                {
                    throw new UnsupportedOperationException( "Unknown lock type: " + existingLock );
                }
            }
            heldShareLocks.put( resourceId, 1 );
        }
        return true;
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        for ( long resourceId : resourceIds )
        {
            if ( releaseLocalLock( resourceType, resourceId, sharedLockCounts[resourceType.typeId()] ) )
            {
                continue;
            }

            // Only release if we were not holding an exclusive lock as well
            if( !exclusiveLockCounts[resourceType.typeId()].containsKey( resourceId ) )
            {
                releaseGlobalLock( lockMaps[resourceType.typeId()], resourceId );
            }
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        for ( long resourceId : resourceIds )
        {
            if ( releaseLocalLock( resourceType, resourceId, exclusiveLockCounts[resourceType.typeId()] ) )
            {
                continue;
            }

            if( sharedLockCounts[resourceType.typeId()].containsKey( resourceId ) )
            {
                // We are still holding a shared lock, so swap the exclusive lock for that
                lockMaps[resourceType.typeId()].put( resourceId, new SharedLock( this ) );
            }
            else
            {
                releaseGlobalLock( lockMaps[resourceType.typeId()], resourceId );
            }
        }
    }

    @Override
    public void releaseAllShared()
    {
        for ( int i = 0; i < sharedLockCounts.length; i++ )
        {
            PrimitiveLongIntMap localLocks = sharedLockCounts[i];
            if(localLocks != null)
            {
                int size = localLocks.size();
                localLocks.visitKeys( releaseSharedLockVisitor.initialize( exclusiveLockCounts[i], lockMaps[i] ) );
                if(size <= 32)
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    localLocks.clear();
                }
                else
                {
                    sharedLockCounts[i] = Primitive.longIntMap();
                }
            }
        }
    }

    @Override
    public void releaseAllExclusive()
    {
        for ( int i = 0; i < exclusiveLockCounts.length; i++ )
        {
            PrimitiveLongIntMap localLocks = exclusiveLockCounts[i];
            if(localLocks != null)
            {
                int size = localLocks.size();
                localLocks.visitKeys( releaseExclusiveLockVisitor.initialize( sharedLockCounts[i], lockMaps[i] ) );
                if(size <= 32)
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    localLocks.clear();
                }
                else
                {
                    exclusiveLockCounts[i] = Primitive.longIntMap();
                }
            }
        }
    }

    @Override
    public void releaseAll()
    {
        // Force the release of all locks held.
        for ( int i = 0; i < exclusiveLockCounts.length; i++ )
        {
            PrimitiveLongIntMap exclusiveLocks = exclusiveLockCounts[i];
            PrimitiveLongIntMap sharedLocks = sharedLockCounts[i];

            // Begin releasing exclusive locks, as we may hold both exclusive and shared locks on the same resource,
            // and so releasing exclusive locks means we can "throw away" our shared lock (which would normally have been
            // re-instated after releasing the exclusive lock).
            if(exclusiveLocks != null)
            {
                int size = exclusiveLocks.size();
                exclusiveLocks.visitKeys( releaseExclusiveAndClearSharedVisitor.initialize( sharedLocks, lockMaps[i] ));
                if(size <= 32)
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    exclusiveLocks.clear();
                }
                else
                {
                    exclusiveLockCounts[i] = Primitive.longIntMap();
                }
            }

            // Then release all remaining shared locks
            if(sharedLocks != null)
            {
                int size = sharedLocks.size();
                sharedLocks.visitKeys( releaseSharedDontCheckExclusiveVisitor.initialize( lockMaps[i] ) );
                if(size <= 32)
                {
                    // If the map is small, its fast and nice to GC to clear it. However, if its large, it is
                    // 1) Faster to simply allocate a new one and
                    // 2) Safer, because we guard against clients getting giant maps over time
                    sharedLocks.clear();
                }
                else
                {
                    sharedLockCounts[i] = Primitive.longIntMap();
                }
            }
        }
    }

    @Override
    public void close()
    {
        releaseAll();
        clientPool.release( this );
    }

    @Override
    public int getLockSessionId()
    {
        return myId;
    }

    public int waitListSize()
    {
        return waitList.size();
    }

    public void copyWaitListTo( SimpleBitSet other )
    {
        other.put( waitList );
        // TODO It might make sense to somehow put a StoreLoad barrier here,
        // TODO to expidite the observation of the updated waitList in other clients.
    }

    public boolean isWaitingFor( int clientId )
    {
        // TODO Similarly to the above, make reading the waitList a volatile load.
        return clientId != myId && waitList.contains( clientId );
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

        return myId == that.myId;

    }

    @Override
    public int hashCode()
    {
        return myId;
    }

    @Override
    public String toString()
    {
        return String.format( "ForsetiClient[%d]", myId );
    }

    /** Release a lock from the global pool. */
    private void releaseGlobalLock( ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap, long resourceId )
    {
        ForsetiLockManager.Lock lock = lockMap.get( resourceId );
        if( lock instanceof ExclusiveLock )
        {
            lockMap.remove( resourceId );
        }
        else if( lock instanceof SharedLock && ((SharedLock)lock).release(this) )
        {
            // We were the last to hold this lock, it is now dead and we should remove it.
            lockMap.remove( resourceId );
        }
    }

    /** Release a lock locally, and return true if we still hold more references to that lock. */
    private boolean releaseLocalLock( Locks.ResourceType type, long resourceId, PrimitiveLongIntMap localLocks )
    {
        int lockCount = localLocks.remove( resourceId );
        if(lockCount == -1)
        {
            throw new IllegalStateException( this + " cannot release lock that it does not hold: " +
                    type + "[" + resourceId + "]." );
        }

        if(lockCount > 1)
        {
            localLocks.put( resourceId, lockCount-1 );
            return true;
        }
        return false;
    }

    /** Attempt to upgrade a share lock to an exclusive lock, grabbing the share lock if we don't hold it. */
    private boolean tryUpgradeSharedToExclusive( Locks.ResourceType resourceType, ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap,
                                                 long resourceId, SharedLock sharedLock ) throws AcquireLockTimeoutException
    {
        int tries = 0;
        if(!sharedLockCounts[resourceType.typeId()].containsKey( resourceId ))
        {
            // We don't hold the shared lock, we need to grab it to upgrade it to an exclusive one
            if(!sharedLock.acquire( this ))
            {
                return false;
            }

            try
            {
                if(tryUpgradeToExclusiveWithShareLockHeld( resourceType, lockMap, resourceId, sharedLock, tries ))
                {
                    return true;
                }
                else
                {
                    releaseGlobalLock( lockMap, resourceId );
                    return false;
                }
            }
            catch(Throwable e)
            {
                releaseGlobalLock( lockMap, resourceId );
                throw e;
            }
        }
        else
        {
            // We do hold the shared lock, so no reason to deal with the complexity in the case above.
            return tryUpgradeToExclusiveWithShareLockHeld( resourceType, lockMap, resourceId, sharedLock, tries );
        }
    }

    /** Attempt to upgrade a share lock that we hold to an exclusive lock. */
    private boolean tryUpgradeToExclusiveWithShareLockHeld(
            Locks.ResourceType resourceType,
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap,
            long resourceId,
            SharedLock sharedLock,
            int tries ) throws AcquireLockTimeoutException
    {
        if( sharedLock.tryAcquireUpdateLock(this) )
        {
            try
            {
                // Now we just wait for all clients to release the the share lock
                while(sharedLock.numberOfHolders() > 1)
                {
                    waitStrategies[resourceType.typeId()].apply( tries++ );
                    markAsWaitingFor( sharedLock, resourceType, resourceId );
                }

                // No more people other than us holding this lock. Swap it to exclusive
                // TODO Wait, why do we need to do this? An update lock with zero shared holders is an
                // TODO exclusive lock, no? Why is it not enough to just atomically raise the update bit,
                // TODO and then wait for all the shared holders to relinquish their grasp?
                lockMap.put( resourceId, myExclusiveLock );
                return true;

            }
            catch(DeadlockDetectedException e)
            {
                sharedLock.releaseUpdateLock(this);
                throw e;
            }
            catch(Throwable e)
            {
                sharedLock.releaseUpdateLock(this);
                clearWaitList();
                throw new RuntimeException( e );
            }
        }
        return false;
    }

    private void clearWaitList()
    {
        waitList.clear();
        waitList.put( myId );
    }

    private void markAsWaitingFor( ForsetiLockManager.Lock lock, Locks.ResourceType type, long resourceId )
    {
        clearWaitList();
        lock.copyHolderWaitListsInto( waitList );
        if(lock.anyHolderIsWaitingFor( myId ) && lock.holderWaitListSize() >= waitListSize())
        {
            waitList.clear();
            throw new DeadlockDetectedException( this + " can't acquire " + lock + " on " + type + "("+resourceId+"), because holders of that lock " +
                    "are waiting for " + this + ".\n Wait list:" + lock.describeWaitList() );
        }
    }

    public String describeWaitList()
    {
        StringBuilder sb = new StringBuilder( format( "%nClient[%d] waits for [", id() ) );
        PrimitiveIntIterator iter = waitList.iterator();
        for ( int i = 0; iter.hasNext(); i++ )
        {
            sb.append( i > 0 ? "," : "" ).append( iter.next() );
        }
        sb.append( "]" );
        return sb.toString();
    }

    public int id()
    {
        return myId;
    }

    // Visitors used for bulk ops on the lock maps (such as releasing all locks)

    private class ReleaseSharedLocksVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private PrimitiveLongIntMap exclusiveLockCounts;
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( PrimitiveLongIntMap exclusiveLockCounts,
                                                 ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap )
        {
            this.exclusiveLockCounts = exclusiveLockCounts;
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public boolean visited( long resourceId )
        {
            if(!exclusiveLockCounts.containsKey( resourceId ))
            {
                releaseGlobalLock( lockMap, resourceId );
            }
            return false;
        }
    }

    /**
     * This differs from {@link org.neo4j.kernel.ha.lock.forseti.ForsetiClient.ReleaseSharedLocksVisitor} in that
     * this operates under the guarantee that there will be no exclusive locks held by this client, and so it can remove
     * a check otherwise needed. It is used when releasing all locks.
     */
    private class ReleaseSharedDontCheckExclusiveVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap )
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

    private class ReleaseExclusiveLocksVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private PrimitiveLongIntMap sharedLockCounts;
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( PrimitiveLongIntMap sharedLockCounts,
                                                 ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap )
        {
            this.sharedLockCounts = sharedLockCounts;
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public boolean visited( long resourceId )
        {
            if(sharedLockCounts.containsKey( resourceId ))
            {
                lockMap.put( resourceId, new SharedLock( ForsetiClient.this ) );
            }
            else
            {
                releaseGlobalLock( lockMap, resourceId );
            }
            return false;
        }
    }

    /**
     * This differs from {@link org.neo4j.kernel.ha.lock.forseti.ForsetiClient.ReleaseExclusiveLocksVisitor} in that
     * this will not downgrade exclusive locks to shared locks (if the user holds both), instead, it will release the
     * exclusive lock and remove any local reference to the shared lock. This is an optimization used when releasing
     * all locks.
     */
    private class ReleaseExclusiveLocksAndClearSharedVisitor implements PrimitiveLongVisitor<RuntimeException>
    {
        private PrimitiveLongIntMap sharedLockCounts;
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private PrimitiveLongVisitor<RuntimeException> initialize( PrimitiveLongIntMap sharedLockCounts,
                                                 ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap )
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
            if(sharedLockCounts != null)
            {
                sharedLockCounts.remove( resourceId );
            }
            return false;
        }
    }

    private final ReleaseSharedLocksVisitor releaseSharedLockVisitor = new ReleaseSharedLocksVisitor();
    private final ReleaseExclusiveLocksVisitor releaseExclusiveLockVisitor = new ReleaseExclusiveLocksVisitor();
    private final ReleaseExclusiveLocksAndClearSharedVisitor releaseExclusiveAndClearSharedVisitor = new ReleaseExclusiveLocksAndClearSharedVisitor();
    private final ReleaseSharedDontCheckExclusiveVisitor releaseSharedDontCheckExclusiveVisitor = new ReleaseSharedDontCheckExclusiveVisitor();
}
