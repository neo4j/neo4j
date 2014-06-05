/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.FlyweightPool;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.kernel.impl.util.concurrent.WaitStrategy;

public class ForsetiClient implements Locks.Client
{
    /** Id for this client */
    private final int myId;

    /** resourceType -> lock map */
    private final ConcurrentMap[] lockMaps;

    /** resourceType -> wait strategy */
    private final WaitStrategy<AcquireLockTimeoutException>[] waitStrategies;

    /** Handle to return client to pool when closed. */
    private final FlyweightPool<ForsetiClient> clientPool;

    // TODO We should really look into some kind of primitive maps here.
    // TODO As a stop-gap, we could start out by using AtomicInteger values, and thereby remove a lot
    // TODO of object creation due to the boxing of the integers.
    /** resourceType -> Map( resourceId -> num locks ) */
    private final Map<Long, Integer>[] sharedLockCounts;

    /** resourceType -> Map( resourceId -> num locks ) */
    private final Map<Long, Integer>[] exclusiveLockCounts;

    /** List of other clients this client is waiting for. */
    private final SimpleBitSet waitList = new SimpleBitSet( 64 );

    /** For exclusive locks, we only need a single re-usable one per client. */
    private final ExclusiveLock myExclusiveLock = new ExclusiveLock(this);

    public ForsetiClient( int id,
                          ConcurrentMap[] lockMaps,
                          WaitStrategy[] waitStrategies,
                          FlyweightPool<ForsetiClient> clientPool )
    {
        this.myId                = id;
        this.lockMaps            = lockMaps;
        this.waitStrategies      = waitStrategies;
        this.clientPool          = clientPool;
        this.sharedLockCounts    = new HashMap[lockMaps.length];
        this.exclusiveLockCounts = new HashMap[lockMaps.length];

        for ( int i = 0; i < sharedLockCounts.length; i++ )
        {
            sharedLockCounts[i] = new HashMap<>();
            exclusiveLockCounts[i] = new HashMap<>();
        }
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap     = lockMaps[resourceType.typeId()];
        Map<Long, Integer> heldShareLocks     = sharedLockCounts[resourceType.typeId()];
        Map<Long, Integer> heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            Integer heldCount = heldShareLocks.get( resourceId );
            if(heldCount != null)
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

            int tries = 0;
            SharedLock mySharedLock = null;
            while(true)
            {
                ForsetiLockManager.Lock existingLock = lockMap.get( resourceId );
                if(existingLock == null)
                {
                    // Try to create a new shared lock
                    if(mySharedLock == null)
                    {
                        mySharedLock = new SharedLock( this );
                    }
                    if(lockMap.putIfAbsent( resourceId, mySharedLock ) == null)
                    {
                        // Success!
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }
                else if(existingLock instanceof SharedLock)
                {
                    if(((SharedLock)existingLock).acquire(this))
                    {
                        // Success!
                        break;
                    }
                }
                else if(existingLock instanceof ExclusiveLock)
                {
                    // We need to wait, just let the loop run.
                }
                else
                {
                    throw new UnsupportedOperationException( "Unknown lock type: " + existingLock );
                }

                waitStrategies[resourceType.typeId()].apply( tries++ );
                markAsWaitingFor( existingLock, resourceType, resourceId );
            }

            clearWaitList();
            heldShareLocks.put( resourceId, 1 );
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
        Map<Long, Integer> heldLocks      = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            Integer heldCount = heldLocks.get( resourceId );
            if(heldCount != null)
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
        Map<Long, Integer> heldLocks      = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            Integer heldCount = heldLocks.get( resourceId );
            if(heldCount != null)
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
                            return true;
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
        Map<Long, Integer> heldShareLocks     = sharedLockCounts[resourceType.typeId()];
        Map<Long, Integer> heldExclusiveLocks = exclusiveLockCounts[resourceType.typeId()];

        for ( long resourceId : resourceIds )
        {
            Integer heldCount = heldShareLocks.get( resourceId );
            if(heldCount != null)
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
            Map<Long, Integer> localLocks = sharedLockCounts[i];
            if(localLocks != null)
            {
                ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[i];
                for ( Long resourceId : localLocks.keySet() )
                {
                    if(!exclusiveLockCounts[i].containsKey( resourceId ))
                    {
                        releaseGlobalLock( lockMap, resourceId );
                    }
                }
                localLocks.clear();
            }
        }
    }

    @Override
    public void releaseAllExclusive()
    {
        for ( int i = 0; i < exclusiveLockCounts.length; i++ )
        {
            Map<Long, Integer> localLocks = exclusiveLockCounts[i];
            if(localLocks != null)
            {
                ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[i];
                for ( Long resourceId : localLocks.keySet() )
                {
                    if(sharedLockCounts[i].containsKey( resourceId ))
                    {
                        lockMap.put( resourceId, new SharedLock( this ) );
                    }
                    else
                    {
                        releaseGlobalLock( lockMap, resourceId );
                    }
                }
                localLocks.clear();
            }
        }
    }

    @Override
    public void releaseAll()
    {
        releaseAll( exclusiveLockCounts );
        releaseAll( sharedLockCounts );
    }

    private void releaseAll( Map<Long, Integer>[] lockCounts )
    {
        for ( int i = 0; i < lockCounts.length; i++ )
        {
            Map<Long, Integer> localLocks = lockCounts[i];
            if(localLocks != null)
            {
                ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[i];
                for ( Long resourceId : localLocks.keySet() )
                {
                    releaseGlobalLock( lockMap, resourceId );
                }
                localLocks.clear();
            }
        }
    }

    @Override
    public void close()
    {
        releaseAll();
        clientPool.release( this );
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
    private boolean releaseLocalLock( Locks.ResourceType type, long resourceId, Map<Long, Integer> localLocks )
    {
        Integer lockCount = localLocks.remove( resourceId );
        if(lockCount == null)
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
        StringBuilder sb = new StringBuilder( this + " waits for [" );
        PrimitiveIntIterator iter = waitList.iterator();
        while(iter.hasNext())
        {
            sb.append( iter.next() ).append( ", " );
        }
        sb.append( "]" );
        return sb.toString();
    }

    public int id()
    {
        return myId;
    }
}
