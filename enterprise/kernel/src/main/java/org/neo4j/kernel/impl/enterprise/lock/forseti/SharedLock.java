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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

/**
 * A Forseti share lock. Can be upgraded to an update lock, which will block new attempts at acquiring shared lock,
 * but will allow existing holders to complete.
 */
class SharedLock implements ForsetiLockManager.Lock
{
    /**
     * The update lock flag is inlined into the ref count integer, in order to allow common CAS operations across
     * both the update flag and the refCount simultaneously. This avoids a nasty series of race conditions, but
     * makes the reference counting code much mode complicated. May be worth revisiting.
     */
    private static final int UPDATE_LOCK_FLAG = 1<<31;

    /**
     * No more holders than this allowed, don't change this without changing the sizing of
     * {@link #clientsHoldingThisLock}.
     */
    private static final int MAX_HOLDERS = 4680;

    // TODO Investigate inlining and padding the refCount.
    // TODO My gut feeling tells me there's a high chance of false-sharing
    // TODO on these unpadded AtomicIntegers.
    private final AtomicInteger refCount = new AtomicInteger(1);

    /**
     * When reading this, keep in mind the main design goals here: Releasing and acquiring this lock should not require
     * synchronization, and the lock should have as low of a memory footprint as possible.
     *
     * An array of arrays containing references to clients holding this lock. Each client can only show up once.
     * When the lock is created only the first reference array is created (so the last three slots in the outer array
     * are empty). The outer array is populated when the reference arrays are filled up, with exponentially larger
     * reference arrays:
     *
     * clientsHoldingThisLock[0] = 8 slots
     * clientsHoldingThisLock[1] = 64 slots
     * clientsHoldingThisLock[2] = 512 slots
     * clientsHoldingThisLock[3] = 4096 slots
     *
     * Allowing a total of 4680 transactions holding the same shared lock simultaneously.
     *
     * This data structure was chosen over using regular resizing of the array, because we need to be able to increase
     * the size of the array without requiring synchronization between threads writing to the array and threads trying
     * to resize (since the threads writing to the array are on one of the hottest code paths in the database).
     *
     * This data structure is, however, not optimal, since it requires O(n) at worst to search for a slot and to remove
     * a client from the array. This should be revisited in the future.
     */
    private final AtomicReferenceArray<ForsetiClient>[] clientsHoldingThisLock = new AtomicReferenceArray[4];

    /** Client that holds the update lock, if any. */
    private ForsetiClient updateHolder;

    SharedLock(ForsetiClient client)
    {
        addClientHoldingLock( client );
    }

    public boolean acquire(ForsetiClient client)
    {
        // First, bump refcount to make sure no one drops this lock on the floor
        if(!acquireReference())
        {
            return false;
        }

        // Then add our wait list to the pile of things waiting in case if we are not there yet
        // if we already waiting we will release a reference to keep counter in sync
        if ( !clientHoldsThisLock( client ) )
        {
            // try to add client to a clients that holding current lock.
            return addClientHoldingLock( client );
        } else {
            releaseReference();
            return false;
        }
    }

    public boolean release(ForsetiClient client)
    {
        removeClientHoldingLock( client );
        return releaseReference();
    }

    @Override
    public void copyHolderWaitListsInto( SimpleBitSet waitList )
    {
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            for ( int j = 0; holders != null && j < holders.length(); j++ )
            {
                ForsetiClient client = holders.get( j );
                if(client != null)
                {
                    client.copyWaitListTo( waitList );
                }
            }
        }
    }

    @Override
    public int holderWaitListSize()
    {
        int size = 0;
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            for ( int j = 0; holders != null && j < holders.length(); j++ )
            {
                ForsetiClient client = holders.get( j );
                if(client != null)
                {
                    size += client.waitListSize();
                }
            }
        }
        return size;
    }

    @Override
    public boolean anyHolderIsWaitingFor( int clientId )
    {
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            for ( int j = 0; holders != null && j < holders.length(); j++ )
            {
                ForsetiClient client = holders.get( j );
                if(client != null && client.isWaitingFor( clientId ))
                {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean tryAcquireUpdateLock( ForsetiClient client )
    {
        while(true)
        {
            int refs = refCount.get();
            if(refs > 0 /* UPDATE_LOCK flips the sign bit, so refs will be < 0 if it is an update lock. */)
            {
                if(refCount.compareAndSet( refs, refs | UPDATE_LOCK_FLAG ))
                {
                    updateHolder = client;
                    return true;
                }
            }
            else
            {
                return false;
            }
        }
    }

    public void releaseUpdateLock(ForsetiClient client)
    {
        while(true)
        {
            int refs = refCount.get();
            cleanUpdateHolder();
            if(refCount.compareAndSet( refs, refs & ~UPDATE_LOCK_FLAG ))
            {
                return;
            }
        }
    }

    public void cleanUpdateHolder()
    {
        updateHolder = null;
    }

    public int numberOfHolders()
    {
        return refCount.get() & ~UPDATE_LOCK_FLAG;
    }

    public boolean isUpdateLock()
    {
        return (refCount.get() & UPDATE_LOCK_FLAG) == UPDATE_LOCK_FLAG;
    }

    @Override
    public String describeWaitList()
    {
        StringBuilder sb = new StringBuilder( "SharedLock[" );
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            boolean first = true;
            for ( int j = 0; holders != null && j < holders.length(); j++ )
            {
                ForsetiClient current = holders.get( j );
                if(current != null)
                {
                    sb.append( first ? "" : ", " ).append( current.describeWaitList() );
                    first = false;
                }
            }
        }
        return sb.append( "]" ).toString();
    }

    @Override
    public String toString()
    {
        // TODO we should only read out the refCount once, and build a deterministic string based on that
        if(isUpdateLock())
        {
            return "UpdateLock{" +
                "objectId=" + System.identityHashCode( this ) +
                ", refCount=" + (refCount.get() & ~UPDATE_LOCK_FLAG) +
                ", holder=" + updateHolder +
            '}';
        }
        else
        {
            return "SharedLock{" +
                    "objectId=" + System.identityHashCode( this ) +
                    ", refCount=" + refCount +
                    '}';
        }
    }

    private void removeClientHoldingLock( ForsetiClient client )
    {
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            if(holders == null)
            {
                break;
            }

            for ( int j = 0; j < holders.length(); j++ )
            {
                ForsetiClient current = holders.get( j );
                if(current != null && current.equals( client ))
                {
                    holders.set( j, null );
                    return;
                }
            }
        }

        throw new IllegalStateException( client + " asked to be removed from holder list, but it does not hold " + this );
    }

    private boolean addClientHoldingLock( ForsetiClient client )
    {
        while(true)
        {
            for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
            {
                AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
                if(holders == null)
                {
                    holders = addHolderArray( i );
                }

                for ( int j = 0; j < holders.length(); j++ )
                {
                    ForsetiClient c = holders.get( j );
                    if(c == null)
                    {
                        // TODO This means we do CAS on each entry, very likely hitting a lot of failures until we
                        // TODO find a slot. We should look into better strategies here.
                        // TODO One such strategy could be binary searching for a free slot, and then linear scan
                        // TODO after that if the CAS fails on the slot we found with binary search.
                        if( holders.compareAndSet( j, null, client ) )
                        {
                            return true;
                        }
                    }
                }
            }
        }
    }

    private boolean acquireReference()
    {
        while(true)
        {
            int refs = refCount.get();
            // UPDATE_LOCK flips the sign bit, so refs will be < 0 if it is an update lock.
            if(refs > 0 && refs < MAX_HOLDERS)
            {
                if(refCount.compareAndSet( refs, refs+1 ))
                {
                    return true;
                }
            }
            else
            {
                return false;
            }
        }
    }

    private boolean releaseReference()
    {
        while(true)
        {
            int refAndUpdateFlag = refCount.get();
            int newRefCount = (refAndUpdateFlag & ~UPDATE_LOCK_FLAG) - 1;
            if(refCount.compareAndSet( refAndUpdateFlag, newRefCount | (refAndUpdateFlag & UPDATE_LOCK_FLAG)))
            {
                return newRefCount == 0;
            }
        }
    }

    private synchronized AtomicReferenceArray<ForsetiClient> addHolderArray( int slot )
    {
        if( clientsHoldingThisLock[slot] == null )
        {
            clientsHoldingThisLock[slot] = new AtomicReferenceArray<>( (int)(8 * Math.pow( 8, slot )) );
        }
        return clientsHoldingThisLock[slot];
    }

    private boolean clientHoldsThisLock( ForsetiClient client )
    {
        for ( int i = 0; i < clientsHoldingThisLock.length; i++ )
        {
            AtomicReferenceArray<ForsetiClient> holders = clientsHoldingThisLock[i];
            for ( int j = 0; holders != null && j < holders.length(); j++ )
            {
                ForsetiClient current = holders.get( j );
                if(current != null && current.equals( client ))
                {
                    return true;
                }
            }
        }
        return false;
    }
}
