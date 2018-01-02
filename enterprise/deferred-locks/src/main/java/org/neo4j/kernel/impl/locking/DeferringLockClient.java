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
package org.neo4j.kernel.impl.locking;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class DeferringLockClient implements Locks.Client
{
    private final Locks.Client clientDelegate;
    private final Map<LockUnit,MutableInt> locks = new TreeMap<>();
    private volatile boolean stopped;

    public DeferringLockClient( Locks.Client clientDelegate )
    {
        this.clientDelegate = clientDelegate;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, resourceId, false );
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, resourceId, true );
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long resourceId )
    {
        assertNotStopped();

        removeLock( resourceType, resourceId, false );
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long resourceId )
    {
        assertNotStopped();

        removeLock( resourceType, resourceId, true );
    }

    void acquireDeferredLocks()
    {
        assertNotStopped();

        long[] current = new long[10];
        int cursor = 0;
        Locks.ResourceType currentType = null;
        boolean currentExclusive = false;
        for ( LockUnit lockUnit : locks.keySet() )
        {
            if ( currentType == null ||
                 (currentType.typeId() != lockUnit.resourceType().typeId() ||
                  currentExclusive != lockUnit.isExclusive()) )
            {
                // New type, i.e. flush the current array down to delegate in one call
                flushLocks( current, cursor, currentType, currentExclusive );

                cursor = 0;
                currentType = lockUnit.resourceType();
                currentExclusive = lockUnit.isExclusive();
            }

            // Queue into current batch
            if ( cursor == current.length )
            {
                current = Arrays.copyOf( current, cursor * 2 );
            }
            current[cursor++] = lockUnit.resourceId();
        }
        flushLocks( current, cursor, currentType, currentExclusive );
    }

    private void flushLocks( long[] current, int cursor, Locks.ResourceType currentType, boolean exclusive )
    {
        if ( cursor > 0 )
        {
            long[] resourceIds = Arrays.copyOf( current, cursor );
            if ( exclusive )
            {
                clientDelegate.acquireExclusive( currentType, resourceIds );
            }
            else
            {
                clientDelegate.acquireShared( currentType, resourceIds );
            }
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
        clientDelegate.stop();
    }

    @Override
    public void close()
    {
        stopped = true;
        clientDelegate.close();
    }

    @Override
    public int getLockSessionId()
    {
        return clientDelegate.getLockSessionId();
    }

    private void assertNotStopped()
    {
        if ( stopped )
        {
            throw new LockClientStoppedException( this );
        }
    }

    private void addLock( Locks.ResourceType resourceType, long resourceId, boolean exclusive )
    {
        LockUnit lockUnit = new LockUnit( resourceType, resourceId, exclusive );
        MutableInt lockCount = locks.get( lockUnit );
        if ( lockCount == null )
        {
            lockCount = new MutableInt();
            locks.put( lockUnit, lockCount );
        }
        lockCount.increment();
    }

    private void removeLock( Locks.ResourceType resourceType, long resourceId, boolean exclusive )
    {
        LockUnit lockUnit = new LockUnit( resourceType, resourceId, exclusive );
        MutableInt lockCount = locks.get( lockUnit );
        if ( lockCount == null )
        {
            throw new IllegalStateException(
                    "Cannot release " + (exclusive ? "exclusive" : "shared") + " lock that it " +
                    "does not hold: " + resourceType + "[" + resourceId + "]." );
        }

        lockCount.decrement();

        if ( lockCount.intValue() == 0 )
        {
            locks.remove( lockUnit );
        }
    }
}
