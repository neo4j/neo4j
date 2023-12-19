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
package org.neo4j.kernel.impl.locking;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

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
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, resourceId, false );
        }
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, resourceId, true );
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean reEnterShared( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();
        for ( long resourceId : resourceIds )
        {
            removeLock( resourceType, resourceId, false );
        }

    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();
        for ( long resourceId : resourceIds )
        {
            removeLock( resourceType, resourceId, true );
        }
    }

    void acquireDeferredLocks( LockTracer lockTracer )
    {
        assertNotStopped();

        long[] current = new long[10];
        int cursor = 0;
        ResourceType currentType = null;
        boolean currentExclusive = false;
        for ( LockUnit lockUnit : locks.keySet() )
        {
            if ( currentType == null ||
                 (currentType.typeId() != lockUnit.resourceType().typeId() ||
                  currentExclusive != lockUnit.isExclusive()) )
            {
                // New type, i.e. flush the current array down to delegate in one call
                flushLocks( lockTracer, current, cursor, currentType, currentExclusive );

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
        flushLocks( lockTracer, current, cursor, currentType, currentExclusive );
    }

    private void flushLocks( LockTracer lockTracer, long[] current, int cursor, ResourceType currentType, boolean
            exclusive )
    {
        if ( cursor > 0 )
        {
            long[] resourceIds = Arrays.copyOf( current, cursor );
            if ( exclusive )
            {
                clientDelegate.acquireExclusive( lockTracer, currentType, resourceIds );
            }
            else
            {
                clientDelegate.acquireShared( lockTracer, currentType, resourceIds );
            }
        }
    }

    @Override
    public void prepare()
    {
        clientDelegate.prepare();
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

    @Override
    public Stream<? extends ActiveLock> activeLocks()
    {
        return locks.keySet().stream();
    }

    @Override
    public long activeLockCount()
    {
        return locks.size();
    }

    private void assertNotStopped()
    {
        if ( stopped )
        {
            throw new LockClientStoppedException( this );
        }
    }

    private void addLock( ResourceType resourceType, long resourceId, boolean exclusive )
    {
        LockUnit lockUnit = new LockUnit( resourceType, resourceId, exclusive );
        MutableInt lockCount = locks.computeIfAbsent( lockUnit, k -> new MutableInt() );
        lockCount.increment();
    }

    private void removeLock( ResourceType resourceType, long resourceId, boolean exclusive )
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
