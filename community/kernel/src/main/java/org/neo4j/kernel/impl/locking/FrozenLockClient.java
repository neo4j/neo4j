/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.locking;

import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.FrozenLocksException;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

/**
 * A lock client that prevents interactions with the state of the locks. This is used to guarantee that we do not perform any locking while reading from
 * the transaction in parallel, which would be dangerous since {@link Locks.Client} are not thread safe.
 */
public class FrozenLockClient implements Locks.Client
{
    private final Locks.Client delegate;
    private int nesting;

    public FrozenLockClient( Locks.Client delegate )
    {
        this.delegate = delegate;
        this.nesting = 1;
    }

    public Locks.Client getRealLockClient()
    {
        return delegate;
    }

    public void freeze()
    {
        nesting++;
    }

    public boolean thaw()
    {
        nesting--;
        return nesting == 0;
    }

    @Override
    public void initialize( LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config )
    {
        delegate.initialize( leaseClient, transactionId, memoryTracker, config );
    }

    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        throw frozenLockException();
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        throw frozenLockException();
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        throw frozenLockException();
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        throw frozenLockException();
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        throw frozenLockException();
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        throw frozenLockException();
    }

    @Override
    public void prepareForCommit()
    {
        throw frozenLockException();
    }

    @Override
    public void stop()
    {
        delegate.stop();
    }

    @Override
    public void close()
    {
        throw frozenLockException();
    }

    @Override
    public long getTransactionId()
    {
        return delegate.getTransactionId();
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return delegate.activeLocks();
    }

    @Override
    public boolean holdsLock( long id, ResourceType resource, LockType lockType )
    {
        return delegate.holdsLock( id, resource, lockType );
    }

    @Override
    public long activeLockCount()
    {
        return delegate.activeLockCount();
    }

    private FrozenLocksException frozenLockException()
    {
        return new FrozenLocksException( delegate.getTransactionId() );
    }
}
