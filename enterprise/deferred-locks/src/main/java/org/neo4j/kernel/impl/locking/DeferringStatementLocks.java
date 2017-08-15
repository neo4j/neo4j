/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.coreapi.IsolationLevel;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * A {@link StatementLocks} implementation that defers {@link #optimistic() optimistic}
 * locks using {@link DeferringLockClient}.
 */
public class DeferringStatementLocks implements StatementLocks
{
    private final Locks.Client explicit;
    private final DeferringLockClient implicit;
    private Supplier<LockTracer> lockTracerSupplier;

    public DeferringStatementLocks( Locks.Client explicit )
    {
        this.explicit = explicit;
        this.implicit = new DeferringLockClient( this.explicit );
        lockTracerSupplier = DEFAULT_LOCK_TRACER_SUPPLIER;
    }

    @Override
    public Locks.Client explicit()
    {
        return explicit;
    }

    @Override
    public void explicitAcquireExclusive( ResourceType type, long... resourceId )
    {
        explicit.acquireExclusive( lockTracerSupplier.get(), type, resourceId );
    }

    @Override
    public void explicitReleaseExclusive( ResourceType type, long resourceId )
    {
        explicit.releaseExclusive( type, resourceId );
    }

    @Override
    public void explicitAcquireShared( ResourceType type, long... resourceId )
    {
        explicit.acquireShared( lockTracerSupplier.get(), type, resourceId );
    }

    @Override
    public void explicitReleaseShared( ResourceType type, long resourceId )
    {
        explicit.releaseShared( type, resourceId );
    }

    @Override
    public int getLockSessionId()
    {
        return explicit.getLockSessionId();
    }

    @Override
    public Locks.Client optimistic()
    {
        return implicit;
    }

    @Override
    public void prepareForCommit()
    {
        implicit.acquireDeferredLocks();
    }

    @Override
    public void stop()
    {
        implicit.stop();
    }

    @Override
    public void close()
    {
        implicit.close();
    }

    @Override
    public Stream<? extends ActiveLock> activeLocks()
    {
        return Stream.concat( explicit.activeLocks(), implicit.activeLocks() );
    }

    @Override
    public long activeLockCount()
    {
        return explicit.activeLockCount() + implicit.activeLockCount();
    }

    @Override
    public void setIsolationLevel( IsolationLevel isolationLevel )
    {
        throw new IllegalStateException(
                "Isolation level cannot be changed when deferred locking is enabled. Unset or change the `" +
                DeferringStatementLocksFactory.deferred_locks_enabled.name() + "` setting to `false`, to allow " +
                "changing the isolation level on transactions." );
    }

    @Override
    public void setLockTracerSupplier( Supplier<LockTracer> lockTracerSupplier )
    {
        this.lockTracerSupplier = lockTracerSupplier;
    }
}
