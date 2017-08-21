/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.coreapi.IsolationLevel;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * A {@link StatementLocks} implementation that uses given {@link Locks.Client} for both
 * {@link #optimistic() optimistic} and explicit locks.
 */
public class SimpleStatementLocks implements StatementLocks
{
    private final Locks.Client client;
    private Supplier<LockTracer> lockTracerSupplier;

    public SimpleStatementLocks( Locks.Client client )
    {
        this.client = client;
        lockTracerSupplier = DEFAULT_LOCK_TRACER_SUPPLIER;
    }

    @Override
    public void explicitAcquireExclusive( ResourceType type, long... resourceId )
    {
        client.acquireExclusive( lockTracerSupplier.get(), type, resourceId );
    }

    @Override
    public void explicitReleaseExclusive( ResourceType type, long resourceId )
    {
        client.releaseExclusive( type, resourceId );
    }

    @Override
    public void explicitAcquireShared( ResourceType type, long... resourceId )
    {
        client.acquireShared( lockTracerSupplier.get(), type, resourceId );
    }

    @Override
    public void explicitReleaseShared( ResourceType type, long resourceId )
    {
        client.releaseShared( type, resourceId );
    }

    @Override
    public int getLockSessionId()
    {
        return client.getLockSessionId();
    }

    @Override
    public Locks.Client optimistic()
    {
        return client;
    }

    @Override
    public void prepareForCommit()
    {
        // Locks where grabbed eagerly by client so no need to prepare
    }

    @Override
    public void stop()
    {
        client.stop();
    }

    @Override
    public void close()
    {
        client.close();
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

    @Override
    public void setIsolationLevel( IsolationLevel isolationLevel )
    {
        if ( !isolationLevel.isSupported() )
        {
            throw new IllegalStateException( "The isolation level " + isolationLevel + " is not supported." );
        }
    }

    @Override
    public void setLockTracerSupplier( Supplier<LockTracer> lockTracerSupplier )
    {
        this.lockTracerSupplier = lockTracerSupplier;
    }
}
