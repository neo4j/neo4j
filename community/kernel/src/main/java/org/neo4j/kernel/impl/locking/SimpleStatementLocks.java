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
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * A {@link StatementLocks} implementation that uses given {@link Locks.Client} for both
 * optimistic and pessimistic locks.
 */
public class SimpleStatementLocks implements StatementLocks
{
    private static final String defaultIsolationLevel = FeatureToggles.getString(
            StatementLocks.class, "defaultIsolationLevel", null );

    private final Locks.Client client;
    private Supplier<LockTracer> lockTracerSupplier;
    private IsolationLevel isolationLevel;
    private boolean takesEntityIteratorLocks; // Not taken with Read Committed

    public SimpleStatementLocks( Locks.Client client )
    {
        this.client = client;
        lockTracerSupplier = DEFAULT_LOCK_TRACER_SUPPLIER;
        if ( defaultIsolationLevel != null )
        {
            setIsolationLevel( IsolationLevel.valueOf( defaultIsolationLevel ) );
        }
    }

    @Override
    public void pessimisticAcquireExclusive( ResourceType type, long... resourceId )
    {
        client.acquireExclusive( getTracer(), type, resourceId );
    }

    private LockTracer getTracer()
    {
        return lockTracerSupplier.get();
    }

    @Override
    public void pessimisticReleaseExclusive( ResourceType type, long resourceId )
    {
        client.releaseExclusive( type, resourceId );
    }

    @Override
    public void pessimisticAcquireShared( ResourceType type, long... resourceId )
    {
        client.acquireShared( getTracer(), type, false, resourceId );
    }

    @Override
    public void pessimisticReleaseShared( ResourceType type, long resourceId )
    {
        client.releaseShared( type, resourceId );
    }

    @Override
    public int getLockSessionId()
    {
        return client.getLockSessionId();
    }

    @Override
    public void uniquenessConstraintEntryAcquireExclusive( long resource )
    {
        client.acquireExclusive( getTracer(), ResourceTypes.INDEX_ENTRY, resource );
    }

    @Override
    public void uniquenessConstraintEntryReleaseExclusive( long resource )
    {
        client.releaseExclusive( ResourceTypes.INDEX_ENTRY, resource );
    }

    @Override
    public void uniquenessConstraintEntryAcquireShared( long resource )
    {
        client.acquireShared( getTracer(), ResourceTypes.INDEX_ENTRY, false, resource );
    }

    @Override
    public void uniquenessConstraintEntryReleaseShared( long resource )
    {
        client.releaseShared( ResourceTypes.INDEX_ENTRY, resource );
    }

    @Override
    public void schemaModifyAcquireExclusive( ResourceType type, long resource )
    {
        client.acquireExclusive( getTracer(), type, resource );
    }

    @Override
    public void schemaModifyAcquireShared( ResourceType type, long resource )
    {
        client.acquireShared( getTracer(), type, false, resource  );
    }

    @Override
    public void entityModifyAcquireExclusive( ResourceType type, long resource )
    {
        client.acquireExclusive( getTracer(), type, resource );
    }

    @Override
    public void entityModifyReleaseExclusive( ResourceType type, long resource )
    {
        client.releaseExclusive( type, resource );
    }

    @Override
    public void entityIterateAcquireShared( ResourceType type, long resource )
    {
        if ( takesEntityIteratorLocks )
        {
            // In Iterator Stability, these locks are short-lived.
            // In Repeatable Read, they would be long-lived.
            client.acquireShared( getTracer(), type, true, resource );
        }
    }

    @Override
    public void entityIterateReleaseShared( ResourceType type, long resource )
    {
        if ( takesEntityIteratorLocks )
        {
            client.releaseShared( type, resource );
        }
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
        if ( this.isolationLevel != null )
        {
            throw new IllegalStateException( "Isolation level cannot be set more than once." );
        }
        this.isolationLevel = isolationLevel;
        if ( isolationLevel == IsolationLevel.IteratorStability )
        {
            takesEntityIteratorLocks = true;
        }
    }

    @Override
    public void setLockTracerSupplier( Supplier<LockTracer> lockTracerSupplier )
    {
        this.lockTracerSupplier = lockTracerSupplier;
    }
}
