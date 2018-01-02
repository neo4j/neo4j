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
package org.neo4j.kernel.ha.lock;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.StatementLocks;

/**
 * Slave specific statement locks that in addition to standard functionality provided by configured delegate
 * will grab selected shared locks on master during prepareForCommit phase.
 * <p>
 * In order to prevent various indexes collisions on master during transaction commit that originate on one of the
 * slaves we need to grab same locks on {@link ResourceTypes#LABEL} and {@link ResourceTypes#RELATIONSHIP_TYPE} that
 * where obtained on origin. To be able to do that and also prevent shared locks to be propagates to master in cases of
 * read only transactions we will postpone obtaining them till we know that we participating in a
 * transaction that performs modifications. That's why we will obtain them only as
 * part of {@link #prepareForCommit(LockTracer)} call.
 * </p>
 */
public class SlaveStatementLocks implements StatementLocks
{
    private final StatementLocks delegate;

    SlaveStatementLocks( StatementLocks delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public Locks.Client pessimistic()
    {
        return delegate.pessimistic();
    }

    @Override
    public Locks.Client optimistic()
    {
        return delegate.optimistic();
    }

    @Override
    public void prepareForCommit( LockTracer lockTracer )
    {
        delegate.prepareForCommit( lockTracer );
        ((SlaveLocksClient) optimistic()).acquireDeferredSharedLocks( lockTracer );
    }

    @Override
    public void stop()
    {
        delegate.stop();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public Stream<? extends ActiveLock> activeLocks()
    {
        return delegate.activeLocks();
    }

    @Override
    public long activeLockCount()
    {
        return delegate.activeLockCount();
    }
}
