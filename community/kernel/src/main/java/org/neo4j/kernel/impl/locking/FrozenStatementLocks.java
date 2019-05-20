/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.internal.kernel.api.exceptions.FrozenLocksException;
import org.neo4j.lock.LockTracer;

/**
 * StatementLocks implementation that stops all optimistic and pessimistic interactions
 * with the LockClients. This is used to guarantee that we do not perform any locking
 * while reading from the transaction in parallel, which would be dangerous since LockClients
 * are not thread safe.
 */
public class FrozenStatementLocks implements StatementLocks
{
    private final StatementLocks realStatementLocks;
    private int nesting;

    public FrozenStatementLocks( StatementLocks realStatementLocks )
    {
        this.realStatementLocks = realStatementLocks;
        this.nesting = 1;
    }

    public StatementLocks getRealStatementLocks()
    {
        return realStatementLocks;
    }

    // StatementLocks

    @Override
    public Locks.Client pessimistic()
    {
        throw new FrozenLocksException( realStatementLocks.pessimistic().getLockSessionId() );
    }

    @Override
    public Locks.Client optimistic()
    {
        throw new FrozenLocksException( realStatementLocks.pessimistic().getLockSessionId() );
    }

    @Override
    public void prepareForCommit( LockTracer lockTracer )
    {
        throw new FrozenLocksException( realStatementLocks.pessimistic().getLockSessionId() );
    }

    @Override
    public void stop()
    {
        realStatementLocks.stop();
    }

    @Override
    public void close()
    {
        throw new FrozenLocksException( realStatementLocks.pessimistic().getLockSessionId() );
    }

    @Override
    public Stream<? extends ActiveLock> activeLocks()
    {
        return realStatementLocks.activeLocks();
    }

    @Override
    public long activeLockCount()
    {
        return realStatementLocks.activeLockCount();
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
}
