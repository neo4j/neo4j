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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link StatementLocks} implementation that defers {@link #optimistic() optimistic}
 * locks using {@link DeferringLockClient}.
 */
public class DeferringStatementLocks implements StatementLocks
{
    private final Locks.Client explicit;
    private final DeferringLockClient implicit;

    public DeferringStatementLocks( Locks.Client explicit )
    {
        this.explicit = explicit;
        this.implicit = new DeferringLockClient( this.explicit );
    }

    @Override
    public Locks.Client pessimistic()
    {
        return explicit;
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
    public Collection<Locks.ActiveLock> activeLocks()
    {
        Collection<Locks.ActiveLock> explicit = this.explicit.activeLocks(), implicit = this.implicit.activeLocks();
        // minimize (re-)allocation
        if ( explicit instanceof ArrayList<?> && explicit.size() > implicit.size() )
        {
            List<Locks.ActiveLock> locks = (List<Locks.ActiveLock>) explicit;
            locks.addAll( implicit );
            return locks;
        }
        else if ( implicit instanceof ArrayList<?> && implicit.size() > explicit.size() )
        {
            List<Locks.ActiveLock> locks = (List<Locks.ActiveLock>) implicit;
            locks.addAll( explicit );
            return locks;
        }
        else
        {
            List<Locks.ActiveLock> locks = new ArrayList<>( explicit.size() + implicit.size() );
            locks.addAll( explicit );
            locks.addAll( implicit );
            return locks;
        }
    }
}
