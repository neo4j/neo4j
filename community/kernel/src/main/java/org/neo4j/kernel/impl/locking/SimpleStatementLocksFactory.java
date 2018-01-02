/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.configuration.Config;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StatementLocksFactory} that creates {@link SimpleStatementLocks}.
 */
public class SimpleStatementLocksFactory implements StatementLocksFactory
{
    private Locks locks;

    public SimpleStatementLocksFactory()
    {
    }

    /**
     * Creates a new factory initialized with given {@code locks}.
     *
     * @param locks the locks to use.
     */
    public SimpleStatementLocksFactory( Locks locks )
    {
        initialize( locks, null );
    }

    @Override
    public void initialize( Locks locks, Config config )
    {
        this.locks = requireNonNull( locks );
    }

    @Override
    public StatementLocks newInstance()
    {
        if ( locks == null )
        {
            throw new IllegalStateException( "Factory has not been initialized" );
        }

        return new SimpleStatementLocks( locks.newClient() );
    }
}
