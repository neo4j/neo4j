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

/**
 * A {@link StatementLocks} implementation that uses given {@link Locks.Client} for both
 * {@link #optimistic() optimistic} and {@link #pessimistic() pessimistic} locks.
 */
public class SimpleStatementLocks implements StatementLocks
{
    private final Locks.Client client;

    public SimpleStatementLocks( Locks.Client client )
    {
        this.client = client;
    }

    @Override
    public Locks.Client pessimistic()
    {
        return client;
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
}
