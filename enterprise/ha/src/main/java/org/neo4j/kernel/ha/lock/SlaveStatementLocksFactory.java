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

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

/**
 * Statement locks factory that will produce slave specific statement locks
 * that are aware how to grab shared locks for labels and relationship types
 * during slave commit
 */
public class SlaveStatementLocksFactory implements StatementLocksFactory
{
    private final StatementLocksFactory delegate;

    public SlaveStatementLocksFactory( StatementLocksFactory delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void initialize( Locks locks, Config config )
    {
        delegate.initialize( locks, config );
    }

    @Override
    public StatementLocks newInstance()
    {
        StatementLocks statementLocks = delegate.newInstance();
        return new SlaveStatementLocks( statementLocks );
    }
}
