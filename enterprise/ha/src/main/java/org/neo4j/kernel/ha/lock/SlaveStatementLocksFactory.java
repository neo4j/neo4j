/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
