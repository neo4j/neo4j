/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
