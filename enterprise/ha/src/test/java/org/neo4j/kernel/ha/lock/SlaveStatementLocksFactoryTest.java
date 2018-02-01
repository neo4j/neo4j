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

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SlaveStatementLocksFactoryTest
{

    @Test
    public void createSlaveStatementLocks()
    {
        StatementLocksFactory delegate = mock( StatementLocksFactory.class );
        Locks locks = mock( Locks.class );
        Config config = Config.defaults();

        SlaveStatementLocksFactory slaveStatementLocksFactory = new SlaveStatementLocksFactory( delegate );
        slaveStatementLocksFactory.initialize( locks, config );
        StatementLocks statementLocks = slaveStatementLocksFactory.newInstance();

        assertThat( statementLocks, instanceOf( SlaveStatementLocks.class ) );
        verify( delegate ).initialize( locks, config );
        verify( delegate ).newInstance();
    }
}
