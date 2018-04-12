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

import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.StatementLocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlaveStatementLocksTest
{
    @Test
    public void acquireDeferredSharedLocksOnPrepareForCommit()
    {
        StatementLocks statementLocks = mock( StatementLocks.class );
        SlaveLocksClient slaveLocksClient = mock( SlaveLocksClient.class );
        when( statementLocks.optimistic() ).thenReturn( slaveLocksClient );

        SlaveStatementLocks slaveStatementLocks = new SlaveStatementLocks( statementLocks );
        slaveStatementLocks.prepareForCommit( LockTracer.NONE );

        verify( statementLocks ).prepareForCommit( LockTracer.NONE );
        verify( slaveLocksClient ).acquireDeferredSharedLocks( LockTracer.NONE );
    }
}
