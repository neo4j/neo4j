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
