/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.internal.session;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.ndp.runtime.internal.StatementRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SessionStateMachineTest
{
    private final GraphDatabaseService db = mock( GraphDatabaseService.class );
    private final ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
    private final Transaction tx = mock( TopLevelTransaction.class );
    private final SessionStateMachine machine = new SessionStateMachine(
            db, txBridge, mock( StatementRunner.class ), NullLogService.getInstance() );

    @Test
    public void initialStateShouldBeIdle()
    {
        // When
        SessionStateMachine machine = new SessionStateMachine(
                mock( GraphDatabaseService.class ), mock( ThreadToStatementContextBridge.class ),
                mock( StatementRunner.class ), NullLogService.getInstance() );

        // Then
        assertThat( machine.state(), equalTo( SessionStateMachine.State.IDLE ) );
    }

    @Test
    public void shouldStopRunningTxOnHalt() throws Throwable
    {
        // When
        machine.beginTransaction();
        machine.close();

        // Then
        assertThat( machine.state(), equalTo( SessionStateMachine.State.STOPPED ) );
        verify( db ).beginTx();
        verify( tx ).close();
    }

    @Before
    public void setup()
    {
        when( db.beginTx() ).thenReturn( tx );
    }
}
