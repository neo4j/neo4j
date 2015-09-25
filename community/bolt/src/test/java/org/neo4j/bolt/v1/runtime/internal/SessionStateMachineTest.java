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
package org.neo4j.bolt.v1.runtime.internal;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.Collections;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SessionStateMachineTest
{
    private final GraphDatabaseService db = mock( GraphDatabaseService.class );
    private final ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
    private final Transaction tx = mock( TopLevelTransaction.class );
    private final UsageData usageData = new UsageData();
    private final StatementRunner runner = mock( StatementRunner.class );
    private final SessionStateMachine machine = new SessionStateMachine(
            usageData, db, txBridge, runner, NullLogService.getInstance() );

    @Test
    public void initialStateShouldBeUninitalized()
    {
        // When & Then
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.UNITIALIZED ) );
    }

    @Test
    public void shouldRollbackOpenTransactionOnRollbackInducingError() throws Throwable
    {
        // Given
        final TopLevelTransaction tx = mock( TopLevelTransaction.class );
        when( db.beginTx() ).thenReturn( tx );
        when( runner.run( Matchers.any( SessionState.class ), Matchers.anyString(), Matchers.anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );

        machine.init( "FunClient/1.2", null, Session.Callback.NO_OP );
        machine.beginTransaction();

        // When
        machine.run( "Hello, world!", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // Then
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.ERROR ) );
        verify( tx ).failure();
        verify( tx ).close();

        // And when
        machine.acknowledgeFailure( null, Session.Callback.NO_OP );

        // Then the machine goes back to an idle (no open transaction) state
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.IDLE ) );
    }

    @Test
    public void shouldNotLeaveTransactionOpenOnClientErrors() throws Throwable
    {
        // Given
        final TopLevelTransaction tx = mock( TopLevelTransaction.class );
        when( db.beginTx() ).thenReturn( tx );
        when( runner.run( Matchers.any( SessionState.class ), Matchers.anyString(), Matchers.anyMap() ) )
                .thenThrow( new NoTransactionEffectException() );

        machine.init( "FunClient/1.2", null, Session.Callback.NO_OP );
        machine.beginTransaction();

        // When
        machine.run( "Hello, world!", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // Then
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
        verify(tx).failure();
        verify(tx).close();

        // And when
        machine.acknowledgeFailure( null, Session.Callback.NO_OP );

        // Then the machine goes back to an idle (no open transaction) state
        assertThat(machine.state(), equalTo( SessionStateMachine.State.IDLE ));
    }

    @Test
    public void shouldStopRunningTxOnHalt() throws Throwable
    {
        // When
        machine.init( "FunClient/1.2", null, Session.Callback.NO_OP );
        machine.beginTransaction();
        machine.close();

        // Then
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.STOPPED ) );
        verify( db ).beginTx();
        verify( tx ).close();
    }

    @Test
    public void shouldPublishClientName() throws Throwable
    {
        // When
        machine.init( "FunClient/1.2", null, Session.Callback.NO_OP );

        // Then
        assertTrue( usageData.get( UsageDataKeys.clientNames ).recentItems().contains( "FunClient/1.2" ) );
    }

    @Before
    public void setup()
    {
        when( db.beginTx() ).thenReturn( tx );
    }

    public static class RollbackInducingKernelException extends RuntimeException implements Status.HasStatus
    {
        @Override
        public Status status()
        {
            return Status.General.UnknownFailure;
        }
    }

    public static class NoTransactionEffectException extends RuntimeException implements Status.HasStatus
    {
        @Override
        public Status status()
        {
            return Status.Statement.InvalidSyntax;
        }
    }
}
