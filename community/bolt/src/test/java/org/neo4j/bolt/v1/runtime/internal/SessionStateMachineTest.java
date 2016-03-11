/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Collections;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.BasicAuthenticationResult;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.udc.UsageData;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.runtime.Session.Callback.noOp;
import static org.neo4j.bolt.v1.runtime.internal.SessionStateMachine.State.ERROR;
import static org.neo4j.bolt.v1.runtime.internal.SessionStateMachine.State.IDLE;

public class SessionStateMachineTest
{
    private final KernelTransaction ktx = mock( KernelTransaction.class );

    private final SessionStateMachine.SPI spi = mock( SessionStateMachine.SPI.class );
    private final SessionStateMachine machine = new SessionStateMachine( spi );

    @Test
    public void initialStateShouldBeUninitalized()
    {
        // When & Then
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.UNINITIALIZED ) );
    }

    @Test
    public void shouldRollbackExplicitTransactionOnReset() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );

        machine.init( "FunClient/1.2", emptyMap(), null, noOp()  );
        machine.beginImplicitTransaction();

        // When
        machine.run( "Hello, world!", emptyMap(), null, noOp() );

        // Then
        assertThat( machine.state(), equalTo( ERROR ) );
        verify( ktx ).failure();

        // And when
        machine.reset( null, noOp()  );

        // Then the machine goes back to an idle (no open transaction) state
        verify( ktx ).close();
        assertThat( machine.state(), equalTo( IDLE ) );
    }

    @Test
    public void shouldStopRunningTxOnHalt() throws Throwable
    {
        // When
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );
        machine.beginTransaction();
        machine.close();

        // Then
        assertThat( machine.state(), CoreMatchers.equalTo( SessionStateMachine.State.STOPPED ) );
        verify( spi ).beginTransaction( any( KernelTransaction.Type.class ), any( AccessMode.class ));
        verify( ktx ).close();
    }

    @Test
    public void shouldPublishClientName() throws Throwable
    {
        // When
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );

        // Then
        verify( spi ).udcRegisterClient( "FunClient/1.2" );
    }

    @Test
    public void shouldResetToIdleOnIdle() throws Throwable
    {
        // Given
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );

        // When
        TestCallback callback = new TestCallback();
        machine.reset( null, callback );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
        assertThat( callback.completedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldResetToIdleOnInTransaction() throws Throwable
    {
        // Given
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );
        machine.beginTransaction();

        // When
        TestCallback callback = new TestCallback();
        machine.reset( null, callback );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
        assertThat( callback.completedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldResetToIdleOnStreamOpenWithExplicitTransaction() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenReturn( mock( RecordStream.class ) );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );
        machine.beginTransaction();
        machine.run( "RETURN 1", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // When
        TestCallback callback = new TestCallback();
        machine.reset( null, callback );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
        assertThat( callback.completedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldResetToIdleOnStreamOpenWithImplicitTransaction() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenReturn( mock( RecordStream.class ) );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );
        machine.run( "RETURN 1", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // When
        TestCallback callback = new TestCallback();
        machine.reset( null, callback );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
        assertThat( callback.completedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldCallStartedWhenStartingProcessing() throws Throwable
    {
        // Given
        TestCallback callback = new TestCallback();

        // When
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, callback );

        // Then
        assertThat( callback.startedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldResetToIdleOnError() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );
        machine.run( "RETURN 1", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // When
        TestCallback callback = new TestCallback();
        machine.reset( null, callback );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
        assertThat( callback.completedCount, equalTo( 1 ) );
    }

    @Test
    public void shouldErrorWhenOutOfOrderRollback() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, Session.Callback.NO_OP );

        // When
        machine.run( "ROLLBACK", Collections.EMPTY_MAP, null, Session.Callback.NO_OP );

        // Then
        assertThat( machine.state(), equalTo( ERROR ) );
    }

    @Test
    public void shouldGoBackToIdleAfterImplicitTxFailure() throws Throwable
    {
        // Given
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, noOp() );

        // When
        machine.run( "Statement that fails", emptyMap(), null, noOp() );
        machine.ackFailure( null, noOp() );

        // Then
        assertThat( machine.state(), equalTo( IDLE ) );
    }

    @Test
    public void shouldMoveBackTo_IN_TRANSACTION_afterAckFailureOnExplicitTx() throws Throwable
    {
        // Given all statements will fail
        when( spi.run( any( SessionStateMachine.class ), anyString(), anyMap() ) )
                .thenThrow( new RollbackInducingKernelException() );
        machine.init( "FunClient/1.2",  Collections.<String, Object>emptyMap(), null, noOp() );

        // When
        machine.beginTransaction();
        machine.run( "statement that fails", emptyMap(), null, noOp() );
        machine.ackFailure( null, noOp() );

        // Then
        assertThat( machine.state(), equalTo( SessionStateMachine.State.IN_TRANSACTION ) );
    }

    @Before
    public void setup() throws AuthenticationException
    {
        when( spi.authenticate( any() ) ).thenReturn( new BasicAuthenticationResult(AccessMode.Static.FULL, false) );
        when( spi.beginTransaction( any(), any() )).thenAnswer( (inv) -> {
            // The state machine will ask for different types of transactions, we
            // modify out mock to suit it's needs. I'm not sure this is a good way
            // to do this.
            KernelTransaction.Type type = (KernelTransaction.Type) inv.getArguments()[0];
            when( ktx.transactionType() ).thenReturn( type );
            return ktx;
        });

    }

    static class TestCallback<V> extends Session.Callback.Adapter<V, Object>
    {
        public int completedCount;
        public int ignoredCount;
        public int startedCount;

        @Override
        public void started( Object attachment )
        {
            startedCount += 1;
        }

        @Override
        public void completed( Object attachment )
        {
            completedCount += 1;
        }

        @Override
        public void ignored( Object attachment )
        {
            ignoredCount += 1;
        }

        @Override
        public void failure( Neo4jError err, Object attachment )
        {
            err.cause().printStackTrace( System.err );
        }
    }

    public static class RollbackInducingKernelException extends RuntimeException implements Status.HasStatus
    {
        @Override
        public Status status()
        {
            return Status.General.UnknownError;
        }
    }
}
