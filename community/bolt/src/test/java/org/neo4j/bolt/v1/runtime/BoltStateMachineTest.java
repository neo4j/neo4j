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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltMatchers.canReset;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.hasNoTransaction;
import static org.neo4j.bolt.testing.BoltMatchers.hasTransaction;
import static org.neo4j.bolt.testing.BoltMatchers.inState;
import static org.neo4j.bolt.testing.BoltMatchers.isClosed;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.verifyOneResponse;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v1.runtime.BoltStateMachine.State.CONNECTED;
import static org.neo4j.bolt.v1.runtime.BoltStateMachine.State.FAILED;
import static org.neo4j.bolt.v1.runtime.BoltStateMachine.State.READY;
import static org.neo4j.bolt.v1.runtime.BoltStateMachine.State.STREAMING;
import static org.neo4j.bolt.v1.runtime.MachineRoom.EMPTY_PARAMS;
import static org.neo4j.bolt.v1.runtime.MachineRoom.USER_AGENT;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachine;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachineWithTransaction;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachineWithTransactionSPI;
import static org.neo4j.test.assertion.Assert.assertException;

public class BoltStateMachineTest
{

    @Test
    public void allStateTransitionsShouldSendExactlyOneResponseToTheClient() throws Exception
    {
        for ( BoltStateMachine.State initialState : BoltStateMachine.State.values() )
        {
            verifyOneResponse( initialState,
                    ( machine, recorder ) -> machine.init( USER_AGENT, emptyMap(), recorder ) );
            verifyOneResponse( initialState, BoltStateMachine::ackFailure );
            verifyOneResponse( initialState, BoltStateMachine::reset );
            verifyOneResponse( initialState,
                    ( machine, recorder ) -> machine.run( "statement", EMPTY_PARAMS, recorder ) );
            verifyOneResponse( initialState, BoltStateMachine::discardAll );
            verifyOneResponse( initialState, BoltStateMachine::pullAll );
        }
    }

    @Test
    public void initialStateShouldBeConnected()
    {
        assertThat( newMachine(), inState( CONNECTED ) );
    }

    @Test
    public void shouldRollbackOpenTransactionOnReset() throws Throwable
    {
        // Given a FAILED machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction( READY );
        machine.state = FAILED;

        // When RESET occurs
        machine.reset( nullResponseHandler() );

        // Then the transaction should have been rolled back...
        assertThat( machine, hasNoTransaction() );

        // ...and the machine should go back to READY
        assertThat( machine, inState( READY ) );
    }

    @Test
    public void shouldRollbackOpenTransactionOnClose() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction( READY );

        // When the machine is shut down
        machine.close();

        // Then the transaction should have been rolled back
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    public void shouldPublishClientName() throws Throwable
    {
        verify( newMachine( READY ).spi ).udcRegisterClient( USER_AGENT );
    }

    @Test
    public void shouldBeAbleToResetWhenInReadyState() throws Throwable
    {
        assertThat( newMachine( READY ), canReset() );
    }

    @Test
    public void shouldResetWithOpenTransaction() throws Throwable
    {
        assertThat( newMachineWithTransaction( READY ), canReset() );
    }

    @Test
    public void shouldResetWithOpenTransactionAndOpenResult() throws Throwable
    {
        // Given a ready machine with an open transaction...
        final BoltStateMachine machine = newMachineWithTransaction( READY );

        // ...and an open result
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldResetWithOpenResult() throws Throwable
    {
        // Given a ready machine...
        final BoltStateMachine machine = newMachine( READY );

        // ...and an open result
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldResetWhenFailed() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction( READY );

        // ...and (for some reason) a FAILED state
        machine.state = FAILED;

        // Then
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldFailWhenOutOfOrderRollback() throws Throwable
    {
        // Given a failed machine
        final BoltStateMachine machine = newMachine( FAILED );

        // When
        machine.run( "ROLLBACK", EMPTY_PARAMS, nullResponseHandler() );

        // Then
        assertThat( machine, inState( FAILED ) );
    }

    @Test
    public void shouldGoBackToReadyAfterAckFailure() throws Throwable
    {
        // Given a failed machine
        final BoltStateMachine machine = newMachine( FAILED );

        // When
        machine.ackFailure( nullResponseHandler() );

        // Then
        assertThat( machine, inState( READY ) );
    }

    @Test
    public void shouldNotRollbackOpenTransactionOnAckFailure() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction( READY );

        // ...and (for some reason) a FAILED state
        machine.state = FAILED;

        // When the failure is acknowledged
        machine.ackFailure( nullResponseHandler() );

        // Then the transaction should still be open
        assertThat( machine, hasTransaction() );
    }

    @Test
    public void shouldRemainStoppedAfterInterrupted() throws Throwable
    {
        // Given a ready machine
        final BoltStateMachine machine = newMachine( READY );

        // ...which is subsequently closed
        machine.close();
        assertThat( machine, isClosed() );

        // When and interrupt and reset occurs
        machine.interrupt();
        machine.reset( nullResponseHandler() );

        // Then the machine should remain closed
        assertThat( machine, isClosed() );
    }

    @Test
    public void shouldBeAbleToKillMessagesAheadInLineWithAnInterrupt() throws Throwable
    {
        // Given
        final BoltStateMachine machine = newMachine( READY );

        // When
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );
        machine.reset( recorder );
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void multipleInterruptsShouldBeMatchedWithMultipleResets() throws Throwable
    {
        // Given
        final BoltStateMachine machine = newMachine( READY );

        // When
        machine.interrupt();
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );
        machine.reset( recorder );
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );

        // But when
        recorder.reset();
        machine.reset( recorder );
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void testPublishingError() throws Throwable
    {
        // Given a new ready machine...
        BoltStateMachine machine = newMachine( READY );

        // ...and a result ready to be retrieved...
        machine.run( "RETURN 1", null, nullResponseHandler() );

        // ...and a handler guaranteed to break
        BoltResponseRecorder recorder = new BoltResponseRecorder()
        {
            @Override
            public void onRecords( BoltResult result, boolean pull ) throws Exception
            {
                throw new RuntimeException( "I've been expecting you, Mr Bond." );
            }
        };

        // When we pull using that handler
        machine.pullAll( recorder );

        // Then the breakage should surface as a FAILURE
        assertThat( recorder.nextResponse(), failedWithStatus( Status.General.UnknownError ) );

        // ...and the machine should have entered a FAILED state
        assertThat( machine, inState( FAILED ) );
    }

    @Test
    public void testRollbackError() throws Throwable
    {
        // Given
        BoltStateMachine machine = newMachine( READY );

        // Given there is a running transaction
        machine.run( "BEGIN", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // And given that transaction will fail to roll back
        TransactionStateMachine txMachine = (TransactionStateMachine) machine.ctx.statementProcessor;
        when( txMachine.ctx.currentTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransactionFailureException( "No Mr. Bond, I expect you to die." ) ).
                when( txMachine.ctx.currentTransaction ).close();

        // When
        machine.run( "ROLLBACK", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // Then
        assertThat( machine, inState( FAILED ) );
    }

    @Test
    public void testFailOnNestedTransactions() throws Throwable
    {
        // Given
        BoltStateMachine machine = newMachine( READY );

        // Given there is a running transaction
        machine.run( "BEGIN", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When
        machine.run( "BEGIN", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // Then
        assertThat( machine, inState( FAILED ) );
    }

    @Test
    public void testCantDoAnythingIfInFailedState() throws Throwable
    {
        // Given a FAILED machine
        BoltStateMachine machine = newMachine( FAILED );

        // Then no RUN...
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        assertThat( machine, inState( FAILED ) );
        // ...DISCARD_ALL...
        machine.discardAll( nullResponseHandler() );
        assertThat( machine, inState( FAILED ) );
        // ...or PULL_ALL should be possible
        machine.pullAll( nullResponseHandler() );
        assertThat( machine, inState( FAILED ) );
    }

    @Test
    public void testUsingResetToAcknowledgeError() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = newMachine( FAILED );

        // When I RESET...
        machine.reset( recorder );

        // ...successfully
        assertThat( recorder.nextResponse(), succeeded() );

        // Then if I RUN a statement...
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // ...everything should be fine again
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void actionsDisallowedBeforeInitialized() throws Throwable
    {
        // Given
        BoltStateMachine machine = newMachine();

        // When
        try
        {
            machine.run( "RETURN 1", null, nullResponseHandler() );
            fail( "Failed to fail fatally" );
        }

        // Then
        catch ( BoltConnectionFatality e )
        {
            // fatality correctly generated
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateOnAuthExpiryDuringREADYRun() throws Throwable
    {
        // Given
        TransactionStateMachine.SPI transactionSPI = mock( TransactionStateMachine.SPI.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( transactionSPI ).beginTransaction( any() );

        BoltStateMachine machine = newMachineWithTransactionSPI( transactionSPI );
        machine.state = READY;

        // When & Then
        assertException( () -> machine.run( "THIS WILL BE IGNORED", EMPTY_PARAMS, nullResponseHandler() ),
                BoltConnectionAuthFatality.class, "Auth expired!" );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateOnAuthExpiryDuringSTREAMING() throws Throwable
    {
        // Given
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( responseHandler )
                .onRecords( any(), anyBoolean() );
        BoltStateMachine machine = newMachine( STREAMING );
        // We assume the only implementation of statement processor is TransactionStateMachine
        ((TransactionStateMachine) machine.statementProcessor()).ctx.currentResult = BoltResult.EMPTY;

        // When & Then
        assertException( () -> machine.pullAll( responseHandler ),
                BoltConnectionAuthFatality.class, "Auth expired!" );

        // When & Then
        assertException( () -> machine.discardAll( responseHandler ),
                BoltConnectionAuthFatality.class, "Auth expired!" );
    }

    @Test
    public void callResetEvenThoughAlreadyClosed() throws Throwable
    {
        // Given
        BoltStateMachine machine = newMachine( READY );

        // When we close
        TransactionStateMachine statementProcessor = (TransactionStateMachine) machine.statementProcessor();
        machine.close();
        assertThat( statementProcessor.ctx.currentTransaction, nullValue() );
        assertTrue( machine.ctx.closed );

        //But someone runs a query and thus opens a new transaction
        statementProcessor.run( "RETURN 1", EMPTY_PARAMS );
        assertThat( statementProcessor.ctx.currentTransaction, notNullValue() );

        // Then, when we close again we should make sure the transaction is closed againg
        machine.close();
        assertThat( statementProcessor.ctx.currentTransaction, nullValue() );
    }

    @Test
    public void shouldCallOnTerminateWhenClosing() throws Throwable
    {
        // Given
        BoltStateMachineSPI spi = mock( BoltStateMachineSPI.class, RETURNS_MOCKS );
        BoltChannel boltChannel = mock( BoltChannel.class );
        final BoltStateMachine machine = new BoltStateMachine( spi, boltChannel, Clock.systemUTC() );

        // When
        machine.close();

        // Then
        verify( spi ).onTerminate( machine );
    }

    @Test
    public void shouldCloseBoltChannelWhenClosed()
    {
        BoltStateMachineSPI spi = mock( BoltStateMachineSPI.class );
        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachine( spi, boltChannel, Clock.systemUTC() );

        machine.close();

        verify( boltChannel ).close();
    }
}
