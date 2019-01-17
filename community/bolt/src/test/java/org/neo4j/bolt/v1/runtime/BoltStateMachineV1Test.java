/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import static org.neo4j.bolt.v1.runtime.MachineRoom.EMPTY_PARAMS;
import static org.neo4j.bolt.v1.runtime.MachineRoom.USER_AGENT;
import static org.neo4j.bolt.v1.runtime.MachineRoom.init;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachine;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachineWithTransaction;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachineWithTransactionSPI;

public class BoltStateMachineV1Test
{
    @Test
    public void allStateTransitionsShouldSendExactlyOneResponseToTheClient() throws Exception
    {
        List<RequestMessage> messages = Arrays.asList( new InitMessage( USER_AGENT, emptyMap() ),
                AckFailureMessage.INSTANCE,
                ResetMessage.INSTANCE,
                new RunMessage( "RETURN 1", EMPTY_PARAMS ),
                DiscardAllMessage.INSTANCE, PullAllMessage.INSTANCE );

        for ( RequestMessage message: messages )
        {
            verifyOneResponse( ( machine, recorder ) -> machine.process( message, recorder ) );
        }
    }

    @Test
    public void initialStateShouldBeConnected()
    {
        assertThat( newMachine(), inState( ConnectedState.class ) );
    }

    @Test
    public void shouldRollbackOpenTransactionOnReset() throws Throwable
    {
        // Given a FAILED machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction();
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When RESET occurs
        machine.process( ResetMessage.INSTANCE, nullResponseHandler() );

        // Then the transaction should have been rolled back...
        assertThat( machine, hasNoTransaction() );

        // ...and the machine should go back to READY
        assertThat( machine, inState( ReadyState.class ) );
    }

    @Test
    public void shouldRollbackOpenTransactionOnClose() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction();

        // When the machine is shut down
        machine.close();

        // Then the transaction should have been rolled back
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    public void shouldBeAbleToResetWhenInReadyState() throws Throwable
    {
        BoltStateMachine machine = init( newMachine() );
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldResetWithOpenTransaction() throws Throwable
    {
        BoltStateMachine machine = newMachineWithTransaction();
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldResetWithOpenTransactionAndOpenResult() throws Throwable
    {
        // Given a ready machine with an open transaction...
        final BoltStateMachine machine = newMachineWithTransaction();

        // ...and an open result
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldResetWithOpenResult() throws Throwable
    {
        // Given a ready machine...
        final BoltStateMachine machine = init( newMachine() );

        // ...and an open result
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
    }

    @Test
    public void shouldFailWhenOutOfOrderRollback() throws Throwable
    {
        // Given a failed machine
        final BoltStateMachine machine = newMachine();
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When
        machine.process( new RunMessage( "ROLLBACK", EMPTY_PARAMS ), nullResponseHandler() );

        // Then
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void shouldGoBackToReadyAfterAckFailure() throws Throwable
    {
        // Given a failed machine
        final BoltStateMachine machine = newMachine();
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When
        machine.process( AckFailureMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( machine, inState( ReadyState.class ) );
    }

    @Test
    public void shouldNotRollbackOpenTransactionOnAckFailure() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction();

        // ...and (for some reason) a FAILED state
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When the failure is acknowledged
        machine.process( AckFailureMessage.INSTANCE, nullResponseHandler() );

        // Then the transaction should still be open
        assertThat( machine, hasTransaction() );
    }

    @Test
    public void shouldRemainStoppedAfterInterrupted() throws Throwable
    {
        // Given a ready machine
        final BoltStateMachine machine = init( newMachine() );

        // ...which is subsequently closed
        machine.close();
        assertThat( machine, isClosed() );

        // When and interrupt and reset occurs
        machine.interrupt();
        machine.process( ResetMessage.INSTANCE, nullResponseHandler() );

        // Then the machine should remain closed
        assertThat( machine, isClosed() );
    }

    @Test
    public void shouldBeAbleToKillMessagesAheadInLineWithAnInterrupt() throws Throwable
    {
        // Given
        final BoltStateMachine machine = init( newMachine() );

        // When
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );
        machine.process( ResetMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void multipleInterruptsShouldBeMatchedWithMultipleResets() throws Throwable
    {
        // Given
        final BoltStateMachine machine = init( newMachine() );

        // When
        machine.interrupt();
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );
        machine.process( ResetMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );

        // But when
        recorder.reset();
        machine.process( ResetMessage.INSTANCE, recorder );
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void testPublishingError() throws Throwable
    {
        // Given a new ready machine...
        BoltStateMachine machine = init( newMachine() );

        // ...and a result ready to be retrieved...
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() );

        // ...and a handler guaranteed to break
        BoltResponseRecorder recorder = new BoltResponseRecorder()
        {
            @Override
            public void onRecords( BoltResult result, boolean pull )
            {
                throw new RuntimeException( "I've been expecting you, Mr Bond." );
            }
        };

        // When we pull using that handler
        machine.process( PullAllMessage.INSTANCE, recorder );

        // Then the breakage should surface as a FAILURE
        assertThat( recorder.nextResponse(), failedWithStatus( Status.General.UnknownError ) );

        // ...and the machine should have entered a FAILED state
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void testRollbackError() throws Throwable
    {
        // Given
        BoltStateMachine machine = init( newMachine() );

        // Given there is a running transaction
        machine.process( new RunMessage( "BEGIN", EMPTY_PARAMS ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // And given that transaction will fail to roll back
        TransactionStateMachine txMachine = txStateMachine( machine );
        when( txMachine.ctx.currentTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransactionFailureException( "No Mr. Bond, I expect you to die." ) ).
                when( txMachine.ctx.currentTransaction ).close();

        // When
        machine.process( new RunMessage( "ROLLBACK", EMPTY_PARAMS ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void testFailOnNestedTransactions() throws Throwable
    {
        // Given
        BoltStateMachine machine = init( newMachine() );

        // Given there is a running transaction
        machine.process( new RunMessage( "BEGIN", EMPTY_PARAMS ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // When
        machine.process( new RunMessage( "BEGIN", EMPTY_PARAMS ), nullResponseHandler() );
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );

        // Then
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void testCantDoAnythingIfInFailedState() throws Throwable
    {
        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // Then no RUN...
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
        // ...DISCARD_ALL...
        machine.process( DiscardAllMessage.INSTANCE, nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
        // ...or PULL_ALL should be possible
        machine.process( PullAllMessage.INSTANCE, nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void testUsingResetToAcknowledgeError() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When I RESET...
        machine.process( ResetMessage.INSTANCE, recorder );

        // ...successfully
        assertThat( recorder.nextResponse(), succeeded() );

        // Then if I RUN a statement...
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), recorder );

        // ...everything should be fine again
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void actionsDisallowedBeforeInitialized()
    {
        // Given
        BoltStateMachine machine = newMachine();

        // When
        try
        {
            machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() );
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
        TransactionStateMachineSPI transactionSPI = mock( TransactionStateMachineSPI.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( transactionSPI ).beginTransaction( any(), any(), any() );

        BoltStateMachine machine = newMachineWithTransactionSPI( transactionSPI );

        // When & Then
        try
        {
            machine.process( new RunMessage( "THIS WILL BE IGNORED", EMPTY_PARAMS ), nullResponseHandler() );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateOnAuthExpiryDuringSTREAMINGPullAll() throws Throwable
    {
        // Given
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( responseHandler )
                .onRecords( any(), anyBoolean() );
        BoltStateMachine machine = init( newMachine() );
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() ); // move to streaming state
        // We assume the only implementation of statement processor is TransactionStateMachine
        txStateMachine( machine ).ctx.currentResult = BoltResult.EMPTY;

        // When & Then
        try
        {
            machine.process( PullAllMessage.INSTANCE, responseHandler );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateOnAuthExpiryDuringSTREAMINGDiscardAll() throws Throwable
    {
        // Given
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( responseHandler )
                .onRecords( any(), anyBoolean() );
        BoltStateMachine machine = init( newMachine() );
        machine.process( new RunMessage( "RETURN 1", EMPTY_PARAMS ), nullResponseHandler() ); // move to streaming state
        // We assume the only implementation of statement processor is TransactionStateMachine
        txStateMachine( machine ).ctx.currentResult = BoltResult.EMPTY;

        // When & Then
        try
        {
            machine.process( DiscardAllMessage.INSTANCE, responseHandler );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }
    }

    @Test
    public void callResetEvenThoughAlreadyClosed() throws Throwable
    {
        // Given
        BoltStateMachine machine = init( newMachine() );

        // When we close
        TransactionStateMachine statementProcessor = txStateMachine( machine );
        machine.close();
        assertThat( statementProcessor.ctx.currentTransaction, nullValue() );
        assertThat( machine, isClosed() );

        //But someone runs a query and thus opens a new transaction
        statementProcessor.run( "RETURN 1", EMPTY_PARAMS );
        assertThat( statementProcessor.ctx.currentTransaction, notNullValue() );

        // Then, when we close again we should make sure the transaction is closed again
        machine.close();
        assertThat( statementProcessor.ctx.currentTransaction, nullValue() );
    }

    @Test
    public void shouldCloseBoltChannelWhenClosed()
    {
        BoltStateMachineV1SPI spi = mock( BoltStateMachineV1SPI.class );
        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachineV1( spi, boltChannel, Clock.systemUTC() );

        machine.close();

        verify( boltChannel ).close();
    }

    @Test
    public void shouldSetPendingErrorOnMarkFailedIfNoHandler()
    {
        BoltStateMachineV1SPI spi = mock( BoltStateMachineV1SPI.class );
        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachineV1( spi, boltChannel, Clock.systemUTC() );
        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );

        machine.markFailed( error );

        assertEquals( error, pendingError( machine ) );
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( new InitMessage( "Test/1.0", emptyMap() ), handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( new RunMessage( "RETURN 1", VirtualValues.EMPTY_MAP ), handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( PullAllMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( DiscardAllMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testReadyStateAfterMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( ResetMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldGotoReadyStateOnNextAckFailureMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testReadyStateAfterMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( AckFailureMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.handleExternalFailure( Neo4jError.from( Status.Request.Invalid, "invalid" ), handler ) );
    }

    @Test
    public void shouldSetPendingIgnoreOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        BoltStateMachine machine = newMachine();
        Neo4jError error1 = Neo4jError.from( new RuntimeException() );
        machine.markFailed( error1 );

        Neo4jError error2 = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );
        machine.markFailed( error2 );

        assertTrue( pendingIgnore( machine ) );
        assertEquals( error1, pendingError( machine ) ); // error remained the same and was ignored
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> machine.process( new InitMessage( "Test/1.0", emptyMap() ), handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
                ( machine, handler ) -> machine.process( new RunMessage( "RETURN 1", VirtualValues.EMPTY_MAP ), handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> machine.process( PullAllMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> machine.process( DiscardAllMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldSuccessIfAlreadyFailed( ( machine, handler ) -> machine.process( ResetMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextAckFailureMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldSuccessIfAlreadyFailed( ( machine, handler ) -> machine.process( AckFailureMessage.INSTANCE, handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
                ( machine, handler ) -> machine.handleExternalFailure( Neo4jError.from( Status.Request.Invalid, "invalid" ), handler ) );
    }

    @Test
    public void shouldInvokeResponseHandlerOnMarkFailedIfThereIsHandler() throws Exception
    {
        BoltStateMachine machine = init( newMachine() );
        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        ((BoltStateMachineV1) machine).connectionState().setResponseHandler( responseHandler );
        machine.markFailed( error );

        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( FailedState.class ) );
        verify( responseHandler ).markFailed( error );
    }

    @Test
    public void shouldNotFailWhenMarkedForTerminationAndPullAll() throws Exception
    {
        BoltStateMachineV1SPI spi = mock( BoltStateMachineV1SPI.class, RETURNS_MOCKS );
        BoltStateMachine machine = init( newMachine( spi ) );
        machine.process( new RunMessage( "RETURN 42", EMPTY_PARAMS ), nullResponseHandler() ); // move to streaming state
        txStateMachine( machine ).ctx.currentResult = BoltResult.EMPTY;

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        machine.markForTermination();
        machine.process( PullAllMessage.INSTANCE, responseHandler );

        verify( spi, never() ).reportError( any() );
        assertThat( machine, not( inState( FailedState.class ) ) );
    }

    @Test
    public void shouldSucceedOnResetOnFailedState() throws Exception
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );

        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // When I RESET...
        machine.interrupt();
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( ResetMessage.INSTANCE, recorder );

        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.NoThreadsAvailable ) );
        // ...successfully
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldSucceedOnConsecutiveResetsOnFailedState() throws Exception
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );

        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( PullAllMessage.INSTANCE, recorder );

        // When I RESET...
        machine.interrupt();
        machine.interrupt();
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( ResetMessage.INSTANCE, recorder );
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( ResetMessage.INSTANCE, recorder );

        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.NoThreadsAvailable ) );
        // ...successfully
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    private static void testMarkFailedOnNextMessage( ThrowingBiConsumer<BoltStateMachine,BoltResponseHandler,BoltConnectionFatality> action ) throws Exception
    {
        // Given
        BoltStateMachine machine = init( newMachine() );
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );
        machine.markFailed( error );

        // When
        action.accept( machine, responseHandler );

        // Expect
        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( FailedState.class ) );
        verify( responseHandler ).markFailed( error );
    }

    private static void testReadyStateAfterMarkFailedOnNextMessage( ThrowingBiConsumer<BoltStateMachine,BoltResponseHandler,BoltConnectionFatality> action )
            throws Exception
    {
        // Given
        BoltStateMachine machine = init( newMachine() );
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );
        machine.markFailed( error );

        // When
        action.accept( machine, responseHandler );

        // Expect
        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( ReadyState.class ) );
        verify( responseHandler, never() ).markFailed( any() );
        verify( responseHandler, never() ).markIgnored();
    }

    private static void testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
            ThrowingBiConsumer<BoltStateMachine,BoltResponseHandler,BoltConnectionFatality> action ) throws Exception
    {
        // Given
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );
        machine.markFailed( error );

        // When
        action.accept( machine, responseHandler );

        // Expect
        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( FailedState.class ) );
        verify( responseHandler ).markIgnored();
    }

    private static void testMarkFailedShouldYieldSuccessIfAlreadyFailed(
            ThrowingBiConsumer<BoltStateMachine,BoltResponseHandler,BoltConnectionFatality> action ) throws Exception
    {
        // Given
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );
        machine.markFailed( error );

        // When
        action.accept( machine, responseHandler );

        // Expect
        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( ReadyState.class ) );
        verify( responseHandler, never() ).markIgnored();
        verify( responseHandler, never() ).markFailed( any() );
    }

    private static TransactionStateMachine txStateMachine( BoltStateMachine machine )
    {
        return (TransactionStateMachine) ((BoltStateMachineV1) machine).statementProcessor();
    }

    private static Neo4jError pendingError( BoltStateMachine machine )
    {
        return ((BoltStateMachineV1) machine).connectionState().getPendingError();
    }

    private static boolean pendingIgnore( BoltStateMachine machine )
    {
        return ((BoltStateMachineV1) machine).connectionState().hasPendingIgnore();
    }
}
