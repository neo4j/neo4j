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
package org.neo4j.bolt.runtime.statemachine.impl;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.TransactionStateMachine.StatementOutcome;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v3.runtime.ConnectedState;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.bolt.v4.runtime.FailedState;
import org.neo4j.bolt.v4.runtime.ReadyState;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.init;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.newMachine;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.newMachineWithTransaction;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.newMachineWithTransactionSPI;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.reset;
import static org.neo4j.bolt.testing.BoltMatchers.canReset;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.hasNoTransaction;
import static org.neo4j.bolt.testing.BoltMatchers.inState;
import static org.neo4j.bolt.testing.BoltMatchers.isClosed;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.verifyOneResponse;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;

class BoltStateMachineV4Test
{
    @Test
    void allStateTransitionsShouldSendExactlyOneResponseToTheClient() throws Exception
    {
        List<RequestMessage> messages = BoltV4Messages.supported().collect( Collectors.toList() );

        for ( RequestMessage message: messages )
        {
            verifyOneResponse( ( machine, recorder ) -> machine.process( message, recorder ) );
        }
    }

    @Test
    void initialStateShouldBeConnected()
    {
        assertThat( newMachine(), inState( ConnectedState.class ) );
    }

    @Test
    void shouldRollbackOpenTransactionOnReset() throws Throwable
    {
        // Given a FAILED machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction();
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When RESET occurs
        reset( machine, nullResponseHandler() );

        // Then the transaction should have been rolled back...
        assertThat( machine, hasNoTransaction() );

        // ...and the machine should go back to READY
        assertThat( machine, inState( ReadyState.class ) );
    }

    @Test
    void shouldRollbackOpenTransactionOnClose() throws Throwable
    {
        // Given a ready machine with an open transaction
        final BoltStateMachine machine = newMachineWithTransaction();

        // When the machine is shut down
        machine.close();

        // Then the transaction should have been rolled back
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    void shouldBeAbleToResetWhenInReadyState() throws Throwable
    {
        BoltStateMachine machine = init( newMachine() );
        assertThat( machine, canReset() );
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    void shouldResetWithOpenTransaction() throws Throwable
    {
        BoltStateMachine machine = newMachineWithTransaction();
        assertThat( machine, canReset() );
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    void shouldResetWithOpenTransactionAndOpenResult() throws Throwable
    {
        // Given a ready machine with an open transaction...
        final BoltStateMachine machine = newMachineWithTransaction();

        // ...and an open result
        machine.process( BoltV4Messages.run(), nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    void shouldResetWithOpenResult() throws Throwable
    {
        // Given a ready machine...
        final BoltStateMachine machine = init( newMachine() );

        // ...and an open result
        machine.process( BoltV4Messages.run(), nullResponseHandler() );

        // Then
        assertThat( machine, canReset() );
        assertThat( machine, hasNoTransaction() );
    }

    @Test
    void shouldFailWhenOutOfOrderRollback() throws Throwable
    {
        // Given a failed machine
        final BoltStateMachine machine = newMachine();
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When
        machine.process( BoltV4Messages.rollback(), nullResponseHandler() );

        // Then
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    void shouldRemainStoppedAfterInterrupted() throws Throwable
    {
        // Given a ready machine
        final BoltStateMachine machine = init( newMachine() );

        // ...which is subsequently closed
        machine.close();
        assertThat( machine, isClosed() );

        // When and interrupt and reset occurs
        reset( machine, nullResponseHandler() );

        // Then the machine should remain closed
        assertThat( machine, isClosed() );
    }

    @Test
    void shouldBeAbleToKillMessagesAheadInLineWithAnInterrupt() throws Throwable
    {
        // Given
        final BoltStateMachine machine = init( newMachine() );

        // When
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( BoltV4Messages.run(), recorder );
        machine.process( BoltV4Messages.reset(), recorder );
        machine.process( BoltV4Messages.run(), recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void multipleInterruptsShouldBeMatchedWithMultipleResets() throws Throwable
    {
        // Given
        final BoltStateMachine machine = init( newMachine() );

        // When
        machine.interrupt();
        machine.interrupt();

        // ...and
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( BoltV4Messages.run(), recorder );
        machine.process( BoltV4Messages.reset(), recorder );
        machine.process( BoltV4Messages.run(), recorder );

        // Then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( recorder.nextResponse(), wasIgnored() );

        // But when
        recorder.reset();
        machine.process( BoltV4Messages.reset(), recorder );
        machine.process( BoltV4Messages.run(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void testPublishingError() throws Throwable
    {
        // Given a new ready machine...
        BoltStateMachine machine = init( newMachine() );

        // ...and a result ready to be retrieved...
        machine.process( BoltV4Messages.run(), nullResponseHandler() );

        // ...and a handler guaranteed to break
        BoltResponseRecorder recorder = new BoltResponseRecorder()
        {
            @Override
            public boolean onPullRecords( BoltResult result, long size )
            {
                throw new RuntimeException( "I've been expecting you, Mr Bond." );
            }
        };

        // When we pull using that handler
        machine.process( BoltV4Messages.pullAll(), recorder );

        // Then the breakage should surface as a FAILURE
        assertThat( recorder.nextResponse(), failedWithStatus( Status.General.UnknownError ) );

        // ...and the machine should have entered a FAILED state
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    void testRollbackError() throws Throwable
    {
        // Given
        BoltStateMachine machine = init( newMachine() );

        // Given there is a running transaction
        machine.process( BoltV4Messages.begin(), nullResponseHandler() );

        // And given that transaction will fail to roll back
        TransactionStateMachine txMachine = txStateMachine( machine );
        doThrow( new TransactionFailureException( "No Mr. Bond, I expect you to die." ) ).
                when( txMachine.ctx.currentTransaction ).rollback();

        // When
        machine.process(BoltV4Messages.rollback(), nullResponseHandler() );

        // Then
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    void testFailOnNestedTransactions() throws Throwable
    {
        // Given
        BoltStateMachine machine = init( newMachine() );

        // Given there is a running transaction
        machine.process( BoltV4Messages.begin(), nullResponseHandler() );

        // When
        assertThrows( BoltProtocolBreachFatality.class, () -> machine.process( BoltV4Messages.begin(), nullResponseHandler() ) );

        // Then
        assertThat( machine, inState( null ) );
    }

    @Test
    void testCantDoAnythingIfInFailedState() throws Throwable
    {
        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // Then no RUN...
        machine.process( BoltV4Messages.run(), nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
        // ...DISCARD_ALL...
        machine.process( BoltV4Messages.discardAll(), nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
        // ...or PULL_ALL should be possible
        machine.process( BoltV4Messages.pullAll(), nullResponseHandler() );
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    void testUsingResetToAcknowledgeError() throws Throwable
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );
        machine.markFailed( Neo4jError.from( new RuntimeException() ) );

        // When I RESET...
        reset( machine, recorder );

        // ...successfully
        assertThat( recorder.nextResponse(), succeeded() );

        // Then if I RUN a statement...
        machine.process( BoltV4Messages.run(), recorder );

        // ...everything should be fine again
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void actionsDisallowedBeforeInitialized()
    {
        // Given
        BoltStateMachine machine = newMachine();

        // When
        try
        {
            machine.process( BoltV4Messages.run(), nullResponseHandler() );
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
    void shouldTerminateOnAuthExpiryDuringREADYRun() throws Throwable
    {
        // Given
        TransactionStateMachineSPI transactionSPI = mock( TransactionStateMachineSPI.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( transactionSPI ).beginTransaction( any(), any(), any(), any(), any() );

        BoltStateMachine machine = newMachineWithTransactionSPI( transactionSPI );

        // When & Then
        try
        {
            machine.process( BoltV4Messages.run( "THIS WILL BE IGNORED" ), nullResponseHandler() );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }
    }

    @Test
    void shouldTerminateOnAuthExpiryDuringSTREAMINGPullAll() throws Throwable
    {
        // Given
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( responseHandler )
                .onPullRecords( any(), eq( STREAM_LIMIT_UNLIMITED ) );
        BoltStateMachine machine = init( newMachine() );
        machine.process( BoltV4Messages.run(), nullResponseHandler() ); // move to streaming state
        // We assume the only implementation of statement processor is TransactionStateMachine
        txStateMachine( machine ).ctx.statementOutcomes.put( StatementMetadata.ABSENT_QUERY_ID, new StatementOutcome( BoltResult.EMPTY ) );

        // When & Then
        try
        {
            machine.process( BoltV4Messages.pullAll(), responseHandler );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }

        verify( responseHandler ).onPullRecords( any(), eq( STREAM_LIMIT_UNLIMITED ) );
    }

    @Test
    void shouldTerminateOnAuthExpiryDuringSTREAMINGDiscardAll() throws Throwable
    {
        // Given
        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        doThrow( new AuthorizationExpiredException( "Auth expired!" ) ).when( responseHandler )
                .onDiscardRecords( any(), eq( STREAM_LIMIT_UNLIMITED ) );
        BoltStateMachine machine = init( newMachine() );
        machine.process( BoltV4Messages.run(), nullResponseHandler() ); // move to streaming state
        // We assume the only implementation of statement processor is TransactionStateMachine
        txStateMachine( machine ).ctx.statementOutcomes.put( StatementMetadata.ABSENT_QUERY_ID, new StatementOutcome( BoltResult.EMPTY ) );

        // When & Then
        try
        {
            machine.process( BoltV4Messages.discardAll(), responseHandler );
            fail( "Exception expected" );
        }
        catch ( BoltConnectionAuthFatality e )
        {
            assertEquals( "Auth expired!", e.getCause().getMessage() );
        }
    }

    @Test
    void shouldCloseBoltChannelWhenClosed()
    {
        BoltStateMachineSPIImpl spi = mock( BoltStateMachineSPIImpl.class );
        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachineV4( spi, boltChannel, Clock.systemUTC() );

        machine.close();

        verify( boltChannel ).close();
    }

    @Test
    void shouldSetPendingErrorOnMarkFailedIfNoHandler()
    {
        BoltStateMachineSPIImpl spi = mock( BoltStateMachineSPIImpl.class );
        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachineV4( spi, boltChannel, Clock.systemUTC() );
        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );

        machine.markFailed( error );

        assertEquals( error, pendingError( machine ) );
        assertThat( machine, inState( FailedState.class ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( BoltV4Messages.hello(), handler ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.process( BoltV4Messages.run(), handler ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> {
            try
            {
                machine.process( BoltV4Messages.pullAll(), handler );
            }
            catch ( BoltIOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> {
            try
            {
                machine.process( BoltV4Messages.discardAll(), handler );
            }
            catch ( BoltIOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testReadyStateAfterMarkFailedOnNextMessage( BoltV4MachineRoom::reset );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfNoHandler() throws Exception
    {
        testMarkFailedOnNextMessage( ( machine, handler ) -> machine.handleExternalFailure( Neo4jError.from( Status.Request.Invalid, "invalid" ), handler ) );
    }

    @Test
    void shouldSetPendingIgnoreOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
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
    void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> machine.process( BoltV4Messages.hello(), handler ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
                ( machine, handler ) -> machine.process( BoltV4Messages.run(), handler ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> {
            try
            {
                machine.process( BoltV4Messages.pullAll(), handler );
            }
            catch ( BoltIOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed( ( machine, handler ) -> {
            try
            {
                machine.process( BoltV4Messages.discardAll(), handler );
            }
            catch ( BoltIOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldSuccessIfAlreadyFailed( ( machine, handler ) -> {
            reset( machine, handler );
        } );
    }

    @Test
    void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    {
        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
                ( machine, handler ) -> machine.handleExternalFailure( Neo4jError.from( Status.Request.Invalid, "invalid" ), handler ) );
    }

    @Test
    void shouldInvokeResponseHandlerOnMarkFailedIfThereIsHandler() throws Exception
    {
        BoltStateMachine machine = init( newMachine() );
        Neo4jError error = Neo4jError.from( Status.Request.NoThreadsAvailable, "no threads" );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        ((AbstractBoltStateMachine) machine).connectionState().setResponseHandler( responseHandler );
        machine.markFailed( error );

        assertNull( pendingError( machine ) );
        assertFalse( pendingIgnore( machine ) );
        assertThat( machine, inState( FailedState.class ) );
        verify( responseHandler ).markFailed( error );
    }

    @Test
    void shouldNotFailWhenMarkedForTerminationAndPullAll() throws Exception
    {
        BoltStateMachineSPIImpl spi = mock( BoltStateMachineSPIImpl.class, RETURNS_MOCKS );
        BoltStateMachine machine = init( newMachine( spi ) );
        machine.process( BoltV4Messages.run(), nullResponseHandler() ); // move to streaming state
        txStateMachine( machine ).ctx.statementOutcomes.put( StatementMetadata.ABSENT_QUERY_ID, new StatementOutcome( BoltResult.EMPTY ) );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );

        machine.markForTermination();
        machine.process( BoltV4Messages.pullAll(), responseHandler );

        verify( spi, never() ).reportError( any() );
        assertThat( machine, not( inState( FailedState.class ) ) );
    }

    @Test
    void shouldSucceedOnResetOnFailedState() throws Exception
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );

        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( BoltV4Messages.pullAll(), recorder );

        // When I RESET...
        machine.interrupt();
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( BoltV4Messages.reset(), recorder );

        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.NoThreadsAvailable ) );
        // ...successfully
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void shouldSucceedOnConsecutiveResetsOnFailedState() throws Exception
    {
        // Given
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // Given a FAILED machine
        BoltStateMachine machine = init( newMachine() );

        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( BoltV4Messages.pullAll(), recorder );

        // When I RESET...
        machine.interrupt();
        machine.interrupt();
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( BoltV4Messages.reset(), recorder );
        machine.markFailed( Neo4jError.from( Status.Request.NoThreadsAvailable, "No Threads Available" ) );
        machine.process( BoltV4Messages.reset(), recorder );

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
        return (TransactionStateMachine) ((AbstractBoltStateMachine) machine).statementProcessor();
    }

    private static Neo4jError pendingError( BoltStateMachine machine )
    {
        return ((AbstractBoltStateMachine) machine).connectionState().getPendingError();
    }

    private static boolean pendingIgnore( BoltStateMachine machine )
    {
        return ((AbstractBoltStateMachine) machine).connectionState().hasPendingIgnore();
    }
}
