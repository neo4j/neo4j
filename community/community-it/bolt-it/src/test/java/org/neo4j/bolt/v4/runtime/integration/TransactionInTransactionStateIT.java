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
package org.neo4j.bolt.v4.runtime.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.BoltResponseHandlerV1Adaptor;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.FailedState;
import org.neo4j.bolt.v3.runtime.InterruptedState;
import org.neo4j.bolt.v3.runtime.ReadyState;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.PullNMessage;
import org.neo4j.bolt.v4.runtime.InTransactionState;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.storable.BooleanValue;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.BoltMatchers.containsRecord;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;

class TransactionInTransactionStateIT extends BoltStateMachineV4StateTestBase
{
    @Test
    void shouldMoveFromInTxToReadyOnCommit_succ() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( COMMIT_MESSAGE, recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertTrue( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
    }

    @Test
    void shouldMoveFromInTxToReadyOnRollback_succ() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( ROLLBACK_MESSAGE, recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertFalse( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
    }

    @Test
    void shouldStayInTxOnDiscardAll_succ() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( DiscardAllMessage.INSTANCE, recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertFalse( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
    }

    @Test
    void shouldStayInTxOnPullN_succ() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( newPullNMessage( 100L ), recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertTrue( response.hasMetadata( "type" ) );
        assertTrue( response.hasMetadata( "t_last" ) );
        assertFalse( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
    }

    @Test
    void shouldStayInTxOnPullN_succ_hasMore() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState( "Unwind [1, 2, 3] as n return n" );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new PullNMessage( ValueUtils.asMapValue( singletonMap( "n", 2L ) ) ), recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, containsRecord( 1L ) );
        assertThat( response, succeededWithMetadata( "has_more", BooleanValue.TRUE ) );

        machine.process( new PullNMessage( ValueUtils.asMapValue( singletonMap( "n", 2L ) ) ), recorder );
        response = recorder.nextResponse();
        assertThat( response, containsRecord( 3L ) );
        assertTrue( response.hasMetadata( "type" ) );
        assertTrue( response.hasMetadata( "t_last" ) );
        assertFalse( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
    }

    @Test
    void shouldStayInTxOnAnotherRun_succ() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( "MATCH (n) RETURN n LIMIT 1" ), recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertFalse( response.hasMetadata( "bookmark" ) );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
    }

    @Test
    void shouldMoveFromInTxToFailedOnAnotherRun_fail() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( "any string" ), recorder );

        // Then
        assertThat( machine.state(), instanceOf( FailedState.class ) );
    }

    @Test
    void shouldMoveFromInTxToInterruptedOnInterrupt() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( InterruptSignal.INSTANCE, recorder );

        // Then
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );
    }

    @ParameterizedTest
    @MethodSource( "pullAllDiscardAllMessages" )
    void shouldMoveFromInTxStateToFailedStateOnfail( RequestMessage message ) throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = getBoltStateMachineInTxState();

        // When

        BoltResponseHandlerV1Adaptor handler = mock( BoltResponseHandlerV1Adaptor.class );
        doThrow( new RuntimeException( "Fail" ) ).when( handler ).onPullRecords( any(), anyLong() );
        doThrow( new RuntimeException( "Fail" ) ).when( handler ).onDiscardRecords( any() );
        machine.process( message, handler );

        // Then
        assertThat( machine.state(), instanceOf( FailedState.class ) );
    }

    @ParameterizedTest
    @MethodSource( "illegalV4Messages" )
    void shouldCloseConnectionOnIllegalV4MessagesInTxStreamingState( RequestMessage message ) throws Throwable
    {
        shouldThrowExceptionOnIllegalMessagesInTxStreamingState( message );
    }

    private void shouldThrowExceptionOnIllegalMessagesInTxStreamingState( RequestMessage message ) throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        machine.process( new BeginMessage(), nullResponseHandler() );
        machine.process( new RunMessage( "CREATE (n {k:'k'}) RETURN n.k", EMPTY_PARAMS ), nullResponseHandler() );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.process( message, recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
        assertNull( machine.state() );
    }

    private static Stream<RequestMessage> illegalV4Messages() throws BoltIOException
    {
        return Stream.of(
                newHelloMessage(),
                new BeginMessage(),
                ResetMessage.INSTANCE,
                GOODBYE_MESSAGE );
    }

    private static Stream<RequestMessage> pullAllDiscardAllMessages() throws BoltIOException
    {
        return Stream.of( newPullNMessage( 100L ), DiscardAllMessage.INSTANCE );
    }

    private BoltStateMachineV4 getBoltStateMachineInTxState() throws BoltConnectionFatality, BoltIOException
    {
        return getBoltStateMachineInTxState( "CREATE (n {k:'k'}) RETURN n.k" );
    }

    private BoltStateMachineV4 getBoltStateMachineInTxState( String query ) throws BoltConnectionFatality, BoltIOException
    {
        BoltStateMachineV4 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        machine.process( new BeginMessage(), nullResponseHandler() );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
        machine.process( new RunMessage( query, EMPTY_PARAMS ), nullResponseHandler() );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) ); // tx streaming state
        return machine;
    }

    private static PullNMessage newPullNMessage( long size ) throws BoltIOException
    {
        return new PullNMessage( ValueUtils.asMapValue( Collections.singletonMap( "n", size ) ) );
    }
}
