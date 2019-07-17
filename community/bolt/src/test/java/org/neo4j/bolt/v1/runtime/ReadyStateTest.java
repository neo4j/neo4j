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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.bolt.v1.runtime.spi.BookmarkResult;
import org.neo4j.graphdb.security.AuthorizationExpiredException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class ReadyStateTest
{
    private final ReadyState state = new ReadyState();

    private final BoltStateMachineState streamingState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState interruptedState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState failedState = mock( BoltStateMachineState.class );
    private final StatementProcessor statementProcessor = mock( StatementProcessor.class );

    private final StateMachineContext context = mock( StateMachineContext.class );
    private final MutableConnectionState connectionState = new MutableConnectionState();

    @BeforeEach
    void setUp() throws Exception
    {
        state.setStreamingState( streamingState );
        state.setInterruptedState( interruptedState );
        state.setFailedState( failedState );

        when( context.connectionState() ).thenReturn( connectionState );
        when( context.clock() ).thenReturn( Clock.systemUTC() );
        when( context.setCurrentStatementProcessorForDatabase( any() ) ).thenReturn( statementProcessor );
    }

    @Test
    void shouldThrowWhenNotInitialized() throws Exception
    {
        ReadyState state = new ReadyState();

        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setStreamingState( streamingState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setStreamingState( null );
        state.setInterruptedState( interruptedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setInterruptedState( null );
        state.setFailedState( failedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );
    }

    @Test
    void shouldProcessRunMessage() throws Exception
    {
        StatementMetadata statementMetadata = mock( StatementMetadata.class );
        when( statementMetadata.fieldNames() ).thenReturn( new String[]{"foo", "bar", "baz"} );
        when( statementProcessor.run( any(), any() ) ).thenReturn( statementMetadata );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        connectionState.setResponseHandler( responseHandler );

        BoltStateMachineState nextState = state.process( new RunMessage( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( streamingState, nextState );
        verify( statementProcessor ).run( "RETURN 1", EMPTY_MAP );
        verify( responseHandler ).onMetadata( "fields", stringArray( "foo", "bar", "baz" ) );
        verify( responseHandler ).onMetadata( eq( "result_available_after" ), any() );
    }

    @Test
    void shouldHandleAuthFailureDuringRunMessageProcessing() throws Exception
    {
        AuthorizationExpiredException error = new AuthorizationExpiredException( "Hello" );
        when( statementProcessor.run( any(), any() ) ).thenThrow( error );

        BoltStateMachineState nextState = state.process( new RunMessage( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, true );
    }

    @Test
    void shouldHandleFailureDuringRunMessageProcessing() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        when( statementProcessor.run( any(), any() ) ).thenThrow( error );

        BoltStateMachineState nextState = state.process( new RunMessage( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, false );
    }

    @Test
    void shouldProcessResetMessage() throws Exception
    {
        when( context.resetMachine() ).thenReturn( true ); // reset successful

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( state, newState );
    }

    @Test
    void shouldHandleFailureDuringResetMessageProcessing() throws Exception
    {
        when( context.resetMachine() ).thenReturn( false ); // reset failed

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( failedState, newState );
    }

    @Test
    void shouldProcessInterruptMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( InterruptSignal.INSTANCE, context );

        assertEquals( interruptedState, newState );
    }

    @Test
    void shouldNotProcessUnsupportedMessages() throws Exception
    {
        List<RequestMessage> unsupportedMessages = asList( PullAllMessage.INSTANCE, DiscardAllMessage.INSTANCE, AckFailureMessage.INSTANCE );

        for ( RequestMessage message : unsupportedMessages )
        {
            assertNull( state.process( message, context ) );
        }
    }

    @Test
    void shouldBeginTransactionWithoutBookmark() throws Exception
    {
        BoltStateMachineState newState = state.process( new RunMessage( "BEGIN", EMPTY_MAP ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).beginTransaction( List.of() );
    }

    @Test
    void shouldBeginTransactionWithSingleBookmark() throws Exception
    {
        Map<String,Object> params = map( "bookmark", "neo4j:bookmark:v1:tx15" );

        BoltStateMachineState newState = state.process( new RunMessage( "BEGIN", asMapValue( params ) ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).beginTransaction( List.of( new BookmarkWithPrefix( 15 ) ) );
    }

    @Test
    void shouldBeginTransactionWithMultipleBookmarks() throws Exception
    {
        Map<String,Object> params =
                map( "bookmarks", asList( "neo4j:bookmark:v1:tx7", "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:tx92", "neo4j:bookmark:v1:tx39" ) );

        BoltStateMachineState newState = state.process( new RunMessage( "BEGIN", asMapValue( params ) ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).beginTransaction( List.of( new BookmarkWithPrefix( 92 ) ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"begin", "BEGIN", "   begin   ", "   BeGiN ;   "} )
    void shouldBeginTransaction( String statement ) throws Exception
    {
        BoltStateMachineState newState = state.process( new RunMessage( statement ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).beginTransaction( any() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"commit", "COMMIT", "   commit   ", "   CoMmIt ;   "} )
    void shouldCommitTransaction( String statement ) throws Throwable
    {
        BoltStateMachineState newState = state.process( new RunMessage( statement ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).commitTransaction();

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        context.connectionState().setResponseHandler( responseHandler );
        newState = newState.process( PullAllMessage.INSTANCE, context );
        assertEquals( state, newState );
        verify( responseHandler ).onPullRecords( any( BookmarkResult.class ), anyLong() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"rollback", "ROLLBACK", "   rollback   ", "   RoLlBaCk ;   "} )
    void shouldRollbackTransaction( String statement ) throws Exception
    {
        BoltStateMachineState newState = state.process( new RunMessage( statement ), context );
        assertEquals( state, newState );
        verify( statementProcessor ).rollbackTransaction();
    }

    @ParameterizedTest
    @MethodSource( "testPullAllDiscardAllParameters" )
    void shouldHandlePullAllDiscardAll( String runStatement, RequestMessage message, Class<BoltResult> resultClass ) throws Throwable
    {
        BoltStateMachineState newState = state.process( new RunMessage( runStatement ), context );
        assertEquals( state, newState );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        context.connectionState().setResponseHandler( responseHandler );

        newState = newState.process( message, context );
        assertEquals( state, newState );
        if ( message instanceof PullAllMessage )
        {
            verify( responseHandler ).onPullRecords( any( resultClass ), anyLong() );
        }
        else
        {
            verify( responseHandler ).onDiscardRecords( any( resultClass ), anyLong() );
        }
    }

    private static Stream<Arguments> testPullAllDiscardAllParameters()
    {
        return Stream.of( Arguments.of( "BEGIN", PullAllMessage.INSTANCE, BoltResult.EMPTY.getClass() ),
                Arguments.of( "BEGIN", DiscardAllMessage.INSTANCE, BoltResult.EMPTY.getClass() ),
                Arguments.of( "COMMIT", PullAllMessage.INSTANCE, BookmarkResult.class ),
                Arguments.of( "COMMIT", DiscardAllMessage.INSTANCE, BookmarkResult.class ),
                Arguments.of( "ROLLBACK", PullAllMessage.INSTANCE, BoltResult.EMPTY.getClass() ),
                Arguments.of( "ROLLBACK", DiscardAllMessage.INSTANCE, BoltResult.EMPTY.getClass() ) );
    }
}
