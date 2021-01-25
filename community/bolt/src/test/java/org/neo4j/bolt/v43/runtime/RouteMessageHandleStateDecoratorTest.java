/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.v43.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.routing.RoutingTableGetter;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.v43.messaging.request.RouteMessage;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RouteMessageHandleStateDecoratorTest
{
    private static final String MOCKED_STATE_NAME = "MOCKED_STATE";

    private static Stream<Arguments> shouldReturnTheNextStateWhenItReceivesANonRouteMessage()
    {
        return Stream.of( arguments( mock( BoltStateMachineState.class ) ) );
    }

    private static Stream<Arguments> shouldThrowTheOriginalExceptionWhenItReceivesANonRouteMessage()
    {
        return Stream.of( arguments( new NullPointerException() ),
                          arguments( new RuntimeException() ),
                          arguments( new BoltConnectionFatality( "Something went wrong", new RuntimeException() ) ) );
    }

    @Test
    void showThrowANullPointerExceptionWhenItsDecoratingAnNullPointer()
    {
        assertThrows( NullPointerException.class, () -> RouteMessageHandleStateDecorator.decorate( null ) );
    }

    @Test
    void shouldApplyMethodCallThroughTheOriginalState()
    {
        var state = mockStateWithName();
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );

        decoratedState.apply( it -> assertEquals( MOCKED_STATE_NAME, it.name() ) );

        verify( state, times( 1 ) ).name();
    }

    @Test
    void shouldNotChangeTheOriginalStateName()
    {
        var state = mockStateWithName();
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );

        assertEquals( state.name(), decoratedState.name() );
    }

    @Test
    void shouldRedirectTheProcessToTheOriginalStateWhenItReceivesANonRouteMessage() throws Exception
    {
        var state = mock( BoltStateMachineState.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );
        var requestMessage = mock( RequestMessage.class );
        var context = mock( StateMachineContext.class );

        decoratedState.process( requestMessage, context );

        verify( state ).process( requestMessage, context );
    }

    @ParameterizedTest
    @NullSource
    @MethodSource
    void shouldReturnTheNextStateWhenItReceivesANonRouteMessage( BoltStateMachineState nextState ) throws Exception
    {
        var state = mock( BoltStateMachineState.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );
        var requestMessage = mock( RequestMessage.class );
        var context = mock( StateMachineContext.class );
        doReturn( nextState ).when( state ).process( requestMessage, context );

        assertEquals( nextState, decoratedState.process( requestMessage, context ) );
    }

    @Test
    void shouldReturnThisWhenTheNextStateIsTheWrappedState() throws Exception
    {
        var state = mock( BoltStateMachineState.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );
        var requestMessage = mock( RequestMessage.class );
        var context = mock( StateMachineContext.class );
        doReturn( state ).when( state ).process( requestMessage, context );

        assertEquals( decoratedState, decoratedState.process( requestMessage, context ) );
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowTheOriginalExceptionWhenItReceivesANonRouteMessage( Exception exception ) throws Exception
    {
        var state = mock( BoltStateMachineState.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state );
        var requestMessage = mock( RequestMessage.class );
        var context = mock( StateMachineContext.class );
        doThrow( exception ).when( state ).process( requestMessage, context );

        var actualException = assertThrows( exception.getClass(), () -> decoratedState.process( requestMessage, context ) );

        assertEquals( exception, actualException );
    }

    @Test
    void shouldProcessTheRoutingMessageAndSetTheRoutingTableOnTheMetadata() throws Exception
    {
        var routingMessage = new RouteMessage( new MapValueBuilder().build(), "databaseName" );
        var state = mock( BoltStateMachineState.class );
        var routingTableGetter = mock( RoutingTableGetter.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state, routingTableGetter );
        var context = mock( StateMachineContext.class );
        var connectionState = mockMutableConnectionState( context );
        var statementProcessor = mockStatementProcessor( routingMessage, context );
        var routingTable = mockRoutingTable( routingMessage, routingTableGetter, statementProcessor );

        var nextState = decoratedState.process( routingMessage, context );

        assertEquals( decoratedState, nextState );
        verify( connectionState ).onMetadata( "rt", routingTable );
    }

    @Test
    void shouldHandleFatalFailureIfTheRoutingTableFailedToBeGot() throws Exception
    {
        var routingMessage = new RouteMessage( new MapValueBuilder().build(), "databaseName" );
        var state = mock( BoltStateMachineState.class );
        var routingTableGetter = mock( RoutingTableGetter.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state, routingTableGetter );
        var context = mock( StateMachineContext.class );
        var statementProcessor = mockStatementProcessor( routingMessage, context );
        var runtimeException = mockCompletedRuntimeException( routingMessage, routingTableGetter, statementProcessor );

        var nextState = decoratedState.process( routingMessage, context );

        assertEquals( decoratedState, nextState );
        verify( context ).handleFailure( runtimeException, true );
    }

    @Test
    void shouldHandleFatalFailureIfGetRoutingTableThrowsAnException() throws Exception
    {
        var routingMessage = new RouteMessage( new MapValueBuilder().build(), "databaseName" );
        var state = mock( BoltStateMachineState.class );
        var routingTableGetter = mock( RoutingTableGetter.class );
        var decoratedState = RouteMessageHandleStateDecorator.decorate( state, routingTableGetter );
        var context = mock( StateMachineContext.class );
        var statementProcessor = mockStatementProcessor( routingMessage, context );
        var runtimeException = mockRuntimeException( routingMessage, routingTableGetter, statementProcessor );

        var nextState = decoratedState.process( routingMessage, context );

        assertEquals( decoratedState, nextState );
        verify( context ).handleFailure( runtimeException, true );
    }

    private RuntimeException mockRuntimeException( RouteMessage routingMessage, RoutingTableGetter routingTableGetter, StatementProcessor statementProcessor )
    {
        var runtimeException = new RuntimeException( "Something happened" );
        doThrow( runtimeException )
                .when( routingTableGetter )
                .get( statementProcessor, routingMessage.getRequestContext(), routingMessage.getDatabaseName() );
        return runtimeException;
    }

    private RuntimeException mockCompletedRuntimeException( RouteMessage routingMessage, RoutingTableGetter routingTableGetter,
                                                            StatementProcessor statementProcessor )
    {
        var runtimeException = new RuntimeException( "Something happened" );
        doReturn( CompletableFuture.failedFuture( runtimeException ) )
                .when( routingTableGetter )
                .get( statementProcessor, routingMessage.getRequestContext(), routingMessage.getDatabaseName() );
        return runtimeException;
    }

    private MutableConnectionState mockMutableConnectionState( StateMachineContext context )
    {
        var connectionState = mock( MutableConnectionState.class );
        doReturn( connectionState ).when( context ).connectionState();
        return connectionState;
    }

    private MapValue mockRoutingTable( RouteMessage routingMessage, RoutingTableGetter routingTableGetter, StatementProcessor statementProcessor )
    {
        var routingTable = routingTable();
        doReturn( CompletableFuture.completedFuture( routingTable ) )
                .when( routingTableGetter )
                .get( statementProcessor, routingMessage.getRequestContext(), routingMessage.getDatabaseName() );
        return routingTable;
    }

    private StatementProcessor mockStatementProcessor( RouteMessage routingMessage, StateMachineContext context )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        var statementProcessor = mock( StatementProcessor.class );
        doReturn( statementProcessor )
                .when( context )
                .setCurrentStatementProcessorForDatabase( routingMessage.getDatabaseName() );
        return statementProcessor;
    }

    private BoltStateMachineState mockStateWithName()
    {
        var state = mock( BoltStateMachineState.class );
        doReturn( MOCKED_STATE_NAME ).when( state ).name();
        return state;
    }

    private MapValue routingTable()
    {
        var builder = new MapValueBuilder();
        builder.add( "TTL", Values.intValue( 300 ) );
        var serversBuilder = ListValueBuilder.newListBuilder();
        builder.add( "servers", serversBuilder.build() );
        return builder.build();
    }
}
