/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.routing.ProcedureRoutingTableGetter;
import org.neo4j.bolt.routing.RoutingTableGetter;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.v4.runtime.ReadyState;
import org.neo4j.bolt.v43.messaging.request.RouteMessage;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

/**
 * Extends the behaviour of a given State by adding the capacity of handle the {@link RouteMessage}
 *
 * When a message comes to be processed, this decorator checks if it's a {@link RouteMessage} and:
 *
 * If true, it handles the message by retrieving the routing table, setting on the connection state metadata
 * and return itself as the next state. In case of failure, it sets the failure on the context and return itself
 * as the next state.
 *
 * If it's another message type, it redirects the call to the wrapped state and follow the state transitions defined
 * by the wrapped state.
 *
 * Currently, It's only used to decorate the {@link ReadyState} and it is fully integrated tested with this state.
 *
 * @param <T> The state machine state type which will be wrapped by the decorator.
 */
public class RouteMessageHandleStateDecorator<T extends BoltStateMachineState> implements BoltStateMachineState
{
    private static final String ROUTING_TABLE_KEY = "rt";
    private final T state;
    private final RoutingTableGetter routingTableGetter;

    private RouteMessageHandleStateDecorator( T state, RoutingTableGetter routingTableGetter )
    {
        this.state = state;
        this.routingTableGetter = routingTableGetter;
    }

    public static <T extends BoltStateMachineState> RouteMessageHandleStateDecorator<T> decorate( T state )
    {
        return decorate( state, new ProcedureRoutingTableGetter() );
    }

    public static <T extends BoltStateMachineState> RouteMessageHandleStateDecorator<T> decorate( T state, RoutingTableGetter routingTableGetter )
    {
        if ( state == null )
        {
            throw new NullPointerException( "State should not be null" );
        }
        return new RouteMessageHandleStateDecorator<>( state, routingTableGetter );
    }

    public void apply( Consumer<T> consumer )
    {
        consumer.accept( state );
    }

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        if ( message instanceof RouteMessage )
        {
            handleRouteMessage( (RouteMessage) message, context );
            return this;
        }
        return redirectToWrappedState( message, context );
    }

    private void handleRouteMessage( RouteMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        try
        {
            var statementProcessor = getStatementProcessor( message, context );
            routingTableGetter.get( statementProcessor, message.getRequestContext(), message.getDatabaseName() )
                              .thenAccept( routingTable -> context.connectionState().onMetadata( ROUTING_TABLE_KEY, routingTable ) )
                              .join();
        }
        catch ( CompletionException e )
        {
            context.handleFailure( e.getCause(), true );
        }
        catch ( Throwable e )
        {
            context.handleFailure( e, true );
        }
    }

    private StatementProcessor getStatementProcessor( RouteMessage routeMessage, StateMachineContext context )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        return context.setCurrentStatementProcessorForDatabase( Optional.ofNullable( routeMessage.getDatabaseName() ).orElse( ABSENT_DB_NAME ) );
    }

    private BoltStateMachineState redirectToWrappedState( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        var resultState = state.process( message, context );
        return resultState == this.state ? this : resultState;
    }

    @Override
    public String name()
    {
        return state.name();
    }
}
