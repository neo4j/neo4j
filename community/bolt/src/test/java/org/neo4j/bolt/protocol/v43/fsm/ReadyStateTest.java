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
package org.neo4j.bolt.protocol.v43.fsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.protocol.v40.fsm.state.FailedState;
import org.neo4j.bolt.protocol.v43.fsm.state.ReadyState;
import org.neo4j.dbms.routing.RoutingResult;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class ReadyStateTest {
    private State failedState;
    private ReadyState state;
    private RoutingService routingService;

    @BeforeEach
    void prepareStateMachine() {
        this.routingService = mock(RoutingService.class);

        this.failedState = mock(FailedState.class);
        var streamingState = mock(State.class);
        var transactionReadyState = mock(State.class);

        this.state = new ReadyState();
        state.setFailedState(failedState);
        state.setStreamingState(streamingState);
        state.setTransactionReadyState(transactionReadyState);
    }

    @Test
    void shouldProcessTheRoutingMessageAndSetTheRoutingTableOnTheMetadata() throws Exception {
        var routingMessage = new RouteMessage(MapValue.EMPTY, List.of(), "databaseName", null);
        var context = mock(StateMachineContext.class, RETURNS_MOCKS);
        var connectionState = mock(MutableConnectionState.class);
        doReturn(connectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();

        // RoutingService mock
        Connection connection = mock(Connection.class);
        Connector connector = mock(Connector.class);
        doReturn(routingService).when(connector).routingService();
        doReturn(connector).when(connection).connector();
        doReturn(connection).when(context).connection();

        doReturn(routingResult()).when(routingService).route(eq(routingMessage.getDatabaseName()), any(), any());

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.state, nextState);
        verify(connectionState).onMetadata("rt", routingTableMap());
    }

    @Test
    void shouldHandleFatalFailureIfTheRoutingTableFailedToBeGot() throws Exception {
        var routingMessage = new RouteMessage(MapValue.EMPTY, List.of(), "databaseName", null);
        var context = mock(StateMachineContext.class, RETURNS_MOCKS);
        var mutableConnectionState = mock(MutableConnectionState.class);

        doReturn(mutableConnectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();

        // RoutingService mock
        Connection connection = mock(Connection.class);
        Connector connector = mock(Connector.class);
        doReturn(routingService).when(connector).routingService();
        doReturn(connector).when(connection).connector();
        doReturn(connection).when(context).connection();

        var runtimeException = new RuntimeException("Something happened");

        doThrow(runtimeException).when(routingService).route(eq(routingMessage.getDatabaseName()), any(), any());

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.failedState, nextState);
        verify(context).handleFailure(runtimeException, false);
    }

    @Test
    void shouldHandleFatalFailureIfGetRoutingTableThrowsAnException() throws Exception {
        var routingMessage = new RouteMessage(MapValue.EMPTY, List.of(), "databaseName", null);
        var context = mock(StateMachineContext.class, RETURNS_MOCKS);
        var mutableConnectionState = mock(MutableConnectionState.class);

        doReturn(mutableConnectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();

        // RoutingService mock
        Connection connection = mock(Connection.class);
        Connector connector = mock(Connector.class);
        doReturn(routingService).when(connector).routingService();
        doReturn(connector).when(connection).connector();
        doReturn(connection).when(context).connection();

        var runtimeException = new RuntimeException("Something happened");
        doThrow(runtimeException).when(routingService).route(eq(routingMessage.getDatabaseName()), any(), any());

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.failedState, nextState);
        verify(context).handleFailure(runtimeException, false);
    }

    private static MapValue routingTableMap() {
        var builder = new MapValueBuilder();
        builder.add("ttl", Values.intValue(300));
        var serversBuilder = ListValueBuilder.newListBuilder();
        builder.add("servers", serversBuilder.build());
        return builder.build();
    }

    private static RoutingResult routingResult() {
        return new RoutingResult(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), 300000);
    }
}
