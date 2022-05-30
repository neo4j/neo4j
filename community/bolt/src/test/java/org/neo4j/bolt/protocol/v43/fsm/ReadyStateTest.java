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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.routing.RoutingTableGetter;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v43.message.request.RouteMessage;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class ReadyStateTest {
    private RoutingTableGetter routingTableGetter;

    private State failedState;
    private ReadyState state;

    @BeforeEach
    private void prepareStateMachine() {
        this.routingTableGetter = mock(RoutingTableGetter.class);

        this.failedState = mock(FailedState.class);
        var streamingState = mock(State.class);
        var interruptedState = mock(State.class);
        var transactionReadyState = mock(State.class);

        this.state = new ReadyState(this.routingTableGetter);
        state.setFailedState(failedState);
        state.setStreamingState(streamingState);
        state.setInterruptedState(interruptedState);
        state.setTransactionReadyState(transactionReadyState);
    }

    @Test
    void shouldProcessTheRoutingMessageAndSetTheRoutingTableOnTheMetadata() throws Exception {
        var routingMessage = new RouteMessage(new MapValueBuilder().build(), List.of(), "databaseName");
        var context = mock(StateMachineContext.class);
        var connectionState = mockMutableConnectionState(context);
        var transactionManager = mockTransactionManager(context);
        var routingTable = mockRoutingTable(routingMessage, this.routingTableGetter, transactionManager);

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.state, nextState);
        verify(connectionState).onMetadata("rt", routingTable);
    }

    @Test
    void shouldHandleFatalFailureIfTheRoutingTableFailedToBeGot() throws Exception {
        var routingMessage = new RouteMessage(new MapValueBuilder().build(), List.of(), "databaseName");
        var context = mock(StateMachineContext.class);
        var mutableConnectionState = mock(MutableConnectionState.class);

        doReturn(mutableConnectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();

        var transactionManager = mockTransactionManager(context);
        var runtimeException = mockCompletedRuntimeException(routingMessage, routingTableGetter, transactionManager);

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.failedState, nextState);
        verify(context).handleFailure(runtimeException, false);
    }

    @Test
    void shouldHandleFatalFailureIfGetRoutingTableThrowsAnException() throws Exception {
        var routingMessage = new RouteMessage(new MapValueBuilder().build(), List.of(), "databaseName");
        var context = mock(StateMachineContext.class);
        var mutableConnectionState = mock(MutableConnectionState.class);

        doReturn(mutableConnectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();

        var transactionManager = mockTransactionManager(context);
        var runtimeException = mockRuntimeException(routingMessage, this.routingTableGetter, transactionManager);

        var nextState = this.state.process(routingMessage, context);

        assertEquals(this.failedState, nextState);
        verify(context).handleFailure(runtimeException, false);
    }

    private RuntimeException mockRuntimeException(
            RouteMessage routingMessage, RoutingTableGetter routingTableGetter, TransactionManager transactionManager) {
        var runtimeException = new RuntimeException("Something happened");
        doThrow(runtimeException)
                .when(routingTableGetter)
                .get(
                        anyString(),
                        any(),
                        eq(transactionManager),
                        eq(routingMessage.getRequestContext()),
                        eq(routingMessage.getBookmarks()),
                        eq(routingMessage.getDatabaseName()),
                        eq("123"));
        return runtimeException;
    }

    private RuntimeException mockCompletedRuntimeException(
            RouteMessage routingMessage, RoutingTableGetter routingTableGetter, TransactionManager transactionManager) {
        var runtimeException = new RuntimeException("Something happened");
        doReturn(CompletableFuture.failedFuture(runtimeException))
                .when(routingTableGetter)
                .get(
                        anyString(),
                        any(),
                        eq(transactionManager),
                        eq(routingMessage.getRequestContext()),
                        eq(routingMessage.getBookmarks()),
                        eq(routingMessage.getDatabaseName()),
                        eq("123"));
        return runtimeException;
    }

    private static MutableConnectionState mockMutableConnectionState(StateMachineContext context) {
        var connectionState = mock(MutableConnectionState.class);
        doReturn(connectionState).when(context).connectionState();
        doReturn("123").when(context).connectionId();
        return connectionState;
    }

    private MapValue mockRoutingTable(
            RouteMessage routingMessage, RoutingTableGetter routingTableGetter, TransactionManager transactionManager) {
        var routingTable = routingTable();
        doReturn(CompletableFuture.completedFuture(routingTable))
                .when(routingTableGetter)
                .get(
                        anyString(),
                        any(),
                        eq(transactionManager),
                        eq(routingMessage.getRequestContext()),
                        eq(routingMessage.getBookmarks()),
                        eq(routingMessage.getDatabaseName()),
                        eq("123"));
        return routingTable;
    }

    private TransactionManager mockTransactionManager(StateMachineContext context) {
        var transactionManager = mock(TransactionManager.class);
        doReturn(transactionManager).when(context).transactionManager();
        return transactionManager;
    }

    private static MapValue routingTable() {
        var builder = new MapValueBuilder();
        builder.add("TTL", Values.intValue(300));
        var serversBuilder = ListValueBuilder.newListBuilder();
        builder.add("servers", serversBuilder.build());
        return builder.build();
    }
}
