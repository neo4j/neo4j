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
package org.neo4j.bolt.protocol.v44.fsm;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.routing.RoutingTableGetter;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v44.message.request.BeginMessage;
import org.neo4j.bolt.protocol.v44.message.request.RouteMessage;
import org.neo4j.bolt.protocol.v44.message.request.RunMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.values.virtual.MapValue;

class ReadyStateTest {
    private State inTransactionState;
    private State streamingState;

    private Connection connection;
    private StateMachineSPI spi;
    private TransactionManager transactionManager;
    private RoutingTableGetter routingTableGetter;

    private LoginContext originalContext;
    private LoginContext impersonationContext;

    private StateMachineContext context;
    private ReadyState state;

    @BeforeEach
    void prepareState() {
        this.inTransactionState = mock(State.class);
        this.streamingState = mock(State.class);

        this.spi = mock(StateMachineSPI.class);
        this.transactionManager = mock(TransactionManager.class, RETURNS_MOCKS);
        this.routingTableGetter = mock(RoutingTableGetter.class, RETURNS_MOCKS);

        var originalSubject = mock(AuthSubject.class);
        when(originalSubject.executingUser()).thenReturn("alice");

        var impersonationSubject = mock(AuthSubject.class);
        when(impersonationSubject.executingUser()).thenReturn("bob");

        this.originalContext = mock(LoginContext.class);
        when(this.originalContext.subject()).thenReturn(originalSubject);

        this.impersonationContext = mock(LoginContext.class);
        when(this.impersonationContext.subject()).thenReturn(impersonationSubject);

        this.context = mock(StateMachineContext.class, RETURNS_MOCKS);
        when(this.context.boltSpi()).thenReturn(this.spi);
        when(this.context.transactionManager()).thenReturn(this.transactionManager);

        var impersonatedContext = new AtomicReference<LoginContext>();
        this.connection = ConnectionMockFactory.newFactory()
                .withAnswer(Connection::loginContext, invocation -> Optional.ofNullable(impersonatedContext.get())
                        .orElseGet(() -> this.originalContext))
                .withAnswer(
                        mock -> {
                            try {
                                mock.impersonate("bob");
                            } catch (AuthenticationException ignore) {
                                // never happens
                            }
                        },
                        invocation -> {
                            impersonatedContext.set(impersonationContext);
                            return null; // void function
                        })
                .build();

        when(context.connection()).thenReturn(this.connection);

        this.state = new ReadyState(this.routingTableGetter);
        this.state.setTransactionReadyState(this.inTransactionState);
        this.state.setStreamingState(this.streamingState);
    }

    @Test
    void shouldAuthenticateImpersonationInBeginMessage() throws Exception {
        var message = new BeginMessage(
                MapValue.EMPTY,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                "neo4j",
                "bob");
        var nextState = this.state.processBeginMessage(message, this.context);

        assertSame(this.inTransactionState, nextState);

        var inOrder = Mockito.inOrder(this.connection, this.context, this.spi, this.transactionManager);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");

        // in 4.3 implementation
        inOrder.verify(this.context).transactionManager();
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).loginContext();
        inOrder.verify(this.context).connectionId();
        inOrder.verify(this.transactionManager)
                .begin(eq(this.impersonationContext), eq("neo4j"), any(), eq(false), any(), any(), any(), any());
        inOrder.verify(this.context).connectionState();

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).write(StateSignal.ENTER_STREAMING);

        // impersonation is cleared when leaving transaction state
        inOrder.verify(this.connection, never()).impersonate(null);
    }

    @Test
    void shouldAuthenticateImpersonationInRouteMessage() throws Exception {
        var message = new RouteMessage(MapValue.EMPTY, Collections.emptyList(), "neo4j", "bob");
        var nextState = this.state.processRouteMessage(message, this.context);

        assertSame(this.state, nextState);

        var inOrder = Mockito.inOrder(this.connection, this.context, this.spi, this.routingTableGetter);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");

        // in 4.3 implementation
        inOrder.verify(this.context).connectionState();
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).loginContext();
        inOrder.verify(this.context).transactionManager();
        inOrder.verify(this.context).connectionId();

        inOrder.verify(this.routingTableGetter)
                .get(any(), eq(this.impersonationContext), any(), any(), any(), eq("neo4j"), any());

        inOrder.verify(this.connection).impersonate(null);
    }

    @Test
    void shouldAuthenticateImpersonationInRunMessage() throws Exception {
        var message = new RunMessage(
                "RUN FANCY QUERY",
                MapValue.EMPTY,
                MapValue.EMPTY,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                "neo4j",
                "bob");
        var nextState = this.state.processRunMessage(message, this.context);

        assertSame(this.streamingState, nextState);

        var inOrder = Mockito.inOrder(this.connection, this.context, this.spi, this.transactionManager);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");

        // in 4.3 implementation
        inOrder.verify(this.context).clock();
        inOrder.verify(this.context).connectionState();
        inOrder.verify(this.context).transactionManager();
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).loginContext();
        inOrder.verify(this.context).connectionId();
        inOrder.verify(this.transactionManager)
                .runProgram(
                        any(),
                        eq(this.impersonationContext),
                        eq("neo4j"),
                        eq("RUN FANCY QUERY"),
                        any(),
                        any(),
                        eq(false),
                        any(),
                        any(),
                        any());
        inOrder.verify(this.context).clock();
        inOrder.verify(this.context, times(2)).connectionState();

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).write(StateSignal.ENTER_STREAMING);

        inOrder.verify(this.connection).impersonate(null);
    }
}
