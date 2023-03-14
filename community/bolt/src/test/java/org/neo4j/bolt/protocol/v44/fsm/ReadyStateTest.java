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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.protocol.common.routing.RoutingTableGetter;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v43.fsm.state.FailedState;
import org.neo4j.bolt.protocol.v44.fsm.state.ReadyState;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.values.virtual.MapValue;

class ReadyStateTest {
    private State inTransactionState;
    private State streamingState;

    private Connection connection;
    private StateMachineSPI spi;
    private TransactionManager transactionManager;
    private Transaction transaction;
    private Statement statement;
    private RoutingTableGetter routingTableGetter;

    private LoginContext originalContext;
    private LoginContext impersonationContext;

    private StateMachineContext context;
    private ReadyState state;

    @BeforeEach
    void prepareState() throws TransactionException {
        this.inTransactionState = mock(State.class);
        this.streamingState = mock(State.class);

        this.spi = mock(StateMachineSPI.class);

        this.statement = mock(Statement.class);

        this.transaction = mock(Transaction.class);
        when(this.transaction.run(anyString(), any())).thenReturn(this.statement);

        this.transactionManager = mock(TransactionManager.class, RETURNS_MOCKS);
        when(this.transactionManager.create(any(), any(), anyString(), any(), anyList(), any(), anyMap(), any()))
                .thenReturn(this.transaction);

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

        var impersonatedContext = new AtomicReference<LoginContext>();
        this.connection = ConnectionMockFactory.newFactory()
                .withAnswer(
                        c -> {
                            try {
                                c.beginTransaction(any(), anyString(), any(), anyList(), any(), anyMap(), any());
                            } catch (TransactionException ignore) {
                                // Stubbing invocation - Never occurs
                            }
                        },
                        invocation -> this.transaction)
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
        this.state.setFailedState(new FailedState());
    }

    @Test
    void shouldAuthenticateImpersonationInBeginMessage() throws Exception {
        var message = new BeginMessage(
                Collections.emptyList(), null, AccessMode.WRITE, Collections.emptyMap(), "neo4j", "bob");
        var nextState = this.state.process(message, this.context);

        assertSame(this.inTransactionState, nextState);

        var inOrder = Mockito.inOrder(this.connection, this.context, this.spi, this.transactionManager);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");

        // in 4.3 implementation
        inOrder.verify(this.connection)
                .beginTransaction(
                        eq(TransactionType.EXPLICIT),
                        eq("neo4j"),
                        eq(AccessMode.WRITE),
                        eq(Collections.emptyList()),
                        Mockito.isNull(),
                        eq(Collections.emptyMap()),
                        eq(null));
        inOrder.verify(this.connection).write(StateSignal.ENTER_STREAMING);

        // impersonation is cleared when leaving transaction state
        inOrder.verify(this.connection, never()).impersonate(null);
    }

    @Test
    void shouldAuthenticateImpersonationInRouteMessage() throws Exception {
        var message = new RouteMessage(MapValue.EMPTY, Collections.emptyList(), "neo4j", "bob");
        var nextState = this.state.process(message, this.context);

        assertSame(this.state, nextState);

        var inOrder = Mockito.inOrder(this.connection, this.context, this.spi, this.routingTableGetter);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");
        inOrder.verify(this.connection)
                .beginTransaction(
                        eq(TransactionType.EXPLICIT), eq("system"), eq(AccessMode.READ), any(), any(), any(), any());

        // in 4.3 implementation
        inOrder.verify(this.routingTableGetter).get(any(), any(), eq("neo4j"));

        inOrder.verify(this.connection).impersonate(null);
    }

    @Test
    void shouldAuthenticateImpersonationInRunMessage() throws Exception {
        var message = new RunMessage(
                "RUN FANCY QUERY",
                MapValue.EMPTY,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                "neo4j",
                "bob",
                null);
        var nextState = this.state.process(message, this.context);

        assertSame(this.streamingState, nextState);

        var inOrder = Mockito.inOrder(
                this.connection, this.context, this.spi, this.transactionManager, this.transaction, this.statement);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).impersonate("bob");

        // in 4.3 implementation
        inOrder.verify(this.context).clock();
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection)
                .beginTransaction(
                        TransactionType.IMPLICIT,
                        "neo4j",
                        AccessMode.WRITE,
                        Collections.emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        inOrder.verify(this.transaction).run(eq("RUN FANCY QUERY"), any());
        inOrder.verify(this.context).clock();
        inOrder.verify(this.statement).id();
        inOrder.verify(this.statement).fieldNames();

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).write(StateSignal.ENTER_STREAMING);

        inOrder.verify(this.connection).impersonate(null);
    }
}
