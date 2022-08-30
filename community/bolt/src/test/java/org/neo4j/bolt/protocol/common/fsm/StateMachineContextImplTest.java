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
package org.neo4j.bolt.protocol.common.fsm;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;

class StateMachineContextImplTest {
    private Connection connection;
    private StateMachine machine;
    private MutableConnectionState connectionState;
    private DefaultDatabaseResolver databaseResolver;

    private StateMachineContextImpl context;

    @BeforeEach
    void prepareContext() {
        this.connection = mock(Connection.class, RETURNS_MOCKS);
        this.machine = mock(StateMachine.class);
        this.connectionState = new MutableConnectionState();
        this.databaseResolver = mock(DefaultDatabaseResolver.class);
        var stateMachineSPI = mock(StateMachineSPI.class);
        var transactionManager = mock(TransactionManager.class);

        this.context = new StateMachineContextImpl(
                this.connection,
                this.machine,
                stateMachineSPI,
                this.connectionState,
                Clock.systemUTC(),
                transactionManager);
    }

    @Test
    void shouldHandleFailure() throws BoltConnectionFatality {
        RuntimeException cause = new RuntimeException();
        context.handleFailure(cause, true);

        verify(machine).handleFailure(cause, true);
    }

    @Test
    void shouldResetMachine() throws BoltConnectionFatality {
        context.resetMachine();

        verify(machine).reset();
    }

    @Test
    void releaseShouldResetTransactionState() throws Throwable {
        assertSame(connectionState, context.connectionState());

        context.connectionState().setCurrentTransactionId("123");

        // When
        context.releaseStatementProcessor("123");

        // Then
        assertNull(context.connectionState().getCurrentTransactionId());
    }

    // FIXME: Test
    //    @Test
    //    void impersonationShouldResolveHomeDatabase() {
    //        when(this.databaseResolver.defaultDatabase("bob")).thenReturn("neo4j");
    //        when(this.databaseResolver.defaultDatabase("grace")).thenReturn("secretdb");
    //
    //        var subject = mock(AuthSubject.class);
    //        when(subject.authenticatedUser()).thenReturn("bob");
    //        when(subject.executingUser()).thenReturn("bob");
    //
    //        var loginContext = mock(LoginContext.class);
    //        when(loginContext.subject()).thenReturn(subject);
    //
    //        context.authenticatedAsUser(loginContext, "Test/0.0.0");
    //
    //        verify(this.databaseResolver).defaultDatabase("bob");
    //        verify(this.connection).updateDefaultDatabase("neo4j");
    //        verify(this.connection).updateUser("bob", "Test/0.0.0");
    //
    //        assertThat(this.context.getLoginContext()).isSameAs(loginContext);
    //        assertThat(this.context.defaultDatabase()).isEqualTo("neo4j");
    //
    //        var impersonatedSubject = mock(AuthSubject.class);
    //        when(impersonatedSubject.authenticatedUser()).thenReturn("bob");
    //        when(impersonatedSubject.executingUser()).thenReturn("grace");
    //
    //        var impersonatedLoginContext = mock(LoginContext.class);
    //        when(impersonatedLoginContext.subject()).thenReturn(impersonatedSubject);
    //
    //        context.impersonateUser(impersonatedLoginContext);
    //
    //        verify(this.databaseResolver).defaultDatabase("grace");
    //        verify(this.connection).updateDefaultDatabase("secretdb");
    //        verifyNoMoreInteractions(this.databaseResolver);
    //
    //        assertThat(this.context.getLoginContext()).isSameAs(impersonatedLoginContext);
    //        assertThat(this.context.defaultDatabase()).isEqualTo("secretdb");
    //    }
}
