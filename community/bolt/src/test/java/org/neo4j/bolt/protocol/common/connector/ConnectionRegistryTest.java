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
package org.neo4j.bolt.protocol.common.connector;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;

class ConnectionRegistryTest {

    private static final String CONNECTOR_ID = "bolt-test";
    private static final String CONNECTION_ID = "connection-0";

    private NetworkConnectionTracker connectionTracker;
    private AssertableLogProvider logProvider;

    private ConnectionRegistry connectionRegistry;

    @BeforeEach
    void prepareRegistry() {
        this.connectionTracker = Mockito.mock(NetworkConnectionTracker.class);
        this.logProvider = new AssertableLogProvider();

        this.connectionRegistry = new ConnectionRegistry(CONNECTOR_ID, this.connectionTracker, this.logProvider);
    }

    @Test
    void allocateIdShouldDelegateToConnectionTracker() {
        Mockito.doReturn("some-id").when(this.connectionTracker).newConnectionId(CONNECTOR_ID);

        var connectionId = this.connectionRegistry.allocateId();

        Assertions.assertThat(connectionId).isNotNull().isNotBlank().isEqualTo("some-id");

        Mockito.verify(this.connectionTracker).newConnectionId(CONNECTOR_ID);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void registerShouldDelegateToConnectionTracker() {
        var connection = ConnectionMockFactory.newInstance(CONNECTION_ID);

        this.connectionRegistry.register(connection);

        Mockito.verify(this.connectionTracker).add(connection);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Registered connection", CONNECTION_ID);
    }

    @Test
    void unregisterShouldDelegateToConnectionTracker() {
        var connection = ConnectionMockFactory.newInstance(CONNECTION_ID);

        this.connectionRegistry.unregister(connection);

        Mockito.verify(this.connectionTracker).remove(connection);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Removed connection", CONNECTION_ID);
    }

    @Test
    void stopIdlingShouldIgnoreBusyConnections() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future = (Future<Void>) Mockito.mock(Future.class);

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withIdling(true)
                .withCloseFuture(future)
                .build();
        var connection2 = ConnectionMockFactory.newFactory().withIdling(false).build();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);

        Mockito.verify(connection1).id();
        Mockito.verifyNoMoreInteractions(connection1);

        Mockito.verify(connection2).id();
        Mockito.verifyNoMoreInteractions(connection2);

        Mockito.verify(this.connectionTracker).add(connection1);
        Mockito.verify(this.connectionTracker).add(connection2);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        this.connectionRegistry.stopIdling();

        Mockito.verify(connection1, Mockito.times(3)).id();
        Mockito.verify(connection1).isIdling();
        Mockito.verify(connection1).close();
        Mockito.verify(connection1).closeFuture();
        Mockito.verify(future).get();
        Mockito.verifyNoMoreInteractions(connection1);

        Mockito.verify(connection2).isIdling();
        Mockito.verifyNoMoreInteractions(connection2);

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.INFO)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("Stopping remaining idle connections for connector %s", CONNECTOR_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Stopping idle connection", CONNECTION_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Stopped idle connection", CONNECTION_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.INFO)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("Stopped %d idling connections for connector %s", 1, CONNECTOR_ID);
    }

    @Test
    void stopIdlingShouldHandleInterrupts() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future = (Future<Void>) Mockito.mock(Future.class);

        var cause = new InterruptedException("Fake interrupt");
        Mockito.doThrow(cause).when(future).get();

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withIdling(true)
                .withCloseFuture(future)
                .build();
        var connection2 = ConnectionMockFactory.newFactory().withIdling(true).build();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);
        this.connectionRegistry.stopIdling();

        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();

        Mockito.verify(future).get();

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verify(this.connectionTracker).remove(connection2);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.WARN)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithException(
                        "[" + CONNECTION_ID + "] Interrupted while awaiting clean shutdown of connection", cause);
    }

    @Test
    void stopIdlingShouldHandleExecutionExceptions() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future = (Future<Void>) Mockito.mock(Future.class);

        var cause = new ExecutionException("Fake cause", new IllegalStateException("Oh no"));
        Mockito.doThrow(cause).when(future).get();

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withIdling(true)
                .withCloseFuture(future)
                .build();
        var connection2 = ConnectionMockFactory.newFactory().withIdling(true).build();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);
        this.connectionRegistry.stopIdling();

        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();

        Mockito.verify(future).get();

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verify(this.connectionTracker).remove(connection2);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.WARN)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithException("[" + CONNECTION_ID + "] Clean shutdown of connection has failed", cause);
    }

    @Test
    void stopAllShouldCloseBusyConnections() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future1 = (Future<Void>) Mockito.mock(Future.class);
        @SuppressWarnings("unchecked")
        var future2 = (Future<Void>) Mockito.mock(Future.class);

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withIdling(false)
                .withCloseFuture(future1)
                .build();
        var connection2 = ConnectionMockFactory.newFactory()
                .withIdling(false)
                .withCloseFuture(future2)
                .build();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);

        Mockito.verify(this.connectionTracker).add(connection1);
        Mockito.verify(this.connectionTracker).add(connection2);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        this.connectionRegistry.stopAll();

        Mockito.verify(connection1, Mockito.times(3)).id();
        Mockito.verify(connection1).close();
        Mockito.verify(connection1).closeFuture();
        Mockito.verify(future1).get();
        Mockito.verifyNoMoreInteractions(connection1);

        Mockito.verify(connection2, Mockito.times(3)).id();
        Mockito.verify(connection2).close();
        Mockito.verify(connection2).closeFuture();
        Mockito.verify(future2).get();
        Mockito.verifyNoMoreInteractions(connection2);

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verify(this.connectionTracker).remove(connection2);
        Mockito.verifyNoMoreInteractions(this.connectionTracker);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.INFO)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("Stopping %d connections for connector %s", 2, CONNECTOR_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Stopping connection", CONNECTION_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("[%s] Stopped connection", CONNECTION_ID);
        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.INFO)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithArguments("Stopped all remaining connections for connector %s", CONNECTOR_ID);
    }

    @Test
    void stopAllShouldHandleInterrupts() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future = (Future<Void>) Mockito.mock(Future.class);

        var cause = new InterruptedException("Fake interrupt");
        Mockito.doThrow(cause).when(future).get();

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withCloseFuture(future)
                .build();
        var connection2 = ConnectionMockFactory.newInstance();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);
        this.connectionRegistry.stopAll();

        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();

        Mockito.verify(future).get();

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verify(this.connectionTracker).remove(connection2);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.WARN)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithException(
                        "[" + CONNECTION_ID + "] Interrupted while awaiting clean shutdown of connection", cause);
    }

    @Test
    void stopAllShouldHandleExecutionExceptions() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        var future = (Future<Void>) Mockito.mock(Future.class);

        var cause = new ExecutionException("Fake cause", new IllegalStateException("Oh no"));
        Mockito.doThrow(cause).when(future).get();

        var connection1 = ConnectionMockFactory.newFactory(CONNECTION_ID)
                .withCloseFuture(future)
                .build();
        var connection2 = ConnectionMockFactory.newInstance();

        this.connectionRegistry.register(connection1);
        this.connectionRegistry.register(connection2);
        this.connectionRegistry.stopAll();

        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();

        Mockito.verify(future).get();

        Mockito.verify(this.connectionTracker).remove(connection1);
        Mockito.verify(this.connectionTracker).remove(connection2);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.WARN)
                .forClass(ConnectionRegistry.class)
                .containsMessageWithException("[" + CONNECTION_ID + "] Clean shutdown of connection has failed", cause);
    }
}
