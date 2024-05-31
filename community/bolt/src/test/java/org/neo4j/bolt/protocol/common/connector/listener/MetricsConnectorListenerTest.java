/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connector.listener;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

class MetricsConnectorListenerTest {

    @Test
    void shouldRegisterItselfOnConnectionCreated() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);
        var connection = Mockito.mock(Connection.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onConnectionCreated(connection);

        var listenerCaptor = ArgumentCaptor.forClass(MetricsConnectorListener.class);
        Mockito.verify(connection).registerListener(listenerCaptor.capture());
        Mockito.verifyNoMoreInteractions(connection);

        var registeredListener = listenerCaptor.getValue();
        Assertions.assertThat(registeredListener).isNotNull().isSameAs(listener);

        // monitors are not notified until the connection has actually managed to negotiate a protocol version since we
        // do not wish to pollute the metrics with potentially dead connections
        Mockito.verifyNoInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaConnectionOpenedOnProtocolSelected() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onProtocolSelected(Mockito.mock(BoltProtocol.class));

        Mockito.verify(monitor).connectionOpened();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaConnectionActivatedOnActivated() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onActivated();

        Mockito.verify(monitor).connectionActivated();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaConnectionWaitingOnIdle() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onIdle(42);

        Mockito.verify(monitor).connectionWaiting();
        Mockito.verify(monitor).workerThreadReleased(Mockito.anyLong());
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaWorkerThreadReleasedOnIdle() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onIdle(42);

        Mockito.verify(monitor).connectionWaiting();
        Mockito.verify(monitor).workerThreadReleased(42);
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaMessageReceivedOnRequestReceived() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onRequestReceived(Mockito.mock(RequestMessage.class));

        Mockito.verify(monitor).messageReceived();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaMessageProcessingStartedOnRequestBeginProcessing() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onRequestBeginProcessing(Mockito.mock(RequestMessage.class), 42);

        Mockito.verify(monitor).messageProcessingStarted(42);
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaMessageProcessingCompletedOnRequestCompleteProcessing() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onRequestCompletedProcessing(Mockito.mock(RequestMessage.class), 42);

        Mockito.verify(monitor).messageProcessingCompleted(42);
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaMessageProcessingFailedOnRequestFailedProcessing() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onRequestFailedProcessing(Mockito.mock(RequestMessage.class), new RuntimeException("Oh no :("));

        Mockito.verify(monitor).messageProcessingFailed();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMonitorViaConnectionClosedOnClosed() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onConnectionClosed(true);

        Mockito.verify(monitor).connectionClosed();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotNotifyMonitorViaConnectionClosedOnClosedForUnnegotiatedConnections() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new MetricsConnectorListener(monitor);
        listener.onConnectionClosed(false);

        Mockito.verifyNoMoreInteractions(monitor);
    }
}
