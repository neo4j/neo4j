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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;

class ResponseMetricsConnectorListenerTest {

    @Test
    void shouldRegisterItselfWithNewConnections() {
        var connection = ConnectionMockFactory.newInstance();
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new ResponseMetricsConnectorListener(monitor);

        listener.onConnectionCreated(connection);

        Mockito.verify(connection).registerListener(listener);
        Mockito.verifyNoMoreInteractions(connection);

        Mockito.verifyNoInteractions(monitor);
    }

    @Test
    void shouldNotifyMetricsMonitorOnResponseSuccess() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new ResponseMetricsConnectorListener(monitor);

        listener.onResponseSuccess(MapValue.EMPTY);

        Mockito.verify(monitor).responseSuccess();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMetricsMonitorOnResponseIgnored() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new ResponseMetricsConnectorListener(monitor);

        listener.onResponseIgnored();

        Mockito.verify(monitor).responseIgnored();
        Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldNotifyMetricsMonitorOnResponseFailed() {
        var monitor = Mockito.mock(BoltConnectionMetricsMonitor.class);

        var listener = new ResponseMetricsConnectorListener(monitor);

        listener.onResponseFailed(Error.from(Status.Database.DatabaseNotFound, "Oh no! :("));

        Mockito.verify(monitor).responseFailed(Status.Database.DatabaseNotFound);
        Mockito.verifyNoMoreInteractions(monitor);
    }
}
