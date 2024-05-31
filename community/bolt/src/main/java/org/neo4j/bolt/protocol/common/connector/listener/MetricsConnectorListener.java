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

import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

/**
 * Generates metrics for associated Bolt connectors.
 */
public class MetricsConnectorListener implements ConnectorListener, ConnectionListener {
    private final BoltConnectionMetricsMonitor monitor;

    public MetricsConnectorListener(BoltConnectionMetricsMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void onListenerAdded() {
        // NOOP - Required due to lacking multiple inheritance support
    }

    @Override
    public void onListenerRemoved() {
        // NOOP - Required due to lacking multiple inheritance support
    }

    @Override
    public void onConnectionCreated(Connection connection) {
        connection.registerListener(this);
    }

    @Override
    public void onProtocolSelected(BoltProtocol protocol) {
        this.monitor.connectionOpened();
    }

    @Override
    public void onActivated() {
        this.monitor.connectionActivated();
    }

    @Override
    public void onIdle(long boundTimeMillis) {
        this.monitor.connectionWaiting();
        this.monitor.workerThreadReleased(boundTimeMillis);
    }

    @Override
    public void onRequestReceived(RequestMessage message) {
        this.monitor.messageReceived();
    }

    @Override
    public void onRequestBeginProcessing(RequestMessage message, long queuedForMillis) {
        this.monitor.messageProcessingStarted(queuedForMillis);
    }

    @Override
    public void onRequestCompletedProcessing(RequestMessage message, long processedForMillis) {
        this.monitor.messageProcessingCompleted(processedForMillis);
    }

    @Override
    public void onRequestFailedProcessing(RequestMessage message, Throwable cause) {
        this.monitor.messageProcessingFailed();
    }

    @Override
    public void onConnectionClosed(boolean isNegotiatedConnection) {
        // we only track negotiated connections.
        if (isNegotiatedConnection) {
            this.monitor.connectionClosed();
        }
    }
}
