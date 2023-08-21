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

import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.values.virtual.MapValue;

/**
 * Reports command result metrics.
 * <p />
 * This listener is enabled via the {@link BoltConnectorInternalSettings#enable_response_metrics} configuration
 * property.
 */
public class ResponseMetricsConnectorListener implements ConnectorListener, ConnectionListener {
    private final BoltConnectionMetricsMonitor monitor;

    public ResponseMetricsConnectorListener(BoltConnectionMetricsMonitor monitor) {
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
    public void onResponseSuccess(MapValue metadata) {
        this.monitor.responseSuccess();
    }

    @Override
    public void onResponseFailed(Error error) {
        this.monitor.responseFailed(error.status());
    }

    @Override
    public void onResponseIgnored() {
        this.monitor.responseIgnored();
    }
}
