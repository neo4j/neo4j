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
package org.neo4j.bolt.test.extension.lifecycle;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;

public class TransportConnectionManager implements AfterEachCallback {

    private final Lock lock = new ReentrantLock();
    private final List<BoltTestConnection> activeConnections = new ArrayList<>();

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        this.lock.lock();

        try {
            this.activeConnections.forEach(connection -> {
                try {
                    connection.close();
                } catch (Throwable ignore) {
                }
            });

            this.activeConnections.clear();
        } finally {
            this.lock.unlock();
        }
    }

    public BoltTestConnection acquire(
            ConnectorTransport transport, BoltWire wire, SocketAddress address, TransportType transportType) {
        var connection = transportType.getFactory().create(transport, wire, address);
        this.lock.lock();
        try {
            this.activeConnections.add(connection);
        } finally {
            this.lock.unlock();
        }

        return connection;
    }
}
