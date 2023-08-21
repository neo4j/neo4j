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
package org.neo4j.bolt.protocol.common.connector;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * Provides an intermediary registry of Bolt connections.
 */
public class ConnectionRegistry {
    private final String connectorId;
    private final NetworkConnectionTracker connectionTracker;
    private final InternalLog log;

    private final Deque<Connection> connections = new ConcurrentLinkedDeque<>();

    public ConnectionRegistry(
            String connectorId, NetworkConnectionTracker connectionTracker, InternalLogProvider logging) {
        this.connectorId = connectorId;
        this.connectionTracker = connectionTracker;
        this.log = logging.getLog(ConnectionRegistry.class);
    }

    public String allocateId() {
        return this.connectionTracker.newConnectionId(this.connectorId);
    }

    public void register(Connection connection) {
        this.connections.add(connection);

        if (this.connectionTracker != null) {
            this.connectionTracker.add(connection);
        }

        log.debug("[%s] Registered connection", connection.id());
    }

    public void unregister(Connection connection) {
        this.connections.remove(connection);

        if (this.connectionTracker != null) {
            this.connectionTracker.remove(connection);
        }

        log.debug("[%s] Removed connection", connection.id());
    }

    public void stopIdling() {
        var it = this.connections.iterator();

        log.info("Stopping remaining idle connections for connector %s", this.connectorId);
        var n = 0;
        while (it.hasNext()) {
            var connection = it.next();
            if (!connection.isIdling()) {
                continue;
            }

            log.debug("[%s] Stopping idle connection", connection.id());

            connection.close();

            try {
                connection.closeFuture().get();
            } catch (InterruptedException ex) {
                log.warn("[" + connection.id() + "] Interrupted while awaiting clean shutdown of connection", ex);
            } catch (ExecutionException ex) {
                log.warn("[" + connection.id() + "] Clean shutdown of connection has failed", ex);
            }

            it.remove();
            if (this.connectionTracker != null) {
                this.connectionTracker.remove(connection);
            }

            ++n;

            log.debug("[%s] Stopped idle connection", connection.id());
        }

        log.info("Stopped %d idling connections for connector %s", n, this.connectorId);
    }

    public void stopAll() {
        log.info("Stopping %d connections for connector %s", this.connections.size(), this.connectorId);

        this.connections.forEach(connection -> {
            log.debug("[%s] Stopping connection", connection.id());

            connection.close();

            try {
                connection.closeFuture().get();
            } catch (InterruptedException ex) {
                log.warn("[" + connection.id() + "] Interrupted while awaiting clean shutdown of connection", ex);
            } catch (ExecutionException ex) {
                log.warn("[" + connection.id() + "] Clean shutdown of connection has failed", ex);
            }

            if (this.connectionTracker != null) {
                this.connectionTracker.remove(connection);
            }

            log.debug("[%s] Stopped connection", connection.id());
        });
        this.connections.clear();

        log.info("Stopped all remaining connections for connector %s", this.connectorId);
    }
}
