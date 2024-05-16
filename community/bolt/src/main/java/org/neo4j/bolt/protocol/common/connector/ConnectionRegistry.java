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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.util.concurrent.Futures;

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

    public void stopIdling(Duration shutdownDeadline) {
        var it = this.connections.iterator();

        log.info("Stopping remaining idle connections for connector %s", this.connectorId);
        var n = 0;
        var futures = new ArrayList<Future<?>>();
        while (it.hasNext()) {
            var connection = it.next();
            if (!connection.isIdling()) {
                continue;
            }

            log.debug("[%s] Stopping idle connection", connection.id());

            connection.close();
            futures.add(connection.closeFuture());

            it.remove();
            if (this.connectionTracker != null) {
                this.connectionTracker.remove(connection);
            }

            ++n;

            log.debug("[%s] Stopped idle connection", connection.id());
        }

        var combined = Futures.combine(futures);
        try {
            if (!shutdownDeadline.isZero()) {
                combined.get(shutdownDeadline.toMillis(), TimeUnit.MILLISECONDS);
            } else {
                combined.get();
            }
        } catch (TimeoutException ex) {
            log.warn(
                    "Idle connections have failed to terminate within acceptable duration (" + shutdownDeadline + ")",
                    ex);
        } catch (InterruptedException ex) {
            log.warn("Interrupted while awaiting clean shutdown of idle connections", ex);
        } catch (ExecutionException ex) {
            log.warn("Clean shutdown of idle connections has failed", ex);
        }

        log.info("Stopped %d idling connections for connector %s", n, this.connectorId);
    }

    public void stopAll(Duration shutdownDeadline) {
        log.info("Stopping %d connections for connector %s", this.connections.size(), this.connectorId);

        var futures = this.connections.stream()
                .map(connection -> {
                    log.debug("[%s] Requesting connection closure", connection.id());

                    connection.close();

                    if (this.connectionTracker != null) {
                        this.connectionTracker.remove(connection);
                    }

                    return connection.closeFuture();
                })
                .toList();

        var combined = Futures.combine(futures);
        try {
            if (!shutdownDeadline.isZero()) {
                combined.get(shutdownDeadline.toMillis(), TimeUnit.MILLISECONDS);
            } else {
                combined.get();
            }
        } catch (TimeoutException ex) {
            log.error("Connection have failed to terminate within acceptable duration (" + shutdownDeadline + ")", ex);
            return;
        } catch (InterruptedException ex) {
            log.warn("Interrupted while awaiting clean shutdown of connections", ex);
        } catch (ExecutionException ex) {
            log.warn("Clean shutdown of connections has failed", ex);
        }

        this.connections.clear();
        log.info("Stopped all remaining connections for connector %s", this.connectorId);
    }
}
