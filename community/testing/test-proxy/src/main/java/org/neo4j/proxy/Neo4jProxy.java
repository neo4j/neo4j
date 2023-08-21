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
package org.neo4j.proxy;

import java.io.Closeable;

public interface Neo4jProxy extends Closeable {
    /**
     * Freeze all connections for selected instance and network type but keeps accepting new connections
     */
    void freezeConnection();

    /**
     * Unfreeze all connections for selected instance and network type
     */
    void unfreezeConnection();

    /**
     * Close all connections for selected instance and network type but keeps accepting new connections
     */
    void closeAllConnection();

    /**
     * Close all connections and stop accepting new connections. To start accepting new connections use {@link #startAcceptingConnections()}
     */
    void stopAcceptingConnections();

    /**
     * Start accepting new connections. It needs to be used after {@link #stopAcceptingConnections()}, otherwise it will throw an exception
     */
    void startAcceptingConnections();

    /**
     * @return a configuration that can be used to configure the cluster
     */
    ProxyConfiguration getProxyConfig();
}
