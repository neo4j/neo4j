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
package org.neo4j.bolt.protocol.common.connector.accounting.error;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;

/**
 * Handles the accounting of benign error conditions within Bolt connectors in order to facilitate
 * reporting of systematic issues.
 */
public interface ErrorAccountant {

    /**
     * Notifies the accountant about a network caused connection abort (e.g. connection list,
     * failed to flush buffers, etc).
     */
    void notifyNetworkAbort(Connection connection, Throwable cause);

    /**
     * Notifies the accountant about a thread starvation caused abort (e.g. no thread was available
     * to schedule a certain task).
     */
    void notifyThreadStarvation(Connection connection, Throwable cause);
}
