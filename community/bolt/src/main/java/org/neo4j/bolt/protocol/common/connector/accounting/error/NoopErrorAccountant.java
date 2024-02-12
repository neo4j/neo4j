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
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public final class NoopErrorAccountant implements ErrorAccountant {
    private final Log userLog;

    public NoopErrorAccountant(LogService logging) {
        this.userLog = logging.getUserLog(NoopErrorAccountant.class);
    }

    @Override
    public void notifyNetworkAbort(Connection connection, Throwable cause) {
        this.userLog.warn("[" + connection.id() + "] Terminating connection due to network error", cause);
    }

    @Override
    public void notifyThreadStarvation(Connection connection, Throwable cause) {
        this.userLog.error(
                "[%s] Unable to schedule for execution since there are no available threads to serve it at the "
                        + "moment.",
                connection.id());
    }
}
