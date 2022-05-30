/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connection;

import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.values.storable.Values;

public class DefaultConnectionHintProvider {

    private static final String HINT_CONNECTION_RECV_TIMEOUT_SECONDS = "connection.recv_timeout_seconds";

    public static Function<Config, ConnectionHintProvider> connectionHintProviderFunction = config -> hints -> {
        if (config.get(BoltConnector.connection_keep_alive_type) == BoltConnector.KeepAliveRequestType.ALL) {
            var keepAliveInterval = config.get(BoltConnector.connection_keep_alive);
            var keepAliveProbes = config.get(BoltConnector.connection_keep_alive_probes);

            hints.add(
                    HINT_CONNECTION_RECV_TIMEOUT_SECONDS,
                    Values.longValue(keepAliveInterval.toSeconds() * keepAliveProbes));
        }
    };
}
