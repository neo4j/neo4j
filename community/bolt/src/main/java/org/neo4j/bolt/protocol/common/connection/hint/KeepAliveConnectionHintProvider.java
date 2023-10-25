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
package org.neo4j.bolt.protocol.common.connection.hint;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnector.KeepAliveRequestType;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

public final class KeepAliveConnectionHintProvider extends AbstractSingleKeyConnectionHintProvider {

    private final Config config;

    public KeepAliveConnectionHintProvider(Config config) {
        this.config = config;
    }

    @Override
    public boolean isApplicable() {
        return this.config.get(BoltConnector.connection_keep_alive_type) == KeepAliveRequestType.ALL;
    }

    @Override
    protected String getKey() {
        return "connection.recv_timeout_seconds";
    }

    @Override
    protected AnyValue getValue() {
        var keepAliveInterval = config.get(BoltConnector.connection_keep_alive);
        var keepAliveProbes = config.get(BoltConnector.connection_keep_alive_probes);

        return Values.longValue(keepAliveInterval.toSeconds() * keepAliveProbes);
    }
}
