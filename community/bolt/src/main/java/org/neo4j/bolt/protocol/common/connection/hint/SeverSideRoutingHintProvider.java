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

import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.v58.BoltProtocolV58;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;

public class SeverSideRoutingHintProvider extends AbstractSingleKeyConnectionHintProvider {
    private final Config config;

    public SeverSideRoutingHintProvider(Config config) {
        this.config = config;
    }

    @Override
    protected String getKey() {
        return "ssr.enabled";
    }

    @Override
    protected AnyValue getValue() {
        // hint is guarded by applicable check meaning it will only be present if protocol version
        // supports this hint and the functionality is actually enabled
        return BooleanValue.TRUE;
    }

    @Override
    public boolean isApplicable() {
        return config.get(GraphDatabaseSettings.routing_enabled);
    }

    @Override
    public ProtocolVersion supportedSince() {
        return BoltProtocolV58.VERSION;
    }
}
