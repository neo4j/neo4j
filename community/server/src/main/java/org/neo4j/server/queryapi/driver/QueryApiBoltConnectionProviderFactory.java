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
package org.neo4j.server.queryapi.driver;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.server.queryapi.driver.boltmessage.BoltMessageBuilderProvider;

public final class QueryApiBoltConnectionProviderFactory implements BoltConnectionProviderFactory {
    public static final String SCHEME = "queryapi";

    private static final BoltConnectionProviderFactory DELEGATE = new NettyBoltConnectionProviderFactory();
    private static boolean useJavaObjects;
    private static BoltProtocolVersion preconfiguredProtocolVersion;

    @Override
    public boolean supports(String scheme) {
        return SCHEME.equals(scheme);
    }

    @Override
    public BoltConnectionProvider create(
            LoggingProvider loggingProvider,
            ValueFactory valueFactory,
            ObservationProvider observationProvider,
            Map<String, ?> additionalConfig) {
        Map<String, Object> config = new HashMap<>(additionalConfig);

        if (useJavaObjects) {
            config.put("channelPipelineBuilderProvider", new BoltMessageBuilderProvider());
            config.put("boltProtocolVersion", preconfiguredProtocolVersion);
        }

        var delegate = DELEGATE.create(loggingProvider, valueFactory, observationProvider, config);
        return new QueryApiBoltConnectionProvider(delegate);
    }

    public static void setUseJavaObjects(boolean useJavaObjects) {
        QueryApiBoltConnectionProviderFactory.useJavaObjects = useJavaObjects;
    }

    public static void setPreconfiguredProtocolVersion(
            BoltConnectorInternalSettings.ConfiguredProtocolVersion version) {
        var rawBytesVersion = version.major() + (version.minor() << 8);
        preconfiguredProtocolVersion = BoltProtocolVersion.fromRawBytes(rawBytesVersion);
    }
}
