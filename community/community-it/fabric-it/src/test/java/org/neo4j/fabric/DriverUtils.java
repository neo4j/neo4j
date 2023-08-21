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
package org.neo4j.fabric;

import java.net.URI;
import java.net.URISyntaxException;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.internal.helpers.HostnamePort;

public class DriverUtils {
    public static Driver createDriver(ConnectorPortRegister portRegister) {
        return GraphDatabase.driver(
                getBoltRoutingUri(portRegister),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize(3)
                        .build());
    }

    private static URI getBoltRoutingUri(ConnectorPortRegister portRegister) {
        return getBoltUri(portRegister, "neo4j");
    }

    private static URI getBoltUri(ConnectorPortRegister portRegister, String scheme) {
        HostnamePort hostPort = portRegister.getLocalAddress(ConnectorType.BOLT);
        try {
            return new URI(scheme, null, hostPort.getHost(), hostPort.getPort(), null, null, null);
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(x.getMessage(), x);
        }
    }
}
