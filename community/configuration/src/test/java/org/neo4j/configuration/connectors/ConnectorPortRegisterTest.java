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
package org.neo4j.configuration.connectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;

class ConnectorPortRegisterTest {
    private final ConnectorPortRegister portRegister = new ConnectorPortRegister();

    @Test
    void shouldRegisterInetSocketAddress() {
        InetSocketAddress address = new InetSocketAddress(0);

        portRegister.register(ConnectorType.BOLT, address);

        verifyAddress(ConnectorType.BOLT, address.getHostString(), address.getPort());
    }

    @Test
    void shouldRegisterSocketAddress() {
        SocketAddress address = new SocketAddress("neo4j.com", 12345);

        portRegister.register(ConnectorType.BOLT, address);

        verifyAddress(ConnectorType.BOLT, address.getHostname(), address.getPort());
    }

    @Test
    void shouldDeregister() {
        SocketAddress address = new SocketAddress("neo4j.com", 42);
        portRegister.register(ConnectorType.BOLT, address);
        assertNotNull(portRegister.getLocalAddress(ConnectorType.BOLT));

        portRegister.deregister(ConnectorType.BOLT);

        assertNull(portRegister.getLocalAddress(ConnectorType.BOLT));
    }

    @Test
    void shouldReturnAddressByKey() {
        SocketAddress address1 = new SocketAddress("localhost", 7574);
        SocketAddress address2 = new SocketAddress("neo4j.com", 8989);
        SocketAddress address3 = new SocketAddress("8.8.8.8", 80);

        portRegister.register(ConnectorType.BOLT, address1);
        portRegister.register(ConnectorType.HTTP, address2);
        portRegister.register(ConnectorType.HTTPS, address3);

        verifyAddress(ConnectorType.BOLT, "localhost", 7574);
        verifyAddress(ConnectorType.HTTP, "neo4j.com", 8989);
        verifyAddress(ConnectorType.HTTPS, "8.8.8.8", 80);
    }

    private void verifyAddress(ConnectorType connectorType, String expectedHostname, int expectedPort) {
        HostnamePort expectedAddress = new HostnamePort(expectedHostname, expectedPort);
        assertEquals(expectedAddress, portRegister.getLocalAddress(connectorType));
    }
}
