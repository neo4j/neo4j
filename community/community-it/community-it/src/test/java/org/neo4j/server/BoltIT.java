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
package org.neo4j.server;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class BoltIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @AfterEach
    void stopTheServer() {
        if (testWebContainer != null) {
            testWebContainer.shutdown();
        }
    }

    @Test
    void shouldLaunchBolt() throws Throwable {
        // When I run Neo4j with Bolt enabled
        startServerWithBoltEnabled();

        ConnectorPortRegister connectorPortRegister = getDependency(ConnectorPortRegister.class);

        // Then
        assertEventuallyServerResponds(
                "localhost",
                connectorPortRegister.getLocalAddress(ConnectorType.BOLT).getPort());
    }

    @Test
    void shouldBeAbleToSpecifyHostAndPort() throws Throwable {
        // When
        startServerWithBoltEnabled();

        ConnectorPortRegister connectorPortRegister = getDependency(ConnectorPortRegister.class);
        // Then
        assertEventuallyServerResponds(
                "localhost",
                connectorPortRegister.getLocalAddress(ConnectorType.BOLT).getPort());
    }

    @Test
    void boltAddressShouldComeFromConnectorAdvertisedAddress() throws Throwable {
        // Given
        String host = "neo4j.com";

        startServerWithBoltEnabled(host, 9999, "localhost", 0);

        HttpRequest request =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();

        // When
        HttpResponse<String> response = newHttpClient().send(request, ofString());

        // Then
        Map<String, Object> map = JsonHelper.jsonToMap(response.body());
        assertThat(String.valueOf(map.get("bolt_direct"))).contains("bolt://" + host + ":" + 9999);
    }

    private void startServerWithBoltEnabled() throws IOException {
        startServerWithBoltEnabled("localhost", 7687, "localhost", 7687);
    }

    private void startServerWithBoltEnabled(
            String advertisedHost, int advertisedPort, String listenHost, int listenPort) throws IOException {
        testWebContainer = serverOnRandomPorts()
                .withProperty(BoltConnector.enabled.name(), TRUE)
                .withProperty(BoltConnector.encryption_level.name(), "DISABLED")
                .withProperty(BoltConnector.advertised_address.name(), advertisedHost + ":" + advertisedPort)
                .withProperty(BoltConnector.listen_address.name(), listenHost + ":" + listenPort)
                .usingDataDir(testDirectory.homePath().toString())
                .build();
    }

    private static void assertEventuallyServerResponds(String host, int port) throws Exception {
        try (var connection = new SocketConnection(new InetSocketAddress(host, port))) {
            connection.connect().sendDefaultProtocolVersion();

            assertThat(connection).negotiatesDefaultVersion();
        }
    }

    private <T> T getDependency(Class<T> clazz) {
        return testWebContainer.resolveDependency(clazz);
    }
}
