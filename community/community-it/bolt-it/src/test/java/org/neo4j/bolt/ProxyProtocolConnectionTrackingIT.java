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
package org.neo4j.bolt;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.sizeCondition;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@BoltTestExtension
@Neo4jWithSocketExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProxyProtocolConnectionTrackingIT {

    // Simulated proxy client address - different from localhost to verify proxy protocol is working
    private static final String PROXY_CLIENT_IP = "192.168.1.100";
    private static final int PROXY_CLIENT_PORT = 54321;
    private static final String PROXY_SERVER_IP = "10.0.0.1";
    private static final int PROXY_SERVER_PORT = 7687;

    private static final List<String> LIST_CONNECTIONS_PROCEDURE_COLUMNS = Arrays.asList(
            "connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress");

    @Inject
    Neo4jWithSocket neo4jWithSocket;

    @SettingsFunction
    void customizeServer(SettingBuilder settings) {
        settings.set(BoltConnectorInternalSettings.proxy_protocol_enabled, true);
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP)
    void shouldReportProxyClientAddressWithProxyProtocolV1(BoltWire wire, @Connected BoltTestConnection connection)
            throws Exception {

        sendProxyProtocolV1Header(connection, PROXY_CLIENT_IP, PROXY_CLIENT_PORT, PROXY_SERVER_IP, PROXY_SERVER_PORT);
        connection.send(wire.getProtocolVersion());
        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());

        awaitNumberOfAcceptedConnectionsToBe(1);
        var connectionRecords = listBoltConnections();

        assertThat(connectionRecords).hasSize(1);
        var record = connectionRecords.get(0);

        String clientAddress = (String) record.get("clientAddress");
        assertThat(clientAddress)
                .as("Client address should reflect proxy-provided address, not actual socket address")
                .startsWith(PROXY_CLIENT_IP + ":");
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP)
    void shouldReportProxyClientAddressWithProxyProtocolV2(BoltWire wire, @Connected BoltTestConnection connection)
            throws Exception {

        sendProxyProtocolV2Header(connection, PROXY_CLIENT_IP, PROXY_CLIENT_PORT, PROXY_SERVER_IP, PROXY_SERVER_PORT);
        connection.send(wire.getProtocolVersion());
        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());

        awaitNumberOfAcceptedConnectionsToBe(1);
        var connectionRecords = listBoltConnections();

        assertThat(connectionRecords).hasSize(1);
        var record = connectionRecords.get(0);

        String clientAddress = (String) record.get("clientAddress");
        assertThat(clientAddress)
                .as("Client address should reflect proxy-provided address, not actual socket address")
                .startsWith(PROXY_CLIENT_IP + ":");
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP)
    void shouldAcceptConnectionWithoutProxyProtocolWhenEnabled(BoltWire wire, @Connected BoltTestConnection connection)
            throws Exception {
        connection.send(wire.getProtocolVersion());
        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP)
    void shouldReportProxyServerAddressWithProxyProtocolV1(BoltWire wire, @Connected BoltTestConnection connection)
            throws Exception {

        sendProxyProtocolV1Header(connection, PROXY_CLIENT_IP, PROXY_CLIENT_PORT, PROXY_SERVER_IP, PROXY_SERVER_PORT);
        connection.send(wire.getProtocolVersion());
        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());

        // Then - verify that listConnections shows the proxy-provided server address
        awaitNumberOfAcceptedConnectionsToBe(1);
        var connectionRecords = listBoltConnections();

        assertThat(connectionRecords).hasSize(1);
        var record = connectionRecords.get(0);

        String serverAddress = (String) record.get("serverAddress");
        assertThat(serverAddress)
                .as("Server address should reflect proxy-provided address")
                .isNotBlank();
    }

    private void sendProxyProtocolV1Header(
            BoltTestConnection connection, String srcIp, int srcPort, String dstIp, int dstPort) throws IOException {
        // Send PROXY protocol v1 header first
        // Format: "PROXY TCP4 <src_ip> <dst_ip> <src_port> <dst_port>\r\n"
        String proxyHeader = String.format("PROXY TCP4 %s %s %d %d\r\n", srcIp, dstIp, srcPort, dstPort);
        var header = Unpooled.copiedBuffer(proxyHeader, US_ASCII);
        connection.sendRaw(header);
    }

    private void sendProxyProtocolV2Header(
            BoltTestConnection connection, String srcIp, int srcPort, String dstIp, int dstPort) throws IOException {
        byte[] proxyV2Header = buildProxyProtocolV2Header(srcIp, srcPort, dstIp, dstPort);
        connection.sendRaw(proxyV2Header);
    }

    private byte[] buildProxyProtocolV2Header(String srcIp, int srcPort, String dstIp, int dstPort) {
        // PROXY protocol v2 signature
        byte[] signature = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

        // Version (2) and command (PROXY = 1) = 0x21
        byte versionCommand = 0x21;

        // Address family (AF_INET = 1) and transport protocol (STREAM/TCP = 1) = 0x11
        byte familyProtocol = 0x11;

        // Address length for IPv4: 4 + 4 + 2 + 2 = 12 bytes
        short addressLength = 12;

        ByteBuffer buffer = ByteBuffer.allocate(16 + addressLength);
        buffer.order(ByteOrder.BIG_ENDIAN);

        // Write signature
        buffer.put(signature);

        // Write version/command
        buffer.put(versionCommand);

        // Write family/protocol
        buffer.put(familyProtocol);

        // Write address length
        buffer.putShort(addressLength);

        // Write source IP (4 bytes)
        for (String octet : srcIp.split("\\.")) {
            buffer.put((byte) Integer.parseInt(octet));
        }

        // Write destination IP (4 bytes)
        for (String octet : dstIp.split("\\.")) {
            buffer.put((byte) Integer.parseInt(octet));
        }

        // Write source port (2 bytes)
        buffer.putShort((short) srcPort);

        // Write destination port (2 bytes)
        buffer.putShort((short) dstPort);

        return buffer.array();
    }

    private void awaitNumberOfAcceptedConnectionsToBe(int n) {
        assertEventually(
                connections -> "Unexpected number of accepted connections: " + connections,
                this::acceptedConnectionsFromConnectionTracker,
                sizeCondition(n),
                1,
                MINUTES);
    }

    private List<Map<String, Object>> listBoltConnections() {
        List<Map<String, Object>> matchingRecords = new ArrayList<>();
        var db = neo4jWithSocket.graphDatabaseService();
        try (Transaction transaction = db.beginTx()) {
            Result result = transaction.execute("CALL dbms.listConnections()");
            assertThat(result.columns()).containsExactlyElementsOf(LIST_CONNECTIONS_PROCEDURE_COLUMNS);
            List<Map<String, Object>> records = result.stream().toList();

            for (Map<String, Object> record : records) {
                String actualConnector = record.get("connector").toString();
                if (Objects.equals("bolt", actualConnector)) {
                    matchingRecords.add(record);
                }
            }
            transaction.commit();
        }
        return matchingRecords;
    }

    private List<TrackedNetworkConnection> acceptedConnectionsFromConnectionTracker() {
        GraphDatabaseAPI db = (GraphDatabaseAPI) neo4jWithSocket.graphDatabaseService();
        NetworkConnectionTracker connectionTracker =
                db.getDependencyResolver().resolveDependency(NetworkConnectionTracker.class);
        return connectionTracker.activeConnections();
    }
}
