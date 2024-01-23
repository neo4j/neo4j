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
package org.neo4j.net;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.HttpHeaders.USER_AGENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Index.atIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.bolt.testing.assertions.AnyValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ListValueAssertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.BOLT;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTP;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTP2;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTP2S;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTPS;
import static org.neo4j.server.configuration.ServerSettings.webserver_max_threads;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.sizeCondition;
import static org.neo4j.test.server.HTTP.RawPayload;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.basicAuthHeader;
import static org.neo4j.test.server.HTTP.newClient;
import static org.neo4j.test.server.HTTP.withBasicAuth;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.InProcessNeo4jBuilder;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.configuration.ConfigurableTransports;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConnectionTrackingIT {
    private static final String NEO4J_USER_PWD = "password";
    private static final String OTHER_USER = "otherUser";
    private static final String OTHER_USER_PWD = "password";

    private static final List<String> LIST_CONNECTIONS_PROCEDURE_COLUMNS = Arrays.asList(
            "connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress");

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Set<TransportConnection> connections = ConcurrentHashMap.newKeySet();
    private final Set<HttpClient> httpClients = ConcurrentHashMap.newKeySet();

    private final BoltWire wire = new BoltDefaultWire();

    @Inject
    private TestDirectory dir;

    private GraphDatabaseAPI db;
    private InProcessNeo4j neo4j;
    private long dummyNodeId;

    @BeforeAll
    void beforeAll() {
        neo4j = (InProcessNeo4j) new InProcessNeo4jBuilder(dir.homePath())
                .withConfig(neo4j_home, dir.absolutePath())
                .withConfig(auth_enabled, true)
                .withConfig(HttpConnector.enabled, true)
                .withConfig(HttpsConnector.enabled, true)
                .withConfig(
                        ServerSettings.http_enabled_transports,
                        Set.of(ConfigurableTransports.HTTP1_1, ConfigurableTransports.HTTP2))
                .withConfig(webserver_max_threads, 50) /* higher than the amount of concurrent requests tests execute*/
                .build();
        neo4j.start();
        db = (GraphDatabaseAPI) neo4j.defaultDatabaseService();

        changeDefaultPasswordForUserNeo4j(NEO4J_USER_PWD);
        createNewUser(OTHER_USER, OTHER_USER_PWD);
        dummyNodeId = createDummyNode();
        IOUtils.closeAllSilently(acceptedConnectionsFromConnectionTracker());
    }

    @AfterAll
    void afterAll() {
        executor.shutdownNow();
        neo4j.close();
    }

    @AfterEach
    void afterEach() {
        for (TransportConnection connection : connections) {
            try {
                connection.disconnect();
            } catch (Exception ignore) {
            }
        }
        httpClients.clear();
        IOUtils.closeAllSilently(acceptedConnectionsFromConnectionTracker());
        terminateAllTransactions();
        awaitNumberOfAcceptedConnectionsToBe(0);
    }

    @Test
    void shouldListNoConnectionsWhenIdle() {
        verifyConnectionCount(HTTP, null, 0);
        verifyConnectionCount(HTTPS, null, 0);
        verifyConnectionCount(BOLT, null, 0);
    }

    @Test
    void shouldListUnauthenticatedHttpConnections() throws Exception {
        testListingOfUnauthenticatedConnections(5, 0, 0, 0, 0);
    }

    @Test
    void shouldListUnauthenticatedHttp2Connections() throws Exception {
        testListingOfUnauthenticatedConnections(0, 0, 0, 5, 0);
    }

    @Test
    void shouldListUnauthenticatedHttpsConnections() throws Exception {
        testListingOfUnauthenticatedConnections(0, 2, 0, 0, 0);
    }

    @Test
    void shouldListUnauthenticatedHttp2sConnections() throws Exception {
        testListingOfUnauthenticatedConnections(0, 0, 0, 0, 5);
    }

    @Test
    void shouldListUnauthenticatedBoltConnections() throws Exception {
        testListingOfUnauthenticatedConnections(0, 0, 4, 0, 0);
    }

    @Test
    void shouldListUnauthenticatedConnections() throws Exception {
        testListingOfUnauthenticatedConnections(3, 2, 7, 0, 0);
    }

    @Test
    void shouldListAuthenticatedHttpConnections() throws Exception {
        lockNodeAndExecute(dummyNodeId, () -> {
            for (int i = 0; i < 4; i++) {
                updateNodeViaHttp(dummyNodeId, "neo4j", NEO4J_USER_PWD, HttpClient.Version.HTTP_1_1);
            }
            for (int i = 0; i < 3; i++) {
                updateNodeViaHttp(dummyNodeId, OTHER_USER, OTHER_USER_PWD, HttpClient.Version.HTTP_1_1);
            }
        });
        awaitNumberOfAuthenticatedConnectionsToBe(7);
        verifyAuthenticatedConnectionCount(HTTP, "neo4j", 4);
        verifyAuthenticatedConnectionCount(HTTP, OTHER_USER, 3);
    }

    @Test
    void shouldListAuthenticatedHttpsConnections() throws Exception {
        lockNodeAndExecute(dummyNodeId, () -> {
            for (int i = 0; i < 4; i++) {
                updateNodeViaHttps(dummyNodeId, "neo4j", NEO4J_USER_PWD, HttpClient.Version.HTTP_1_1);
            }
            for (int i = 0; i < 5; i++) {
                updateNodeViaHttps(dummyNodeId, OTHER_USER, OTHER_USER_PWD, HttpClient.Version.HTTP_1_1);
            }

            awaitNumberOfAuthenticatedConnectionsToBe(9);
        });
        verifyAuthenticatedConnectionCount(HTTPS, "neo4j", 4);
        verifyAuthenticatedConnectionCount(HTTPS, OTHER_USER, 5);
    }

    @Test
    void shouldListAuthenticatedBoltConnections() throws Exception {
        lockNodeAndExecute(dummyNodeId, () -> {
            for (int i = 0; i < 2; i++) {
                updateNodeViaBolt(dummyNodeId, "neo4j", NEO4J_USER_PWD);
            }
            for (int i = 0; i < 5; i++) {
                updateNodeViaBolt(dummyNodeId, OTHER_USER, OTHER_USER_PWD);
            }
        });
        awaitNumberOfAuthenticatedConnectionsToBe(7);
        verifyAuthenticatedConnectionCount(BOLT, "neo4j", 2);
        verifyAuthenticatedConnectionCount(BOLT, OTHER_USER, 5);
    }

    @Test
    void shouldListAuthenticatedConnections() throws Exception {
        lockNodeAndExecute(dummyNodeId, () -> {
            for (int i = 0; i < 4; i++) {
                updateNodeViaBolt(dummyNodeId, OTHER_USER, OTHER_USER_PWD);
            }
            for (int i = 0; i < 1; i++) {
                updateNodeViaHttp(dummyNodeId, "neo4j", NEO4J_USER_PWD, HttpClient.Version.HTTP_1_1);
            }
            for (int i = 0; i < 5; i++) {
                updateNodeViaHttps(dummyNodeId, "neo4j", NEO4J_USER_PWD, HttpClient.Version.HTTP_1_1);
            }
            for (int i = 0; i < 5; i++) {
                updateNodeViaHttps(dummyNodeId, "neo4j", NEO4J_USER_PWD, HttpClient.Version.HTTP_2);
            }

            awaitNumberOfAcceptedConnectionsToBe(15);
        });
        verifyConnectionCount(BOLT, OTHER_USER, 4);
        verifyConnectionCount(HTTP, "neo4j", 1);
        verifyConnectionCount(HTTPS, "neo4j", 5);
        verifyConnectionCount(HTTP2S, null, 5);
    }

    @Test
    void shouldKillHttpConnection() throws Exception {
        testKillingOfConnections(neo4j.httpURI(), HTTP, 4);
    }

    @Test
    void shouldKillHttpsConnection() throws Exception {
        testKillingOfHttpConnections(neo4j.httpURI(), HTTP);
    }

    @Test
    void shouldKillHttp2Connection() throws Exception {
        testKillingOfHttpConnections(neo4j.httpURI(), HTTP2);
    }

    @Test
    void shouldKillHttp2sConnection() throws Exception {
        testKillingOfHttpConnections(neo4j.httpsURI(), HTTP2S);
    }

    private void testKillingOfHttpConnections(URI uri, TestConnector testConnector) throws Exception {
        for (int i = 0; i < 2; i++) {
            initiatedHttpClient(uri, testConnector.httpVersion);
        }

        awaitNumberOfAcceptedConnectionsToBe(2);
        verifyConnectionCount(testConnector, null, 2);

        killAcceptedConnectionViaBolt();
        verifyConnectionCount(testConnector, null, 0);
    }

    @Test
    void shouldKillBoltConnection() throws Exception {
        testKillingOfConnections(neo4j.boltURI(), BOLT, 3);
    }

    private void testListingOfUnauthenticatedConnections(
            int httpCount, int httpsCount, int boltCount, int http2Count, int http2sCount) throws Exception {
        for (int i = 0; i < httpCount; i++) {
            connectSocketTo(neo4j.httpURI());
        }

        for (int i = 0; i < httpsCount; i++) {
            initiatedHttpClient(neo4j.httpsURI(), HttpClient.Version.HTTP_1_1);
        }

        for (int i = 0; i < http2Count; i++) {
            initiatedHttpClient(neo4j.httpURI(), HttpClient.Version.HTTP_2);
        }

        for (int i = 0; i < http2sCount; i++) {
            initiatedHttpClient(neo4j.httpsURI(), HttpClient.Version.HTTP_2);
        }

        for (int i = 0; i < boltCount; i++) {
            connectSocketTo(neo4j.boltURI());
        }

        awaitNumberOfAcceptedConnectionsToBe(httpCount + httpsCount + boltCount + http2Count + http2sCount);

        verifyConnectionCount(HTTP, null, httpCount);
        verifyConnectionCount(HTTPS, null, httpsCount);
        verifyConnectionCount(HTTP2, null, http2Count);
        verifyConnectionCount(HTTP2S, null, http2sCount);
        verifyConnectionCount(BOLT, null, boltCount);
    }

    private void testKillingOfConnections(URI uri, TestConnector connector, int count) throws Exception {
        List<TransportConnection> socketConnections = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            socketConnections.add(connectSocketTo(uri));
        }

        awaitNumberOfAcceptedConnectionsToBe(count);
        verifyConnectionCount(connector, null, count);

        killAcceptedConnectionViaBolt();
        verifyConnectionCount(connector, null, 0);

        for (TransportConnection socketConnection : socketConnections) {
            assertConnectionBreaks(socketConnection);
        }
    }

    private TransportConnection connectSocketTo(URI uri) throws IOException {
        var connection = new SocketConnection(new InetSocketAddress(uri.getHost(), uri.getPort())).connect();

        connections.add(connection);

        return connection;
    }

    private HttpClient initiatedHttpClient(URI uri, HttpClient.Version version)
            throws IOException, InterruptedException {
        var httpClient = newClient(version);
        httpClients.add(httpClient);

        var httpRequest = HttpRequest.newBuilder(uri).GET().build();

        httpClient.send(httpRequest, BodyHandlers.ofString());
        return httpClient;
    }

    private void awaitNumberOfAuthenticatedConnectionsToBe(int n) {
        assertEventually(
                "Unexpected number of authenticated connections",
                this::authenticatedConnectionsFromConnectionTracker,
                sizeCondition(n),
                1,
                MINUTES);
    }

    private void awaitNumberOfAcceptedConnectionsToBe(int n) {
        assertEventually(
                connections -> "Unexpected number of accepted connections: " + connections,
                this::acceptedConnectionsFromConnectionTracker,
                sizeCondition(n),
                1,
                MINUTES);
    }

    private void verifyConnectionCount(TestConnector connector, String username, int expectedCount) {
        verifyConnectionCount(connector, username, expectedCount, false);
    }

    private void verifyAuthenticatedConnectionCount(TestConnector connector, String username, int expectedCount) {
        verifyConnectionCount(connector, username, expectedCount, true);
    }

    private void verifyConnectionCount(
            TestConnector connector, String username, int expectedCount, boolean expectAuthenticated) {
        assertEventually(
                connections -> "Unexpected number of listed connections: " + connections,
                () -> listMatchingConnection(connector, username, expectAuthenticated),
                sizeCondition(expectedCount),
                1,
                MINUTES);
    }

    private List<Map<String, Object>> listMatchingConnection(
            TestConnector connector, String username, boolean expectAuthenticated) {
        List<Map<String, Object>> matchingRecords = new ArrayList<>();
        try (Transaction transaction = db.beginTx()) {
            Result result = transaction.execute("CALL dbms.listConnections()");
            assertEquals(LIST_CONNECTIONS_PROCEDURE_COLUMNS, result.columns());
            List<Map<String, Object>> records = result.stream().toList();

            for (Map<String, Object> record : records) {
                String actualConnector = record.get("connector").toString();
                assertNotNull(actualConnector);
                Object actualUsername = record.get("username");
                if (Objects.equals(connector.name, actualConnector) && Objects.equals(username, actualUsername)) {
                    if (expectAuthenticated) {
                        assertEquals(connector.userAgent, record.get("userAgent"));
                    }

                    matchingRecords.add(record);
                }

                assertThat(record.get("connectionId").toString()).startsWith(actualConnector);
                OffsetDateTime connectTime =
                        ISO_OFFSET_DATE_TIME.parse(record.get("connectTime").toString(), OffsetDateTime::from);
                assertNotNull(connectTime);
                assertThat(record.get("serverAddress")).isInstanceOf(String.class);
                assertThat(record.get("clientAddress")).isInstanceOf(String.class);
            }
            transaction.commit();
        }
        return matchingRecords;
    }

    private List<TrackedNetworkConnection> authenticatedConnectionsFromConnectionTracker() {
        return acceptedConnectionsFromConnectionTracker().stream()
                .filter(connection -> connection.username() != null)
                .toList();
    }

    private List<TrackedNetworkConnection> acceptedConnectionsFromConnectionTracker() {
        NetworkConnectionTracker connectionTracker =
                db.getDependencyResolver().resolveDependency(NetworkConnectionTracker.class);
        return connectionTracker.activeConnections();
    }

    private void changeDefaultPasswordForUserNeo4j(String newPassword) {
        var uri = neo4j.httpURI().resolve("db/system/tx/commit").toString();
        Response response = withBasicAuth("neo4j", "neo4j")
                .POST(uri, query(String.format("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '%s'", newPassword)));

        assertEquals(200, response.status());
    }

    private void createNewUser(String username, String password) {
        var uri = neo4j.httpURI().resolve("db/system/tx/commit").toString();

        Response response1 = withBasicAuth("neo4j", NEO4J_USER_PWD)
                .POST(uri, query("CREATE USER " + username + " SET PASSWORD '" + password + "' CHANGE NOT REQUIRED"));
        assertEquals(200, response1.status());

        Response response2 = withBasicAuth("neo4j", NEO4J_USER_PWD).POST(uri, query("GRANT ROLE admin TO " + username));
        assertEquals(200, response2.status());
    }

    private long createDummyNode() {
        try (Transaction transaction = db.beginTx()) {
            long id;
            try (Result result = transaction.execute("CREATE (n:Dummy) RETURN id(n) AS i")) {
                Map<String, Object> record = single(result);
                id = (long) record.get("i");
            }
            transaction.commit();
            return id;
        }
    }

    private void lockNodeAndExecute(long id, ThrowingAction<Exception> action) throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeById(id);
            Lock lock = tx.acquireWriteLock(node);
            try {
                action.apply();
            } finally {
                lock.release();
            }
            tx.rollback();
        }
    }

    private Future<HttpResponse<String>> updateNodeViaHttp(
            long id, String username, String password, HttpClient.Version httpVersion) {
        return updateNodeViaHttp(id, false, username, password, httpVersion);
    }

    private Future<HttpResponse<String>> updateNodeViaHttps(
            long id, String username, String password, HttpClient.Version httpVersion) {
        return updateNodeViaHttp(id, true, username, password, httpVersion);
    }

    private Future<HttpResponse<String>> updateNodeViaHttp(
            long id, boolean encrypted, String username, String password, HttpClient.Version version) {
        URI uri = txCommitUri(encrypted);
        String userAgent = encrypted ? HTTPS.userAgent : HTTP.userAgent;

        return executor.submit(() -> {
            var httpClient = newClient(version);
            httpClients.add(httpClient);

            var httpRequest = HttpRequest.newBuilder(uri)
                    .header(USER_AGENT, userAgent)
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, basicAuthHeader(username, password))
                    .POST(BodyPublishers.ofString(
                            query("MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42")
                                    .get(),
                            UTF_8))
                    .build();

            return httpClient.send(httpRequest, BodyHandlers.ofString());
        });
    }

    private Future<Void> updateNodeViaBolt(long id, String username, String password) {
        return executor.submit(() -> {
            connectSocketTo(neo4j.boltURI())
                    .sendDefaultProtocolVersion()
                    .send(wire.hello(x -> x.withBasicAuth(username, password)))
                    .send(wire.run("MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42"))
                    .send(wire.pull());

            return null;
        });
    }

    private void killAcceptedConnectionViaBolt() throws Exception {
        for (TrackedNetworkConnection connection : acceptedConnectionsFromConnectionTracker()) {
            killConnectionViaBolt(connection);
        }
    }

    private void killConnectionViaBolt(TrackedNetworkConnection trackedConnection) throws Exception {
        var id = trackedConnection.id();
        var user = trackedConnection.username();

        try (var connection = connectSocketTo(neo4j.boltURI())) {
            connection
                    .sendDefaultProtocolVersion()
                    .send(wire.hello(x -> x.withBasicAuth("neo4j", NEO4J_USER_PWD)))
                    .send(wire.run("CALL dbms.killConnection('" + id + "')"))
                    .send(wire.pull());

            assertThat(connection)
                    .negotiatesDefaultVersion()
                    .receivesSuccess(2)
                    .receivesRecord(fields -> assertThat(fields)
                            .hasSize(3)
                            .satisfies(element -> assertThat(element).isNotNull(), atIndex(0))
                            .contains(stringOrNoValue(user), atIndex(1))
                            .contains(stringValue("Connection found"), atIndex(2)))
                    .receivesSuccess();
        }
    }

    private static void assertConnectionBreaks(TransportConnection connection) throws TimeoutException {
        Predicates.await(() -> connectionIsBroken(connection), 1, MINUTES);
    }

    private static boolean connectionIsBroken(TransportConnection connection) {
        try {
            connection.sendRaw(new byte[] {1});
            connection.receive(1);
            return false;
        } catch (SocketException e) {
            return true;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void terminateAllTransactions() {
        KernelTransactions kernelTransactions = db.getDependencyResolver().resolveDependency(KernelTransactions.class);
        kernelTransactions.activeTransactions().forEach(h -> h.markForTermination(Terminated));
    }

    private URI txCommitUri(boolean encrypted) {
        URI baseUri = encrypted ? neo4j.httpsURI() : neo4j.httpURI();
        return baseUri.resolve("db/neo4j/tx/commit");
    }

    private static RawPayload query(String statement) {
        return rawPayload("{\"statements\":[{\"statement\":\"" + statement + "\"}]}");
    }

    enum TestConnector {
        HTTP("http", "http-user-agent", HttpClient.Version.HTTP_1_1),
        HTTPS("https", "https-user-agent", HttpClient.Version.HTTP_1_1),
        HTTP2("http2", "http2-user-agent", HttpClient.Version.HTTP_2),
        HTTP2S("https2", "http2s-user-agent", HttpClient.Version.HTTP_2),
        BOLT("bolt", "BoltDefaultWire/0.0", null);

        final String name;
        final String userAgent;
        final HttpClient.Version httpVersion;

        TestConnector(String name, String userAgent, HttpClient.Version httpVersion) {
            this.name = name;
            this.userAgent = userAgent;
            this.httpVersion = httpVersion;
        }
    }
}
