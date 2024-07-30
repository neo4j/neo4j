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
package org.neo4j.queryapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.server.queryapi.response.format.Fieldnames.BOOKMARKS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.kernel.database.Database;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void beforeAll() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(BoltConnectorInternalSettings.local_channel_address, QueryResourceIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    @Test
    void shouldExecuteSimpleQuery() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).asInt())
                .isEqualTo(1);
    }

    @Test
    void shouldReturnMultipleRecords() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"UNWIND [1,2] as i RETURN i\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).asInt())
                .isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(1).get(0).asInt())
                .isEqualTo(2);
    }

    @Test
    void shouldReturnMultipleRecordMultiFields() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"UNWIND [1,2] as i RETURN i, 'bob'\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(2);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).asInt())
                .isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(1).asText())
                .isEqualTo("bob");
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(1).get(0).asInt())
                .isEqualTo(2);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(1).get(1).asText())
                .isEqualTo("bob");
    }

    @Test
    void shouldReturnBookmarks() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(client, queryEndpoint);

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(BOOKMARKS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(BOOKMARKS_KEY).get(0).asText()).isNotBlank();
    }

    @Test
    void shouldReturnUpdatedBookmark() throws IOException, InterruptedException {
        var responseA = QueryApiTestUtil.simpleRequest(client, queryEndpoint);

        assertThat(responseA.statusCode()).isEqualTo(202);
        var parsedJsonA = MAPPER.readTree(responseA.body());

        var initialBookmark = parsedJsonA.get(BOOKMARKS_KEY).get(0).asText();

        var responseB = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"CREATE (n)\", \"bookmarks\" : [\"" + initialBookmark + "\"]}");

        assertThat(responseB.statusCode()).isEqualTo(202);
        var parsedJsonB = MAPPER.readTree(responseB.body());

        assertThat(parsedJsonB.get(BOOKMARKS_KEY).get(0).asText()).isNotBlank();
        assertThat(parsedJsonB.get(BOOKMARKS_KEY).get(0).asText()).isNotEqualTo(initialBookmark);
    }

    @Test
    void shouldAcceptBookmarksAsInput() throws IOException, InterruptedException {
        var responseA = QueryApiTestUtil.simpleRequest(client, queryEndpoint);

        assertThat(responseA.statusCode()).isEqualTo(202);
        var parsedJsonA = MAPPER.readTree(responseA.body());

        assertThat(parsedJsonA.get(BOOKMARKS_KEY).size()).isEqualTo(1);
        assertThat(parsedJsonA.get(BOOKMARKS_KEY).get(0).asText()).isNotBlank();

        var responseB = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN 1\", \"bookmarks\" : [\""
                        + parsedJsonA.get(BOOKMARKS_KEY).get(0).asText() + "\"]}");

        assertThat(responseB.statusCode()).isEqualTo(202);
        var parsedJsonB = MAPPER.readTree(responseB.body());

        assertThat(parsedJsonB.get(BOOKMARKS_KEY).size()).isEqualTo(1);
        assertThat(parsedJsonB.get(BOOKMARKS_KEY).get(0).asText()).isNotBlank();
    }

    @Test
    void shouldAcceptMultipleBookmarksAsInput() throws IOException, InterruptedException {
        var responseA = QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"CREATE (n)\"}");
        assertThat(responseA.statusCode()).isEqualTo(202);

        var responseB = QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"CREATE (n)\"}");
        assertThat(responseA.statusCode()).isEqualTo(202);

        var bookmarkA =
                MAPPER.readTree(responseA.body()).get(BOOKMARKS_KEY).get(0).asText();
        var bookmarkB =
                MAPPER.readTree(responseB.body()).get(BOOKMARKS_KEY).get(0).asText();

        var combinedBmResponse = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"CREATE (n)\", \"bookmarks\" : [\"" + bookmarkA + "\",\"" + bookmarkB + "\"]}");

        assertThat(combinedBmResponse.statusCode()).isEqualTo(202);
        var combineBmJson = MAPPER.readTree(combinedBmResponse.body());
        var combinedBookmark = combineBmJson.get(BOOKMARKS_KEY).get(0).asText();

        assertThat(combineBmJson.get(BOOKMARKS_KEY).size()).isEqualTo(1);
        assertThat(combinedBookmark).isNotBlank();
        assertThat(combinedBookmark).isNotEqualTo(bookmarkA);
        assertThat(combinedBookmark).isNotEqualTo(bookmarkB);
    }

    @Test
    void shouldTimeoutWaitingForUnreachableBookmark() throws IOException, InterruptedException {
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        QueryApiTestUtil.resolveDependency(dbms, Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        QueryApiTestUtil.getLastClosedTransactionId(dbms) + 1)),
                List.of()));

        var response = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN 1\",  \"bookmarks\" : [\"" + expectedBookmark + "\"]}");
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(response.statusCode()).isEqualTo(400);

        assertThat(parsedJson.get(ERRORS_KEY).get(0).get("code").asText())
                .isEqualTo("Neo.TransientError.Transaction.BookmarkTimeout");
    }

    @Test
    void shouldWaitForUpdatedBookmark() throws IOException, InterruptedException {
        var lastTxId = QueryApiTestUtil.getLastClosedTransactionId(dbms);
        var nextTxId = lastTxId + 1;
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        QueryApiTestUtil.resolveDependency(dbms, Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        nextTxId)),
                List.of()));

        var responseA = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"RETURN 1\", \"bookmarks\" : [\"" + expectedBookmark + "\"]}");

        // initial request times out
        assertThat(responseA.statusCode()).isEqualTo(400);
        assertThat(responseA.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.TransientError.Transaction.BookmarkTimeout\","
                        + "\"mes"
                        + "sage\":\"Database 'neo4j' not up to the requested version: " + nextTxId
                        + ". Latest database version is " + lastTxId + "\"}]}");

        var createNodeRequest =
                QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"CREATE (n)\"}");
        assertThat(createNodeRequest.statusCode()).isEqualTo(202);

        var responseB = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"RETURN 1\", \"bookmarks\" : [\"" + expectedBookmark + "\"]}");
        var parsedJson = MAPPER.readTree(responseB.body());

        assertThat(responseB.statusCode()).isEqualTo(202);
        assertThat(parsedJson.get(BOOKMARKS_KEY).get(0).asText()).isEqualTo(expectedBookmark);
    }

    @Test
    void callInTransactions() throws Exception {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"UNWIND [4, 2, 1, 0] AS i"
                        + " CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).asInt())
                .isEqualTo(4);
    }
}
