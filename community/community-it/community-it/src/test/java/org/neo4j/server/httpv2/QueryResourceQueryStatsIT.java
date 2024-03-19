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
package org.neo4j.server.httpv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.httpv2.HttpV2ClientUtil.baseRequestBuilder;
import static org.neo4j.server.httpv2.HttpV2ClientUtil.resolveDependency;
import static org.neo4j.server.httpv2.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.QUERY_STATS_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
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
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceQueryStatsIT {

    private static DatabaseManagementService database;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    public static void beforeAll() {
        var builder = new TestDatabaseManagementServiceBuilder();
        database = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceQueryStatsIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = resolveDependency(database, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterAll
    public static void teardown() {
        database.shutdown();
    }

    @Test
    public void shouldIncludeQueryStats() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN 1\", \"includeQueryStatistics\": true}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var queryStatsMap = parsedJson.get(QUERY_STATS_KEY);

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(queryStatsMap.size()).isEqualTo(14);
        assertThat(queryStatsMap.get("containsUpdates").asBoolean()).isEqualTo(false);
        assertThat(queryStatsMap.get("containsSystemUpdates").asBoolean()).isEqualTo(false);
        assertThat(queryStatsMap.get("nodesCreated").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("nodesDeleted").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("propertiesSet").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("relationshipsCreated").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("relationshipsDeleted").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("labelsAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("labelsRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("indexesAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("indexesRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("constraintsAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("constraintsRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("systemUpdates").asInt()).isEqualTo(0);
    }

    @Test
    public void shouldNotIncludeQueryStats() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN 1\", \"includeQueryStatistics\": false}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(QUERY_STATS_KEY)).isNull();
    }

    @Test
    public void shouldNotIncludeQueryStatsByDefault() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(QUERY_STATS_KEY)).isNull();
    }

    @Test
    public void shouldErrorIfInvalidInput() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN 1\", " + "\"includeQueryStatistics\": \"banana\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"error\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Bad Request\"}]}");
    }
}
