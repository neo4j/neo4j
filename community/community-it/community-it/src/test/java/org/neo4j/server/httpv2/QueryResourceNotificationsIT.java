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
import static org.neo4j.server.httpv2.response.format.Fieldnames.NOTIFICATIONS_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.VALUES_KEY;

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
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceNotificationsIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void beforeAll() {
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceNotificationsIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    @Test
    void shouldReturnLabelDoesNotExistNotification() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"MATCH (n:thisLabelDoesNotExist) return n\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).size()).isEqualTo(0);

        var notificationsJson = parsedJson.get(NOTIFICATIONS_KEY);

        assertThat(notificationsJson.size()).isEqualTo(1);
        assertThat(notificationsJson.get(0).get("code").asText())
                .isEqualTo(NotificationCodeWithDescription.MISSING_LABEL
                        .getStatus()
                        .code()
                        .serialize());
        assertThat(notificationsJson.get(0).get("title").asText())
                .isEqualTo(NotificationCodeWithDescription.MISSING_LABEL
                        .getStatus()
                        .code()
                        .description());
        assertThat(notificationsJson.get(0).get("description").asText())
                .isEqualTo(String.format(
                        NotificationCodeWithDescription.MISSING_LABEL.getDescription(),
                        "the missing label name is: thisLabelDoesNotExist"));
        assertThat(notificationsJson.get(0).get("position").get("offset").asInt())
                .isEqualTo(9);
        assertThat(notificationsJson.get(0).get("position").get("line").asInt()).isEqualTo(1);
        assertThat(notificationsJson.get(0).get("position").get("column").asInt())
                .isEqualTo(10);
        assertThat(notificationsJson.get(0).get("severity").asText()).isEqualTo("WARNING");
    }

    @Test
    void shouldReturnMultipleNotifications() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"MATCH (n:thisLabelDoesNotExist), "
                        + "(m:thisLabelDoesNotExist) return m, n\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(2);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).size()).isEqualTo(0);

        var notificationsJson = parsedJson.get(NOTIFICATIONS_KEY);

        assertThat(notificationsJson.get(0).get("code").asText())
                .isEqualTo(Status.Statement.CartesianProduct.code().serialize());
        assertThat(notificationsJson.get(1).get("code").asText())
                .isEqualTo(Status.Statement.UnknownLabelWarning.code().serialize());
        assertThat(notificationsJson.get(2).get("code").asText())
                .isEqualTo(Status.Statement.UnknownLabelWarning.code().serialize());
    }

    @Test
    void shouldNotReturnNotificationsIfNonePresent() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(NOTIFICATIONS_KEY)).isNull();
    }
}
