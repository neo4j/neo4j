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
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.PortUtils;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class WebContainerConfigIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @AfterEach
    void stopTheServer() {
        testWebContainer.shutdown();
    }

    @Test
    void shouldRequireAuth() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_auth_allowlist.name(), "")
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), TRUE)
                .build();

        var request =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();
        var response = newHttpClient().send(request, discarding());

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void shouldWhitelist() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_auth_allowlist.name(), "/")
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), TRUE)
                .build();

        var request =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();
        var response = newHttpClient().send(request, discarding());

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void shouldBlacklistPaths() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_paths_blacklist.name(), "/*")
                .build();

        var request =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();
        var response = newHttpClient().send(request, discarding());

        assertThat(response.statusCode()).isEqualTo(403);
    }

    @Test
    void shouldPickUpAddressFromConfig() throws Exception {
        var nonDefaultAddress = new SocketAddress("0.0.0.0", 0);
        testWebContainer = CommunityWebContainerBuilder.builder()
                .onAddress(nonDefaultAddress)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        GraphDatabaseAPI database = testWebContainer.getDefaultDatabase();
        var localHttpAddress = PortUtils.getConnectorAddress(database, ConnectorType.HTTP);
        assertNotEquals(HttpConnector.DEFAULT_PORT, localHttpAddress.getPort());
        assertEquals(nonDefaultAddress.getHostname(), localHttpAddress.getHost());

        var request =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();
        var response = newHttpClient().send(request, discarding());

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void shouldPickupRelativeUrisForDatabaseApi() throws Exception {
        var dbUri = "a/different/db/uri";

        testWebContainer = serverOnRandomPorts()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withRelativeDatabaseApiPath("/" + dbUri)
                .build();

        var uri = testWebContainer.getBaseUri() + dbUri + "/neo4j/tx/commit";
        var txRequest = HttpRequest.newBuilder(URI.create(uri))
                .header(ACCEPT, APPLICATION_JSON)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString("{ 'statements': [ { 'statement': 'CREATE ()' } ] }"))
                .build();
        var txResponse = newHttpClient().send(txRequest, discarding());
        assertEquals(200, txResponse.statusCode());

        var discoveryRequest =
                HttpRequest.newBuilder(testWebContainer.getBaseUri()).GET().build();
        var discoveryResponse = newHttpClient().send(discoveryRequest, ofString());
        assertEquals(200, txResponse.statusCode());
        assertThat(discoveryResponse.body()).contains(dbUri);
    }

    @Test
    void shouldGenerateWADLWhenExplicitlyEnabledInConfig() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .withProperty(ServerSettings.wadl_enabled.name(), TRUE)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var wadlUri = URI.create(testWebContainer.getBaseUri() + "application.wadl");
        var request = HttpRequest.newBuilder(wadlUri)
                .GET()
                .header(CONTENT_TYPE, WILDCARD)
                .build();
        var response = newHttpClient().send(request, ofString());

        assertEquals(200, response.statusCode());
        assertEquals(
                "application/vnd.sun.wadl+xml",
                response.headers().allValues("Content-Type").iterator().next());
        assertThat(response.body()).contains("<application xmlns=\"http://wadl.dev.java.net/2009/02\">");
    }

    @Test
    void shouldNotGenerateWADLWhenNotExplicitlyEnabledInConfig() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var uri = URI.create(testWebContainer.getBaseUri() + "application.wadl");
        var request =
                HttpRequest.newBuilder(uri).GET().header(CONTENT_TYPE, WILDCARD).build();
        var response = newHttpClient().send(request, ofString());

        assertEquals(404, response.statusCode());
    }

    @Test
    void shouldNotGenerateWADLWhenExplicitlyDisabledInConfig() throws Exception {
        testWebContainer = serverOnRandomPorts()
                .withProperty(ServerSettings.wadl_enabled.name(), FALSE)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var warlUri = URI.create(testWebContainer.getBaseUri() + "application.wadl");
        var request = HttpRequest.newBuilder(warlUri)
                .GET()
                .header(CONTENT_TYPE, WILDCARD)
                .build();
        var response = newHttpClient().send(request, ofString());

        assertEquals(404, response.statusCode());
    }
}
