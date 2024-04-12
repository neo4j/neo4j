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

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import org.apache.commons.codec.binary.Base64;
import org.dummy.web.service.DummyThirdPartyWebService;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.WebContainerHelper;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class NeoWebServerJAXRSIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @AfterEach
    void stopServer() {
        if (testWebContainer != null) {
            testWebContainer.shutdown();
        }
    }

    @Test
    void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception {
        var serverBuilder = CommunityWebContainerBuilder.builder();
        testWebContainer = WebContainerHelper.createNonPersistentContainer(serverBuilder);
        var functionalTestHelper = new FunctionalTestHelper(testWebContainer);

        var request =
                HttpRequest.newBuilder(functionalTestHelper.baseUri()).GET().build();
        var httpClient = HttpClient.newBuilder().followRedirects(NORMAL).build();
        var response = httpClient.send(request, discarding());
        assertEquals(200, response.statusCode());
    }

    @Test
    void shouldRequireAuthIfEnabled() throws Exception {
        testWebContainer = CommunityWebContainerBuilder.serverOnRandomPorts()
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), "true")
                .withThirdPartyJaxRsPackage(
                        "org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var httpClient = newBuilder().followRedirects(NORMAL).build();

        var thirdPartyServiceUri = new URI(
                        testWebContainer.getBaseUri() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .normalize();

        var request = HttpRequest.newBuilder(thirdPartyServiceUri).GET().build();
        var response = httpClient.send(request, ofString());
        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void shouldSuccessfullyAuthenticate() throws Exception {
        testWebContainer = CommunityWebContainerBuilder.serverOnRandomPorts()
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), "true")
                .withThirdPartyJaxRsPackage(
                        "org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var httpClient = newBuilder().followRedirects(NORMAL).build();

        var thirdPartyServiceUri = new URI(
                        testWebContainer.getBaseUri() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .normalize();

        var request = HttpRequest.newBuilder(thirdPartyServiceUri)
                .header(HttpHeader.AUTHORIZATION.name(), "Basic " + Base64.encodeBase64String("neo4j:neo4j".getBytes()))
                .GET()
                .build();
        var response = httpClient.send(request, ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void shouldLoadThirdPartyJaxRsClasses() throws Exception {
        testWebContainer = CommunityWebContainerBuilder.serverOnRandomPorts()
                .withThirdPartyJaxRsPackage(
                        "org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .build();

        var httpClient = newBuilder().followRedirects(NORMAL).build();

        var thirdPartyServiceUri = new URI(
                        testWebContainer.getBaseUri() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .normalize();

        var request = HttpRequest.newBuilder(thirdPartyServiceUri).GET().build();
        var response = httpClient.send(request, ofString()).body();
        assertEquals("hello", response);

        // Assert that extensions gets initialized
        var nodesCreated = createSimpleDatabase(testWebContainer.getDefaultDatabase());
        thirdPartyServiceUri = new URI(testWebContainer.getBaseUri()
                        + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT + "/inject-test")
                .normalize();
        request = HttpRequest.newBuilder(thirdPartyServiceUri).GET().build();
        response = httpClient.send(request, ofString()).body();
        assertEquals(String.valueOf(nodesCreated), response, response);
    }

    private static int createSimpleDatabase(final GraphDatabaseAPI graph) {
        final var numberOfNodes = 10;
        new Transactor(graph, tx -> {
                    for (var i = 0; i < numberOfNodes; i++) {
                        tx.createNode();
                    }

                    try (ResourceIterable<Node> allNodes1 = tx.getAllNodes()) {
                        for (var node1 : allNodes1) {
                            try (ResourceIterable<Node> allNodes2 = tx.getAllNodes()) {
                                for (var node2 : allNodes2) {
                                    if (node1.equals(node2)) {
                                        continue;
                                    }

                                    node1.createRelationshipTo(node2, RelationshipType.withName("REL"));
                                }
                            }
                        }
                    }
                })
                .execute();

        return numberOfNodes;
    }
}
