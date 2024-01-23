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
package org.neo4j.server.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.configuration.ServerSettings.http_enabled_transports;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

public class HttpTransportIT extends ExclusiveWebContainerTestBase {

    @Test
    public void shouldFailToStartIfNoTransportConfigured() {
        assertThatExceptionOfType(UnsatisfiedDependencyException.class).isThrownBy(() -> serverOnRandomPorts()
                .withProperty(http_enabled_transports.name(), "")
                .build());
    }

    @ParameterizedTest
    @MethodSource("httpVersions")
    void shouldHandleHttpVersions(HttpClient.Version httpVersion) throws Exception {
        var testWebContainer = serverOnRandomPorts()
                .persistent()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_enabled_transports.name(), "HTTP1_1,HTTP2")
                .build();

        // note: jdk client's http/2 is by upgrade mechanism
        var httpClient = HttpClient.newBuilder()
                .version(httpVersion)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var req = HttpRequest.newBuilder()
                .uri(URI.create(testWebContainer.getBaseUri().toString() + txEndpoint()))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        var response = httpClient.send(req, HttpResponse.BodyHandlers.discarding());

        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(response.version()).isEqualTo(httpVersion);

        testWebContainer.shutdown();
    }

    @Test
    public void shouldServeHttp11WhenHttp2Disabled() throws Exception {
        var testWebContainer = serverOnRandomPorts()
                .persistent()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_enabled_transports.name(), "HTTP1_1")
                .build();

        // note: jdk client's http/2 is by upgrade mechanism
        var httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var req = HttpRequest.newBuilder()
                .uri(URI.create(testWebContainer.getBaseUri().toString()))
                .GET()
                .build();

        var response = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.version()).isEqualTo(HttpClient.Version.HTTP_1_1);

        testWebContainer.shutdown();
    }

    @Test
    public void shouldFailIfNoCommonTransport() throws Exception {
        var testWebContainer = serverOnRandomPorts()
                .persistent()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString())
                .withProperty(ServerSettings.http_enabled_transports.name(), "HTTP2")
                .build();

        // ensure client only uses http1
        var httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var req = HttpRequest.newBuilder()
                .uri(URI.create(testWebContainer.getBaseUri().toString()))
                .GET()
                .build();

        assertThrows(IOException.class, () -> httpClient.send(req, HttpResponse.BodyHandlers.ofString()));

        testWebContainer.shutdown();
    }

    private static Stream<HttpClient.Version> httpVersions() {
        return Stream.of(HttpClient.Version.HTTP_1_1, HttpClient.Version.HTTP_2);
    }
}
