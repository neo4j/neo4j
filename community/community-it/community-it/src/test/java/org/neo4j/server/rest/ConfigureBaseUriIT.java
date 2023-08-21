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
package org.neo4j.server.rest;

import static java.lang.Integer.parseInt;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.server.helpers.FunctionalTestHelper;

public class ConfigureBaseUriIT extends AbstractRestFunctionalTestBase {
    private static FunctionalTestHelper functionalTestHelper;
    private static HttpClient httpClient;

    @BeforeAll
    public static void setupServer() {
        functionalTestHelper = new FunctionalTestHelper(container());
        httpClient = newHttpClient();
    }

    @Test
    public void shouldForwardHttpAndHost() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .header("X-Forwarded-Host", "foobar.com")
                .header("X-Forwarded-Proto", "http")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("http://foobar.com");
        assertThat(responseBodyString).doesNotContain("http://localhost");
    }

    @Test
    public void shouldForwardHttpsAndHost() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .header("X-Forwarded-Host", "foobar.com")
                .header("X-Forwarded-Proto", "https")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("https://foobar.com");
        assertThat(responseBodyString).doesNotContain("https://localhost");
    }

    @Test
    public void shouldForwardHttpAndHostOnDifferentPort() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .header("X-Forwarded-Host", "foobar.com:9999")
                .header("X-Forwarded-Proto", "http")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("http://foobar.com:9999");
        assertThat(responseBodyString).doesNotContain("http://localhost");
    }

    @Test
    public void shouldForwardHttpAndFirstHost() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .header("X-Forwarded-Host", "foobar.com, bazbar.com")
                .header("X-Forwarded-Proto", "http")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("http://foobar.com");
        assertThat(responseBodyString).doesNotContain("http://localhost");
    }

    @Test
    public void shouldForwardHttpsAndHostOnDifferentPort() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .header("X-Forwarded-Host", "foobar.com:9999")
                .header("X-Forwarded-Proto", "https")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("https://foobar.com:9999");
        assertThat(responseBodyString).doesNotContain("https://localhost");
    }

    @Test
    public void shouldUseRequestUriWhenNoXForwardHeadersPresent() throws Exception {
        var request = HttpRequest.newBuilder(functionalTestHelper.baseUri())
                .GET()
                .header("Accept", "application/json")
                .build();

        var response = httpClient.send(request, ofByteArray());

        verifyContentLength(response);

        var responseBodyString = new String(response.body());
        assertThat(responseBodyString).contains("http://localhost");
        assertThat(responseBodyString).doesNotContain("https://foobar.com");
        assertThat(responseBodyString).doesNotContain(":0");
    }

    private static void verifyContentLength(HttpResponse<byte[]> response) {
        var contentLengthValue = response.headers().firstValue("CONTENT-LENGTH");
        assertTrue(contentLengthValue.isPresent());
        assertEquals(parseInt(contentLengthValue.get()), response.body().length);
    }
}
