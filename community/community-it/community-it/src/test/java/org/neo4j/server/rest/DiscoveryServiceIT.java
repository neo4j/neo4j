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

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

public class DiscoveryServiceIT extends AbstractRestFunctionalTestBase {
    @Test
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument() throws Exception {
        var response = requestDiscovery();
        assertEquals(200, response.statusCode());
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument() throws Exception {
        var response = requestDiscovery();
        assertTrue(response.headers().firstValue(CONTENT_LENGTH).isPresent());
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument() throws Exception {
        var response = requestDiscovery();
        assertThat(response.headers().firstValue(CONTENT_TYPE).orElseThrow()).contains(APPLICATION_JSON);
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception {
        var response = requestDiscovery();

        assertJsonResponseBody(response);
    }

    @Test
    public void shouldFigureOutMatchingFormatFromVariousAcceptHeaders() throws Exception {

        var request = HttpRequest.newBuilder(container().getBaseUri())
                .header(ACCEPT, "application/vnd.neo4j.jolt+json-seq; q=1.0")
                .header(ACCEPT, "application/json; q=0.9")
                .header(ACCEPT, "text/html; q=0.0")
                .GET()
                .build();
        var httpClient = HttpClient.newBuilder().followRedirects(NEVER).build();
        var response = httpClient.send(request, ofString());

        assertEquals(200, response.statusCode());
        assertJsonResponseBody(response);
        assertThat(response.headers().firstValue(HttpHeaders.VARY).orElseThrow())
                .isEqualTo(ACCEPT);
    }

    @Test
    public void shouldNotAcceptUnacceptableThings() throws Exception {

        var request = HttpRequest.newBuilder(container().getBaseUri())
                .header(ACCEPT, MediaType.TEXT_PLAIN)
                .GET()
                .build();
        var httpClient = HttpClient.newBuilder().followRedirects(NEVER).build();
        var response = httpClient.send(request, discarding());
        assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.statusCode());
    }

    private void assertJsonResponseBody(HttpResponse<String> response) throws JsonParseException {
        var responseBodyMap = JsonHelper.jsonToMap(response.body());

        var managementKey = "management";
        assertFalse(responseBodyMap.containsKey(managementKey));

        var transactionKey = "transaction";
        assertTrue(responseBodyMap.containsKey(transactionKey));
        assertNotNull(responseBodyMap.get(transactionKey));

        var boltDirectKey = "bolt_direct";
        assertTrue(responseBodyMap.containsKey(boltDirectKey));
        assertNotNull(responseBodyMap.get(boltDirectKey));

        var boltRoutingKey = "bolt_routing";
        assertTrue(responseBodyMap.containsKey(boltRoutingKey));
        assertNotNull(responseBodyMap.get(boltRoutingKey));

        var serverVersionKey = "neo4j_version";
        assertTrue(responseBodyMap.containsKey(serverVersionKey));
        assertNotNull(responseBodyMap.get(serverVersionKey));

        var serverEditionKey = "neo4j_edition";
        assertTrue(responseBodyMap.containsKey(serverEditionKey));
        assertThat(responseBodyMap.get(serverEditionKey)).isEqualTo("community");
    }

    @Test
    public void shouldRedirectOnHtmlRequest() throws Exception {
        var request = HttpRequest.newBuilder(container().getBaseUri())
                .header(ACCEPT, TEXT_HTML)
                .GET()
                .build();
        var httpClient = HttpClient.newBuilder().followRedirects(NEVER).build();
        var response = httpClient.send(request, discarding());

        assertEquals(303, response.statusCode());
        assertThat(response.headers().firstValue(HttpHeaders.VARY).orElseThrow())
                .isEqualTo(ACCEPT);
    }

    private static HttpResponse<String> requestDiscovery() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(container().getBaseUri()).GET().build();
        return newHttpClient().send(request, ofString());
    }
}
