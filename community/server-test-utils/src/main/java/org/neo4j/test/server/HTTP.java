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
package org.neo4j.test.server;

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.server.rest.dbms.AuthorizationHeaders;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

/**
 * A tool for performing REST HTTP requests
 */
public final class HTTP {
    private static final Builder BUILDER = new Builder();
    private static final HttpClient CLIENT = newClient(HttpClient.Version.HTTP_1_1);

    private HTTP() {}

    public static String basicAuthHeader(String username, String password) {
        var usernamePassword = username + ':' + password;
        return AuthorizationHeaders.Scheme.BASIC + " "
                + Base64.getEncoder().encodeToString(usernamePassword.getBytes());
    }

    public static String bearerAuthHeader(String token) {
        return AuthorizationHeaders.Scheme.BEARER + " " + token;
    }

    public static Builder withBasicAuth(String username, String password) {
        return withHeaders(AUTHORIZATION, basicAuthHeader(username, password));
    }

    public static Builder withHeaders(String... kvPairs) {
        return BUILDER.withHeaders(kvPairs);
    }

    public static Builder withBaseUri(URI baseUri) {
        return BUILDER.withBaseUri(baseUri.toString());
    }

    public static Response POST(String uri) {
        return BUILDER.POST(uri);
    }

    public static Response POST(String uri, Object payload) {
        return BUILDER.POST(uri, payload);
    }

    public static Response POST(String uri, RawPayload payload) {
        return BUILDER.POST(uri, payload);
    }

    public static Response GET(String uri) {
        return BUILDER.GET(uri);
    }

    public static Response request(String method, String uri, Object payload) {
        return BUILDER.request(method, uri, payload);
    }

    public static HttpClient newClient(HttpClient.Version httpVersion) {
        try {
            var sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new InsecureTrustManager()}, null);

            return HttpClient.newBuilder()
                    .followRedirects(NEVER)
                    .version(httpVersion)
                    .sslContext(sslContext)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder {
        private final Map<String, String> headers;
        private final String baseUri;

        private Builder() {
            this(Collections.emptyMap(), "");
        }

        private Builder(Map<String, String> headers, String baseUri) {
            this.baseUri = baseUri;
            this.headers = unmodifiableMap(headers);
        }

        public Builder withHeaders(String... kvPairs) {
            return withHeaders(stringMap(kvPairs));
        }

        public Builder withHeaders(Map<String, String> newHeaders) {
            var combinedHeaders = new HashMap<String, String>();
            combinedHeaders.putAll(headers);
            combinedHeaders.putAll(newHeaders);
            return new Builder(combinedHeaders, baseUri);
        }

        public Builder withBaseUri(String baseUri) {
            return new Builder(headers, baseUri);
        }

        public Builder withAppendedUri(String suffix) {
            return new Builder(headers, baseUri + suffix);
        }

        public Response POST(String uri) {
            return request("POST", uri);
        }

        public Response POST(String uri, Object payload) {
            return request("POST", uri, payload);
        }

        public Response POST(String uri, RawPayload payload) {
            return request("POST", uri, payload);
        }

        public Response DELETE(String uri) {
            return request("DELETE", uri);
        }

        public Response GET(String uri) {
            return request("GET", uri);
        }

        public Response request(String method, String uri) {
            var request =
                    requestBuilder(uri).method(method, BodyPublishers.noBody()).build();
            var response = send(request);
            return new Response(response);
        }

        public Response request(String method, String uri, Object payload) {
            return request(method, uri, payload, emptyMap());
        }

        public Response request(String method, String uri, Object payload, Map<String, String> headers) {
            if (payload == null) {
                return request(method, uri);
            }
            var jsonPayload = payload instanceof RawPayload ? ((RawPayload) payload).get() : createJsonFrom(payload);

            var requestBuilder = requestBuilder(uri)
                    .method(method, BodyPublishers.ofString(jsonPayload))
                    .setHeader(CONTENT_TYPE, APPLICATION_JSON);

            headers.forEach(requestBuilder::setHeader);

            var response = send(requestBuilder.build());

            return new Response(response);
        }

        private URI buildUri(String uri) {
            var unprefixedUri = URI.create(uri);
            if (unprefixedUri.isAbsolute()) {
                return unprefixedUri;
            } else {
                return URI.create(baseUri + uri);
            }
        }

        private HttpRequest.Builder requestBuilder(String uri) {
            var builder = HttpRequest.newBuilder(buildUri(uri));
            for (var headerEntry : headers.entrySet()) {
                builder = builder.setHeader(headerEntry.getKey(), headerEntry.getValue());
            }

            return builder;
        }

        private static HttpResponse<String> send(HttpRequest request) {
            try {
                return getStringHttpResponse(request);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("HTTP/1.1 header parser received no bytes")) {
                    // Retry once to avoid flakiness
                    try {
                        return getStringHttpResponse(request);
                    } catch (Exception e2) {
                        throw new RuntimeException(e2);
                    }
                }
                throw new RuntimeException(e);
            }
        }

        private static HttpResponse<String> getStringHttpResponse(HttpRequest request)
                throws InterruptedException, java.util.concurrent.ExecutionException,
                        java.util.concurrent.TimeoutException {
            return CLIENT.sendAsync(request, BodyHandlers.ofString()).get(4, TimeUnit.MINUTES);
        }
    }

    /**
     * Check some general validations that all REST responses should always pass.
     */
    private static HttpResponse<String> sanityCheck(HttpResponse<String> response) {
        var contentEncodings = response.headers().allValues(CONTENT_ENCODING);
        String contentEncoding;
        if (contentEncodings != null && (contentEncoding = Iterables.singleOrNull(contentEncodings)) != null) {
            // Specifically, this is never used for character encoding.
            contentEncoding = contentEncoding.toLowerCase();
            assertThat(contentEncoding).satisfiesAnyOf(s -> assertThat(s).contains("gzip"), s -> assertThat(s)
                    .contains("deflate"));
            assertThat(contentEncoding).doesNotContain("utf-8");
        }
        return response;
    }

    public static class Response {
        private final HttpResponse<String> response;
        private final String entity;

        public Response(HttpResponse<String> response) {
            this.response = sanityCheck(response);
            if (response.statusCode() == NO_CONTENT.getStatusCode()) {
                entity = "";
            } else {
                this.entity = response.body();
            }
        }

        public int status() {
            return response.statusCode();
        }

        public String location() {
            return response.headers()
                    .firstValue(LOCATION)
                    .orElseThrow(() -> new RuntimeException("The request did not contain a location header.\n" + this));
        }

        @SuppressWarnings("unchecked")
        public <T> T content() {
            try {
                return (T) JsonHelper.readJson(entity);
            } catch (JsonParseException e) {
                throw new RuntimeException("Unable to deserialize: " + entity, e);
            }
        }

        public String rawContent() {
            return entity;
        }

        public String stringFromContent(String key) throws JsonParseException {
            return get(key).asText();
        }

        public JsonNode get(String fieldName) throws JsonParseException {
            return JsonHelper.jsonNode(entity).get(fieldName);
        }

        public String header(String name) {
            return response.headers().firstValue(name).orElse(null);
        }

        @Override
        public String toString() {
            var sb = new StringBuilder();
            sb.append("HTTP ").append(response.statusCode()).append("\n");
            for (var headerEntry : response.headers().map().entrySet()) {
                for (var headerValue : headerEntry.getValue()) {
                    sb.append(headerEntry.getKey())
                            .append(": ")
                            .append(headerValue)
                            .append("\n");
                }
            }
            sb.append("\n");
            sb.append(entity).append("\n");

            return sb.toString();
        }
    }

    public static class RawPayload {
        private final String payload;

        public static RawPayload rawPayload(String payload) {
            return new RawPayload(payload);
        }

        public static RawPayload quotedJson(String json) {
            return new RawPayload(json.replace("'", "\""));
        }

        private RawPayload(String payload) {
            this.payload = payload;
        }

        public String get() {
            return payload;
        }
    }
}
