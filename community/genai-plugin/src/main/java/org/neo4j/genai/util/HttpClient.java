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
package org.neo4j.genai.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.genai.vector.MalformedGenAIResponseException;
import org.neo4j.genai.vector.VectorEncoding;

public class HttpClient implements Client {
    private static final String USER_AGENT = "Neo4j-GenAIProcedures/" + VectorEncoding.VERSION;

    public Multimap<String, String> prepareRequestProperties(String host, Multimap<String, String> properties) {
        final var requestProperties = Multimaps.mutable.list.with("Host", host, "User-Agent", USER_AGENT);
        requestProperties.putAll(properties);
        return requestProperties.toImmutable();
    }

    @Override
    public InputStream sendRequest(URI uri, Multimap<String, String> properties, Payload payload)
            throws IOException, GenAIProcedureException {
        return sendRequest(openConnection(uri, prepareRequestProperties(uri.getHost(), properties)), payload);
    }

    InputStream sendRequest(HttpURLConnection connection, Payload payload) throws IOException, GenAIProcedureException {
        connection.setDoOutput(true);
        try (final var writer =
                new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8))) {
            payload.writeTo(writer);
        }

        final int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
            throw new GenAIProcedureException("Not authorized to make API request; check your credentials.");
        }

        if (responseCode != HttpURLConnection.HTTP_OK) {
            final var responseMessage = connection.getResponseMessage();
            final var responseBody = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new MalformedGenAIResponseException(
                    "Unexpected HTTP response code: %d %s - %s".formatted(responseCode, responseMessage, responseBody));
        }

        final var responseStream = connection.getInputStream();
        final var contentLength = connection.getContentLengthLong();
        return contentLength > 0 ? new LimitedInputStream(responseStream, contentLength) : responseStream;
    }

    private HttpURLConnection openConnection(URI uri, Multimap<String, String> properties) throws IOException {
        if (!(uri.toURL().openConnection() instanceof final HttpURLConnection connection)) {
            throw new IllegalArgumentException("Not a HTTP(S) URI: %s".formatted(uri));
        }

        connection.setRequestMethod("POST");
        connection.setInstanceFollowRedirects(true);
        properties.forEachKeyValue(connection::setRequestProperty);
        return connection;
    }
}
