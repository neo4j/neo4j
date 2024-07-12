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
package org.neo4j.genai.vector.providers.openai;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.genai.util.Client.Payload;
import org.neo4j.genai.util.HttpClient;
import org.neo4j.genai.util.JsonResponseParser;
import org.neo4j.genai.vector.MalformedGenAIResponseException;
import org.neo4j.genai.vector.VectorEncoding.BatchRow;
import org.neo4j.genai.vector.VectorEncoding.Provider;
import org.neo4j.util.VisibleForTesting;

public abstract class OpenAIBasedEncoder implements Provider.Encoder {
    private static final String ENCODING_FORMAT = "float";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpClient client;
    private final URI endpoint;
    private final String providerName;
    private final OptionalLong dimensions;

    /**
     * Extend the headers with provider-specific fields.
     * @param headers mutable base headers
     */
    protected void extendHeaders(MutableMultimap<String, String> headers) {}

    /**
     * Extend the payload with provider-specific fields.
     * @param payload mutable base payload
     */
    protected void extendPayload(MutableMap<String, Object> payload) {}

    protected OpenAIBasedEncoder(String providerName, HttpClient client, URI endpoint, OptionalLong dimensions) {
        // TODO: OptionalLong when it's supported by parameter thingy
        this.providerName = providerName;
        this.client = client;
        this.endpoint = endpoint;
        this.dimensions = dimensions;
    }

    @Override
    public float[] encode(String data) {
        return encode(List.of(data), EMPTY_INT_ARRAY).findFirst().orElseThrow().vector();
    }

    @Override
    public Stream<BatchRow> encode(List<String> resources, int[] nullIndexes) {
        try {
            final var headers = Multimaps.mutable.list.of(
                    "Content-Type",
                    "application/json; charset=" + StandardCharsets.UTF_8,
                    "Accept",
                    "application/json");
            extendHeaders(headers);

            final Payload payload = writer -> writeRequestPayload(writer, resources);
            try (final var inputStream = client.sendRequest(endpoint, headers, payload)) {
                return parseResponse(resources, inputStream, nullIndexes);
            }

        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private Stream<BatchRow> parseResponse(List<String> resources, InputStream inputStream, int[] nullIndexes) {
        return parseResponse(providerName, resources, inputStream, nullIndexes);
    }

    /*
    relevant part of response:
        {
            "data": [
                (vector:List<Double>),
                (vector:List<Double>),
                ...,
                (vector:List<Double>)
            ]
        }
    */
    @VisibleForTesting
    public static Stream<BatchRow> parseResponse(
            String providerName, List<String> resources, InputStream inputStream, int[] nullIndexes)
            throws MalformedGenAIResponseException {
        final String[] properties = {"embedding"};
        return JsonResponseParser.parseResponse(providerName, "data", properties, resources, inputStream, nullIndexes);
    }

    /*
     payload:
        {
           "input": [
               (resource:String),
               (resource:String),
               ...,
               (resource:String)
           ],
           "encoding_format": "float",
           <provider specific fields>
       }
    */
    private Object buildPayload(List<String> resources) {
        final var payload = Maps.mutable.of(
                "input", resources,
                "encoding_format", ENCODING_FORMAT);
        dimensions.ifPresent(d -> payload.put("dimensions", d));
        extendPayload(payload);
        return payload;
    }

    private void writeRequestPayload(Writer writer, List<String> resources) throws IOException {
        OBJECT_MAPPER.writeValue(writer, buildPayload(resources));
    }
}
