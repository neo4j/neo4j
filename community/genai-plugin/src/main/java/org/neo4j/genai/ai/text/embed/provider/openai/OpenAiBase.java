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
package org.neo4j.genai.ai.text.embed.provider.openai;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.genai.ai.provider.openai.OpenAiRequestSupport;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

public interface OpenAiBase<PARAMS> extends VectorEmbedding.Provider.Implementation, OpenAiRequestSupport {
    String ENCODING_FORMAT = "float";

    @Override
    URI endpoint();

    @Override
    HttpService httpService();

    PARAMS params();

    @Override
    String[] authHeader();

    void extendPayload(MutableMap<String, Object> payload);

    @Override
    default long maxBatchSize() {
        // Default token limit for embeddings endpoint
        return 8192;
    }

    @Override
    default VectorValue encode(String resource) {
        var vectors = encode(List.of(resource), EMPTY_INT_ARRAY, 0).toList();
        if (vectors.size() != 1) {
            throw new MalformedGenAIResponseException(
                    "Expected exactly one vector embedding, but found " + vectors.size());
        }
        return vectors.getFirst().vector();
    }

    @Override
    default Stream<VectorEmbedding.InternalBatchRow> encodeBatch(
            List<String> resources, int[] nullIndexes, int batchOffset) {
        return encode(resources, nullIndexes, batchOffset);
    }

    private Stream<VectorEmbedding.InternalBatchRow> encode(
            List<String> resources, int[] nullIndexes, int batchOffset) {
        return postJson(
                buildPayload(resources),
                inputStream -> parseResponse(resources, inputStream, nullIndexes, batchOffset));
    }

    @VisibleForTesting
    static Stream<VectorEmbedding.InternalBatchRow> parseResponse(
            List<String> resources, InputStream inputStream, int[] nullIndexes, int batchOffset)
            throws MalformedGenAIResponseException {
        final var response = JsonUtils.readValue(inputStream, ResponseModel.Response.class);

        final var data = response.data();
        if (data == null) {
            throw new MalformedGenAIResponseException("Expected response to contain 'data' array");
        }
        if (data.size() != resources.size()) {
            throw new MalformedGenAIResponseException(
                    ("Expected to receive %d embeddings; however got %d").formatted(resources.size(), data.size()));
        }

        final var offset = new MutableInt();
        offset.add(batchOffset);
        return IntStream.range(batchOffset, batchOffset + resources.size() + nullIndexes.length)
                .mapToObj(index -> {
                    try {
                        if (Arrays.binarySearch(nullIndexes, index - batchOffset) >= 0) {
                            offset.increment();
                            return new VectorEmbedding.InternalBatchRow(index, null, null);
                        }
                        final int offsetIndex = index - offset.intValue();
                        final var vector = data.get(offsetIndex).embedding();
                        if (vector == null) {
                            throw new MalformedGenAIResponseException(
                                    "Expected embedding to be present at index " + offsetIndex);
                        }
                        return new VectorEmbedding.InternalBatchRow(
                                index, resources.get(offsetIndex), Values.float32Vector(vector));
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                });
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
        final var payload = Maps.mutable.<String, Object>empty();
        extendPayload(payload);
        // Put in after so the user can't override the input and encoding_format
        payload.put("input", resources);
        payload.put("encoding_format", ENCODING_FORMAT);
        return payload;
    }

    /*
     * Expected shape (subset):
     * {
     *   "data": [ { "embedding": [float, ...] }, ... ]
     * }
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Data(@JsonProperty(required = true) float[] embedding) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(@JsonProperty(required = true) List<Data> data) {}
    }
}
