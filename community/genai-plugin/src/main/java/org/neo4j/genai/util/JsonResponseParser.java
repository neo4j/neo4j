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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.genai.util.CheckedAccessors.Json;
import org.neo4j.genai.vector.MalformedGenAIResponseException;
import org.neo4j.genai.vector.VectorEncoding.BatchRow;

public class JsonResponseParser {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<float[]> VECTOR_TYPE_REFERENCE = new TypeReference<>() {};

    public static Stream<BatchRow> parseResponse(
            String name,
            String topLevelKey,
            String[] properties,
            List<String> resources,
            InputStream inputStream,
            int[] nullIndexes)
            throws MalformedGenAIResponseException {
        final JsonNode tree;
        try {
            tree = OBJECT_MAPPER.readTree(inputStream);
        } catch (IOException e) {
            throw new MalformedGenAIResponseException("Unexpected error occurred while parsing the API response", e);
        }

        final var predictions = getExpectedFrom(name, tree, topLevelKey);
        if (!predictions.isArray()) {
            throw new MalformedGenAIResponseException("Expected response to contain an array of embeddings");
        } else if (predictions.size() != resources.size()) {
            throw new MalformedGenAIResponseException("Expected to receive %d embeddings; however got %d"
                    .formatted(resources.size(), predictions.size()));
        }

        final var offset = new MutableInt();
        return IntStream.range(0, resources.size() + nullIndexes.length).mapToObj(index -> {
            try {
                if (Arrays.binarySearch(nullIndexes, index) >= 0) {
                    offset.increment();
                    return new BatchRow(index, null, null);
                }
                final int offsetIndex = index - offset.intValue();
                final var embedding = getExpectedFrom(name, predictions.get(offsetIndex), properties);
                if (!embedding.isArray()) {
                    throw new MalformedGenAIResponseException("Expected embedding to be an array");
                }
                try (final var parser = embedding.traverse(OBJECT_MAPPER)) {
                    return new BatchRow(index, resources.get(offsetIndex), parser.readValueAs(VECTOR_TYPE_REFERENCE));
                } catch (IOException e) {
                    throw new MalformedGenAIResponseException(
                            "Unexpected error occurred while parsing the embedding", e);
                }
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });
    }

    private static JsonNode getExpectedFrom(String name, JsonNode json, String... properties)
            throws MalformedGenAIResponseException {
        return Json.getExpectedFrom(name, json, properties);
    }
}
