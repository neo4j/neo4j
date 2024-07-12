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
package org.neo4j.genai.vector.providers;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.text.StringSubstitutor;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.util.Client.Payload;
import org.neo4j.genai.util.HttpClient;
import org.neo4j.genai.util.JsonResponseParser;
import org.neo4j.genai.vector.MalformedGenAIResponseException;
import org.neo4j.genai.vector.VectorEncoding.BatchRow;
import org.neo4j.genai.vector.VectorEncoding.Provider;

@ServiceProvider
public final class VertexAI implements Provider<VertexAI.Parameters> {
    public static final String NAME = "VertexAI";
    private static final String ENDPOINT_TEMPLATE = "https://${region}-aiplatform.googleapis.com/v1"
            + "/projects/${projectId}/locations/${region}/publishers/google/models/${model}:predict";
    static final String DEFAULT_REGION = "us-central1";
    static final Set<String> SUPPORTED_REGIONS = Set.of(
            // https://cloud.google.com/vertex-ai/docs/general/locations
            "us-west1",
            "us-west2",
            "us-west3",
            "us-west4",
            "us-central1",
            "us-east1",
            "us-east4",
            "us-south1",
            "northamerica-northeast1",
            "northamerica-northeast2",
            "southamerica-east1",
            "southamerica-west1",
            "europe-west2",
            "europe-west1",
            "europe-west4",
            "europe-west6",
            "europe-west3",
            "europe-north1",
            "europe-central2",
            "europe-west8",
            "europe-west9",
            "europe-southwest1",
            "asia-south1",
            "asia-southeast1",
            "asia-southeast2",
            "asia-east2",
            "asia-east1",
            "asia-northeast1",
            "asia-northeast2",
            "australia-southeast1",
            "australia-southeast2",
            "asia-northeast3",
            "me-west1");
    private static final String STRINGIFIED_SUPPORTED_REGIONS =
            SUPPORTED_REGIONS.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ", "[", "]"));
    static final String DEFAULT_MODEL = "textembedding-gecko@001";
    static final Set<String> SUPPORTED_MODELS = Set.of(
            DEFAULT_MODEL,
            "textembedding-gecko@002",
            "textembedding-gecko@003",
            "textembedding-gecko-multilingual@001");
    private static final String STRINGIFIED_SUPPORTED_MODELS =
            SUPPORTED_MODELS.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ", "[", "]"));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final HttpClient client = new HttpClient();

    public static class Parameters {
        public String token;
        public String projectId;
        public String model = DEFAULT_MODEL;
        public String region = DEFAULT_REGION;
        public Optional<String> taskType;
        public Optional<String> title;
    }

    @Override
    public Class<Parameters> parameterDeclarations() {
        return Parameters.class;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Provider.Encoder configure(Parameters configuration) {
        if (!SUPPORTED_MODELS.contains(configuration.model)) {
            throw new IllegalArgumentException("Provided model '%s' is not supported. Supported models: %s"
                    .formatted(configuration.model, STRINGIFIED_SUPPORTED_MODELS));
        }
        if (!SUPPORTED_REGIONS.contains(configuration.region)) {
            throw new IllegalArgumentException("Provided region '%s' is not supported. Supported regions: %s"
                    .formatted(configuration.region, STRINGIFIED_SUPPORTED_REGIONS));
        }

        final var endpoint = URI.create(StringSubstitutor.replace(
                ENDPOINT_TEMPLATE,
                Map.of(
                        "region",
                        configuration.region,
                        "projectId",
                        configuration.projectId,
                        "model",
                        configuration.model)));
        return new Encoder(client, endpoint, configuration);
    }

    record Encoder(HttpClient client, URI endpoint, Parameters configuration) implements Provider.Encoder {

        @Override
        public float[] encode(String data) {
            return encode(List.of(data), EMPTY_INT_ARRAY)
                    .findFirst()
                    .orElseThrow()
                    .vector();
        }

        @Override
        public Stream<BatchRow> encode(List<String> resources, int[] nullIndexes) {
            try {
                final var headers = Multimaps.mutable.list.of(
                        "Authorization", "Bearer " + configuration.token,
                        "Content-Type", "application/json; charset=" + StandardCharsets.UTF_8,
                        "Accept", "application/json");

                final Payload payload = writer -> writeRequestPayload(writer, resources);
                try (final var inputStream = client.sendRequest(endpoint, headers, payload)) {
                    return JsonResponseParser.parseResponse(
                            NAME,
                            "predictions",
                            new String[] {"embeddings", "values"},
                            resources,
                            inputStream,
                            nullIndexes);
                }

            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }

        /*
        relevant part of response:
            {
                "predictions": [
                    { "embeddings": { "values": (vector:List<Double>) } },
                    { "embeddings": { "values": (vector:List<Double>) } },
                    ...,
                    { "embeddings": { "values": (vector:List<Double>) } },
                ]
            }
        */
        static Stream<BatchRow> parseResponse(List<String> resources, InputStream inputStream, int[] nullIndexes)
                throws MalformedGenAIResponseException {
            final String[] properties = {"embeddings", "values"};
            return JsonResponseParser.parseResponse(
                    NAME, "predictions", properties, resources, inputStream, nullIndexes);
        }
        /*
         payload:
            {
               "instances": [
                   { "content": (resource:String) },
                   { "content": (resource:String) },
                   ...,
                   { "content": (resource:String) },
               ]
           }
        */
        private void writeRequestPayload(Writer writer, List<String> resources) throws IOException {
            OBJECT_MAPPER.writeValue(
                    writer,
                    Map.of(
                            "instances",
                            resources.stream()
                                    .map(resource -> {
                                        var instance = Maps.mutable.of("content", resource);
                                        configuration.taskType.ifPresent(x -> instance.put("task_type", x));
                                        configuration.title.ifPresent(x -> instance.put("title", x));
                                        return instance;
                                    })
                                    .toList()));
        }
    }
}
