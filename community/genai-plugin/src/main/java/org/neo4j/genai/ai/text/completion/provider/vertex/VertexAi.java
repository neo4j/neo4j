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
package org.neo4j.genai.ai.text.completion.provider.vertex;

import static org.neo4j.genai.util.HttpService.jsonBody;
import static org.neo4j.genai.util.Parameters.parse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.genai.util.UrlPath;
import org.neo4j.values.virtual.MapValue;

@ServiceProvider
public class VertexAi implements TextCompletion.Provider {
    protected static final String DEFAULT_BASE_URL_TEMPLATE = "https://%s-aiplatform.googleapis.com";
    private static final String DEFAULT_API_PATH_TEMPLATE =
            "v1/projects/%s/locations/%s/publishers/%s/models/%s:generateContent";

    private final Function<Parameters, URI> baseUriResolver;

    public VertexAi() {
        this.baseUriResolver = new DefaultBaseUriResolver();
    }

    public VertexAi(Function<Parameters, URI> baseUriResolver) {
        this.baseUriResolver = baseUriResolver;
    }

    public static class Parameters {
        public Optional<String> token; // OAuth Access Token
        public Optional<String> apiKey; // API Key
        public String model;
        public String project;
        public String region;
        public String publisher = "google";
        public Map<String, Object> vendorOptions = Map.of();
        public List<Map<String, Object>> chatHistory = List.of();
    }

    @Override
    public String name() {
        return "VertexAI";
    }

    @Override
    public Class<?> paramType() {
        return Parameters.class;
    }

    @Override
    public Implementation configure(HttpService httpService, MapValue conf, GenAIConfig genAIConfig) {
        final var params = parse(Parameters.class, conf);
        if (params.token.isEmpty() && params.apiKey.isEmpty()) {
            throw new IllegalArgumentException("'token or apiKey' is expected to have been set");
        } else if (params.token.isPresent() && params.apiKey.isPresent()) {
            throw new IllegalArgumentException("Only one of either 'token' or 'apiKey' is expected to have been set");
        }
        return new Implementation(name(), endpoint(params), httpService, params);
    }

    private record Implementation(String name, URI endpoint, HttpService httpService, Parameters params)
            implements TextCompletion.Provider.Implementation {

        @Override
        public String complete(String prompt) {
            final Object payload = buildPayload(prompt);

            URI requestEndpoint = endpoint;
            if (params.apiKey.isPresent()) {
                String encodedKey = URLEncoder.encode(params.apiKey.get(), StandardCharsets.UTF_8);
                requestEndpoint = URI.create(endpoint.toString() + "?key=" + encodedKey);
            }

            final var response = httpService()
                    .request(
                            requestEndpoint,
                            builder -> {
                                var b = builder.header(
                                                "Content-Type", "application/json; charset=" + StandardCharsets.UTF_8)
                                        .header("Accept", "application/json");
                                if (params.token.isPresent()) {
                                    b = b.header("Authorization", "Bearer " + params.token.get());
                                }
                                return b.POST(jsonBody(payload)).build();
                            },
                            Implementation::parseResponse);
            if (response.size() != 1) {
                throw new MalformedGenAIResponseException("Expected exactly one message, but found " + response.size());
            }
            return response.getFirst();
        }

        private static List<String> parseResponse(InputStream inputStream) {
            final var response = JsonUtils.readValue(inputStream, ResponseModel.Response.class);
            return response.candidates().stream()
                    .filter(c -> "model".equals(c.content().role()))
                    .flatMap(c -> c.content().parts().stream())
                    .map(ResponseModel.Part::text)
                    .toList();
        }

        private Map<String, Object> buildPayload(String prompt) {
            final var payload = Maps.mutable.ofMap(params.vendorOptions);
            final var newContent =
                    Map.of("role", "user", "parts", Lists.immutable.of(Maps.immutable.of("text", prompt)));

            List<Map<String, Object>> contents;
            if (params.chatHistory != null && !params.chatHistory.isEmpty()) {
                contents = params.chatHistory;
                contents.add(newContent);
            } else {
                contents = List.of(newContent);
            }

            payload.put("contents", contents);
            return payload;
        }
    }

    private URI endpoint(Parameters params) {
        return baseUriResolver
                .apply(params)
                .resolve(DEFAULT_API_PATH_TEMPLATE.formatted(
                        UrlPath.pathSafe(params.project, "project"),
                        UrlPath.pathSafe(params.region, "region"),
                        UrlPath.pathSafe(params.publisher, "publisher"),
                        UrlPath.pathSafe(params.model, "model")));
    }

    /*
     * {
     *   "candidates": [
     *     {
     *       "content": {
     *         "role": "assistant",
     *         "parts": [
     *           "text": "Bla bla..."
     *         ]
     *       }
     *     }
     *   ]
     * }
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Part(String text) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Content(String role, List<Part> parts) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Candidate(Content content) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(List<Candidate> candidates) {}
    }

    private static class DefaultBaseUriResolver implements Function<VertexAi.Parameters, URI> {
        @Override
        public URI apply(VertexAi.Parameters parameters) {
            final var region = UrlPath.pathSafe(parameters.region, "region");
            return URI.create(DEFAULT_BASE_URL_TEMPLATE.formatted(region));
        }
    }
}
