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
package org.neo4j.genai.ai.text.tokenCount.provider.vertex;

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
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.tokenCount.TextTokenCount;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.UrlPath;
import org.neo4j.values.virtual.MapValue;

@ServiceProvider
public class VertexAi implements TextTokenCount.Provider {
    protected static final String DEFAULT_BASE_URL_TEMPLATE = "https://%s-aiplatform.googleapis.com";
    private static final String DEFAULT_API_PATH_TEMPLATE =
            "v1/projects/%s/locations/%s/publishers/%s/models/%s:countTokens";

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
    public Implementation configure(HttpService httpService, MapValue configuration, GenAIConfig genAIConfig) {
        final var params = parse(Parameters.class, configuration);
        if (params.token.isEmpty() && params.apiKey.isEmpty()) {
            throw new IllegalArgumentException("'token or apiKey' is expected to have been set");
        } else if (params.token.isPresent() && params.apiKey.isPresent()) {
            throw new IllegalArgumentException("Only one of either 'token' or 'apiKey' is expected to have been set");
        }
        return new Impl(name(), endpoint(params), httpService, params);
    }

    private record Impl(String name, URI endpoint, HttpService httpService, Parameters params)
            implements Implementation {

        @Override
        public long tokenCount(String prompt) {
            final var payload = buildPayload(prompt);

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
                            Impl::parseResponse);
            return response.totalTokens();
        }

        private static ResponseModel.Response parseResponse(InputStream inputStream) {
            return org.neo4j.genai.util.JsonUtils.readValue(inputStream, ResponseModel.Response.class);
        }

        /*
         * {
         *   "contents": [{
         *     "role": "User",
         *     "parts": [{
         *       "text": prompt
         *     }]
         *   }]
         * }
         */
        private Map<String, Object> buildPayload(String prompt) {
            final MutableMap<String, Object> payload = Maps.mutable.empty();

            List<Map<String, Object>> contents = new java.util.ArrayList<>();
            if (params.chatHistory != null && !params.chatHistory.isEmpty()) {
                contents.addAll(params.chatHistory);
            }

            contents.add(Map.of("role", "user", "parts", List.of(Map.of("text", prompt))));
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

    /* Expected Vertex subset response:
     * {
     *   "totalTokens": 31,
     *   "totalBillableCharacters": 96,
     *   "promptTokensDetails": [
     *     {
     *       "modality": "TEXT",
     *       "tokenCount": 31
     *     }
     *   ]
     * }
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(long totalTokens) {}
    }

    private static class DefaultBaseUriResolver implements Function<Parameters, URI> {
        @Override
        public URI apply(Parameters parameters) {
            final var region = UrlPath.pathSafe(parameters.region, "region");
            return URI.create(DEFAULT_BASE_URL_TEMPLATE.formatted(region));
        }
    }
}
