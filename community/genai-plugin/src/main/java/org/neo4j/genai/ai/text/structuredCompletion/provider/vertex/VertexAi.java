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
package org.neo4j.genai.ai.text.structuredCompletion.provider.vertex;

import static org.neo4j.genai.util.HttpService.jsonBody;
import static org.neo4j.genai.util.Parameters.parse;
import static org.neo4j.genai.util.Parameters.toJavaMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.genai.util.UrlPath;
import org.neo4j.values.virtual.MapValue;

@ServiceProvider
public class VertexAi implements TextStructuredCompletion.Provider {
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
            implements TextStructuredCompletion.Provider.Implementation {

        @Override
        public MapValue complete(String prompt, MapValue schema) {
            final var payload = buildPayload(prompt, schema);

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
            return org.neo4j.kernel.impl.util.ValueUtils.asMapValue(response);
        }

        private static Map<String, Object> parseResponse(InputStream inputStream) {
            final var response = JsonUtils.readValue(inputStream, ResponseModel.Response.class);
            // We expect the model to return JSON string as text in the first candidate's parts
            for (var c : response.candidates()) {
                if (c.content() == null || c.content().parts() == null) continue;
                for (var p : c.content().parts()) {
                    if (p.text() != null) {
                        try {
                            return new ObjectMapper().readValue(p.text(), new TypeReference<>() {});
                        } catch (Exception e) {
                            throw new MalformedGenAIResponseException(
                                    "Failed to parse structured response: " + e.getMessage());
                        }
                    }
                }
            }
            throw new MalformedGenAIResponseException("No parsed content in response");
        }

        private Map<String, Object> buildPayload(String prompt, MapValue schemaValue) {
            final MutableMap<String, Object> payload = Maps.mutable.empty();

            // contents
            final var newContent =
                    Map.of("role", "user", "parts", Lists.immutable.of(Maps.immutable.of("text", prompt)));

            List<Map<String, Object>> contents = new java.util.ArrayList<>();
            if (params.chatHistory != null && !params.chatHistory.isEmpty()) {
                contents.addAll(params.chatHistory);
            }
            contents.add(newContent);
            payload.put("contents", contents);

            // generation_config
            var schema = toJavaMap(schemaValue);
            final var generationConfig = Maps.mutable.<String, Object>empty();
            generationConfig.put("responseMimeType", "application/json");
            generationConfig.put("responseSchema", schema);
            // Merge vendorOptions into generation_config, as used for options like maxOutputTokens
            // But if a 'system_instruction' is present, it should be pulled out of vendorOptions and
            // added as a top-level payload field according to Vertex API.
            Object systemInstruction = null;
            if (params.vendorOptions != null && !params.vendorOptions.isEmpty()) {
                final var vendorCopy = Maps.mutable.ofMap(params.vendorOptions);
                if (vendorCopy.containsKey("system_instruction")) {
                    systemInstruction = vendorCopy.remove("system_instruction");
                }
                if (!vendorCopy.isEmpty()) {
                    generationConfig.putAll(vendorCopy);
                }
            }
            payload.put("generation_config", generationConfig);

            // If we have a system instruction, place it separately in the payload as expected by Vertex
            if (systemInstruction != null) {
                if (systemInstruction instanceof String s) {
                    // Normalize string into Vertex content format
                    payload.put(
                            "system_instruction",
                            Map.of("role", "system", "parts", Lists.immutable.of(Maps.immutable.of("text", s))));
                } else if (systemInstruction instanceof Map) {
                    // Assume already in correct structure; pass through
                    @SuppressWarnings("unchecked")
                    Map<String, Object> si = (Map<String, Object>) systemInstruction;
                    payload.put("system_instruction", si);
                } else {
                    // Fallback: toString and wrap as text
                    payload.put(
                            "system_instruction",
                            Map.of(
                                    "role",
                                    "system",
                                    "parts",
                                    Lists.immutable.of(Maps.immutable.of("text", systemInstruction.toString()))));
                }
            }

            // model must be part of the path; nothing to add here beyond vendorOptions
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
     *   "candidates": [
     *     {
     *       "content": {
     *         "role": "model",
     *         "parts": [ {"text": "{ ...json... }"} ]
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
