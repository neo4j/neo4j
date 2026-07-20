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
package org.neo4j.genai.ai.text.structuredCompletion.provider.openai;

import static org.neo4j.genai.util.Parameters.toJavaMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.genai.ai.provider.openai.OpenAiRequestSupport;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.virtual.MapValue;

public interface OpenAiBase<PARAMS> extends TextStructuredCompletion.Provider.Implementation, OpenAiRequestSupport {

    @Override
    URI endpoint();

    @Override
    HttpService httpService();

    PARAMS params();

    @Override
    String[] authHeader();

    void extendPayload(MutableMap<String, Object> payload);

    List<Map<String, Object>> chatHistory();

    @Override
    default MapValue complete(String prompt, MapValue schema) {
        final var payload = payload(prompt, schema);
        final var response = postJson(payload, OpenAiBase::parseResponse);
        return ValueUtils.asMapValue(response);
    }

    @VisibleForTesting
    private static Map<String, Object> parseResponse(InputStream inputStream) throws MalformedGenAIResponseException {
        final var response = JsonUtils.readValue(inputStream, ResponseModel.Response.class);

        // Find refusal if present
        for (var output : response.output()) {
            if (!"message".equals(output.type())) continue;
            for (var c : output.content()) {
                if ("refusal".equals(c.type()) && c.refusal() != null) {
                    Map<String, Object> m = new HashMap<>();
                    m.put("refusal", c.refusal());
                    return m;
                }
            }
        }

        for (var output : response.output()) {
            if (!"message".equals(output.type())) continue;
            for (var c : output.content()) {
                if (c.text() != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> dataMap;
                    try {
                        dataMap = mapper.readValue(c.text(), new TypeReference<>() {});
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    return dataMap;
                }
            }
        }

        if (response.incomplete_details() != null) {
            final var reason = response.incomplete_details().get("reason");
            throw new MalformedGenAIResponseException(
                    "Request failed due to: " + (reason == null ? "an unknown reason." : reason));
        }
        throw new MalformedGenAIResponseException("No parsed content in response");
    }

    private MutableMap<String, Object> payload(String prompt, MapValue schemaValue) {
        final var payload = Maps.mutable.<String, Object>empty();
        extendPayload(payload);

        var schema = toJavaMap(schemaValue);
        payload.put(
                "text",
                Map.of(
                        "format",
                        Map.of(
                                "type",
                                "json_schema",
                                "strict",
                                true,
                                "schema",
                                schema,
                                "name",
                                "answer_with_structured_ouput")));

        var chatHistory = chatHistory();
        final Map<String, Object> newInput = Map.of("role", "user", "content", prompt);

        List<Map<String, Object>> input;
        if (chatHistory != null && !chatHistory.isEmpty()) {
            input = chatHistory;
            input.add(newInput);
        } else {
            input = List.of(newInput);
        }

        payload.put("input", input);
        return payload;
    }

    /* Expected shape (subset):
     * {
     *   "output": [
     *     {
     *       "type": "message",
     *       "content": [
     *         {"type": "refusal", "refusal": "..."},
     *         {"type": "output_text", "text": " ... "}
     *       ]
     *     }
     *   ],
     *   "incomplete_details": {"reason": "..."}
     * }
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Content(@JsonProperty(required = true) String type, String text, String refusal) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Output(@JsonProperty(required = true) String type, List<Content> content) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(
                @JsonProperty(required = true) List<Output> output,
                @JsonProperty("incomplete_details") Map<String, String> incomplete_details) {}
    }
}
