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
package org.neo4j.genai.ai.text.completion.provider.openai;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.genai.ai.provider.openai.OpenAiRequestSupport;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.util.VisibleForTesting;

public interface OpenAiBase<PARAMS> extends TextCompletion.Provider.Implementation, OpenAiRequestSupport {

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
    default String complete(String prompt) {
        final var payload = payload(prompt);
        final var response = postJson(payload, OpenAiBase::parseResponse);
        if (response.size() != 1) {
            throw new MalformedGenAIResponseException("Expected exactly one message, but found " + response.size());
        }
        return response.getFirst();
    }

    @VisibleForTesting
    private static List<String> parseResponse(InputStream inputStream) throws MalformedGenAIResponseException {
        final var response = JsonUtils.readValue(inputStream, ResponseModel.Response.class);
        final var messages = response.output().stream()
                .filter(o -> "message".equals(o.type()))
                .flatMap(o -> o.content().stream())
                .filter(c -> "output_text".equals(c.type()))
                .map(ResponseModel.Content::text)
                .toList();
        if (messages.isEmpty() && response.incomplete_details() != null) {
            final var reason = response.incomplete_details().get("reason");
            throw new MalformedGenAIResponseException(
                    "Request to OpenAI failed due to: " + (reason == null ? "an unknown reason." : reason));
        }
        return messages;
    }

    private MutableMap<String, Object> payload(String prompt) {
        final var payload = Maps.mutable.<String, Object>empty();
        extendPayload(payload);

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

    /*
     * {
     *   "incomplete_details": {
     *      "reason": "reason"
     *   },
     *   "output": [
     *     {
     *       "type": "message",
     *       "content": [
     *           {
     *             "type": "output_text",
     *             "text": "Hello! How can I assist you today?",
     *           }
     *       ],
     *     }
     *   ],
     * }
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Content(@JsonProperty(required = true) String type, String text) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Output(@JsonProperty(required = true) String type, List<Content> content) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(
                @JsonProperty(required = true) List<Output> output,
                @JsonProperty("incomplete_details") Map<String, String> incomplete_details) {}
    }
}
