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
package org.neo4j.genai.ai.text.chat.provider.openai;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.genai.ai.provider.openai.OpenAiRequestSupport;
import org.neo4j.genai.ai.text.chat.TextChat;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;

public interface OpenAiChatBase<PARAMS> extends TextChat.Provider.Implementation, OpenAiRequestSupport {
    @Override
    URI endpoint();

    @Override
    HttpService httpService();

    PARAMS params();

    @Override
    String[] authHeader();

    void extendPayload(MutableMap<String, Object> payload);

    @Override
    default TextChat.ChatResult chat(String prompt, Optional<String> previousResponseId) {
        final var payload = payload(List.of(prompt), previousResponseId);
        final var response = postJson(payload, OpenAiChatBase::parseResponse);
        if (response.messages().size() != 1) {
            throw new MalformedGenAIResponseException("Expected exactly one message, but found "
                    + response.messages().size());
        }
        return new TextChat.ChatResult(response.messages().getFirst(), response.id());
    }

    private static ParsedResponse parseResponse(InputStream inputStream) throws MalformedGenAIResponseException {
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
        return new ParsedResponse(response.id(), messages);
    }

    private MutableMap<String, Object> payload(List<String> prompts, Optional<String> previousResponseId) {
        final var payload = Maps.mutable.<String, Object>empty();
        extendPayload(payload);

        final var messages = prompts.stream()
                .map(prompt -> Map.of("role", "user", "content", prompt))
                .toList();
        payload.put("input", messages);
        previousResponseId.ifPresent(id -> payload.put("previous_response_id", id));

        return payload;
    }

    record ParsedResponse(String id, List<String> messages) {}

    /*
     * Minimal subset of the OpenAI responses schema we care about
     */
    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Content(@JsonProperty(required = true) String type, String text) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Output(@JsonProperty(required = true) String type, List<Content> content) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(
                @JsonProperty(required = true) String id,
                @JsonProperty(required = true) List<Output> output,
                @JsonProperty("incomplete_details") Map<String, String> incomplete_details) {}
    }
}
