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
package org.neo4j.genai.ai.text.completion.provider.bedrock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.util.VisibleForTesting;

@ServiceProvider
public class BedrockNova extends BedrockBase implements TextCompletion.Provider {
    public BedrockNova() {}

    @VisibleForTesting
    public BedrockNova(Function<Parameters, URI> baseUriResolver) {
        super(baseUriResolver);
    }

    @Override
    public String name() {
        return "Bedrock-Nova";
    }

    @Override
    protected RequestHandler requestHandler() {
        return new NovaRequestHandler();
    }
}

final class NovaRequestHandler implements BedrockBase.RequestHandler {
    @Override
    public Map<String, Object> payload(String prompt) {
        return Map.of("messages", List.of(Map.of("role", "user", "content", List.of(Map.of("text", prompt)))));
    }

    @Override
    public String parseResponse(InputStream stream) {
        final var contents =
                JsonUtils.readValue(stream, ResponseModel.Response.class).output().message().content().stream()
                        .filter(c -> c.text() != null)
                        .toList();
        if (contents.size() != 1) {
            throw new MalformedGenAIResponseException("Expected exactly one `content`");
        }
        return contents.getFirst().text();
    }

    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Content(String text) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Message(String role, List<Content> content) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Output(Message message) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(Output output) {}
    }
}
