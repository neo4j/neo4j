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
import java.util.Objects;
import java.util.function.Function;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.util.VisibleForTesting;

@Deprecated
@ServiceProvider
public class BedrockTitan extends BedrockBase implements TextCompletion.Provider {
    public BedrockTitan() {}

    @VisibleForTesting
    public BedrockTitan(Function<Parameters, URI> baseUriResolver) {
        super(baseUriResolver);
    }

    @Override
    public String name() {
        return "Bedrock-Titan";
    }

    @Override
    protected RequestHandler requestHandler() {
        return new TitanRequestHandler();
    }
}

final class TitanRequestHandler implements BedrockBase.RequestHandler {
    @Override
    public Map<String, Object> payload(String prompt) {
        return Map.of("inputText", prompt);
    }

    @Override
    public String parseResponse(InputStream stream) {
        final var results = JsonUtils.readValue(stream, ResponseModel.Response.class).results().stream()
                .map(ResponseModel.Result::outputText)
                .filter(Objects::nonNull)
                .toList();
        if (results.size() != 1) {
            throw new MalformedGenAIResponseException("Expected exactly one response, but found " + results.size());
        }
        return results.getFirst();
    }

    interface ResponseModel {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Result(String outputText) {}

        @JsonIgnoreProperties(ignoreUnknown = true)
        record Response(List<Result> results) {}
    }
}
