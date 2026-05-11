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
package org.neo4j.genai.ai.image.embed.provider.bedrock;

import static org.neo4j.genai.util.JsonUtils.TYPE_REF_FLOAT_VECTOR;
import static org.neo4j.genai.util.JsonUtils.getExpectedFrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.collections.api.factory.Maps;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbedding;
import org.neo4j.genai.ai.text.embed.provider.bedrock.BedrockBase;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

@ServiceProvider
public class BedrockTitanImage extends BedrockBase implements ImageVectorEmbedding.Provider {
    public BedrockTitanImage() {}

    @VisibleForTesting
    public BedrockTitanImage(Function<BedrockBase.Parameters, URI> baseUriResolver) {
        super(baseUriResolver);
    }

    @Override
    public String name() {
        return "Bedrock-Titan";
    }

    @Override
    protected RequestHandler requestHandler() {
        return new TitanImageRequestHandler();
    }
}

final class TitanImageRequestHandler implements BedrockBase.RequestHandler {
    @Override
    public Map<String, Object> payload(String resource) {
        return Maps.mutable.of("inputImage", resource);
    }

    @Override
    public VectorValue parseResponse(InputStream inputStream) {
        final JsonNode tree;
        final ObjectMapper objectMapper = JsonUtils.getObjectMapper();
        try {
            tree = objectMapper.readTree(inputStream);
        } catch (IOException e) {
            throw new MalformedGenAIResponseException("Unexpected error occurred while parsing the API response", e);
        }

        final var embedding = getExpectedFrom("BEDROCK", tree, "embedding");
        if (!embedding.isArray()) {
            throw new MalformedGenAIResponseException("Expected embedding to be an array");
        }

        try (final var parser = embedding.traverse(objectMapper)) {
            return Values.float32Vector(parser.readValueAs(TYPE_REF_FLOAT_VECTOR));
        } catch (IOException e) {
            throw new MalformedGenAIResponseException("Unexpected error occurred while parsing the embedding", e);
        }
    }
}
