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
package org.neo4j.genai.ai.image.embed.provider.vertexai;

import static org.neo4j.genai.util.Parameters.parse;

import java.net.URI;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.collections.api.factory.Maps;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbedding;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.ai.text.embed.provider.vertexai.VertexAi;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.UrlPath;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.virtual.MapValue;

@ServiceProvider
public class VertexAiImage implements ImageVectorEmbedding.Provider {
    private final Function<VertexAi.Parameters, URI> baseUriResolver;

    public VertexAiImage() {
        this.baseUriResolver =
                p -> URI.create(VertexAi.DEFAULT_BASE_URL_TEMPLATE.formatted(UrlPath.pathSafe(p.region, "region")));
    }

    @VisibleForTesting
    public VertexAiImage(Function<VertexAi.Parameters, URI> baseUriResolver) {
        this.baseUriResolver = baseUriResolver;
    }

    @Override
    public String name() {
        return "VertexAI";
    }

    @Override
    public Class<?> paramType() {
        return VertexAi.Parameters.class;
    }

    @Override
    public VectorEmbedding.Provider.Implementation configure(
            HttpService httpService, MapValue conf, GenAIConfig genAIConfig) {
        final var params = parse(VertexAi.Parameters.class, conf);
        if (params.token.isEmpty() && params.apiKey.isEmpty()) {
            throw new IllegalArgumentException("'token or apiKey' is expected to have been set");
        } else if (params.token.isPresent() && params.apiKey.isPresent()) {
            throw new IllegalArgumentException("Only one of either 'token' or 'apiKey' is expected to have been set");
        }
        final var endpoint = VertexAi.computeEndpoint(params, baseUriResolver);
        return VertexAi.newImplementation(
                name(), endpoint, httpService, params, this::imageInstance, VertexAi.IMAGE_RESPONSE_PROPERTIES);
    }

    private Map<String, ?> imageInstance(String resource) {
        return Maps.mutable.of("image", Maps.mutable.of("bytesBase64Encoded", resource));
    }
}
