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
package org.neo4j.genai.ai.text.embed.provider.bedrock;

import static org.neo4j.genai.util.Parameters.parse;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.aws.AwsSignatureV4HeaderGenerator;
import org.neo4j.genai.util.aws.URLUtils.URLEncoder;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.MapValue;

public abstract class BedrockBase {
    public abstract String name();

    protected static final String DEFAULT_BASE_URL_TEMPLATE = "https://bedrock-runtime.%s.amazonaws.com";
    private static final String DEFAULT_API_PATH_TEMPLATE = "/model/%s/invoke";
    private final Function<Parameters, URI> baseUriResolver;

    public BedrockBase() {
        this.baseUriResolver = new DefaultBaseUriResolver();
    }

    @VisibleForTesting
    public BedrockBase(Function<Parameters, URI> baseUriResolver) {
        this.baseUriResolver = baseUriResolver;
    }

    public static class Parameters {
        public String accessKeyId;
        public String secretAccessKey;
        public String region;
        public String model;
        public Map<String, Object> vendorOptions = Map.of();
    }

    public final String metricsName() {
        return "Bedrock";
    }

    public final Class<?> paramType() {
        // Bedrock models currently share the same parameters, but there is nothing preventing us from changing that.
        return Parameters.class;
    }

    protected abstract RequestHandler requestHandler();

    public final VectorEmbedding.Provider.Implementation configure(
            HttpService httpService, MapValue configuration, GenAIConfig genAIConfig) {
        final var params = parse(Parameters.class, configuration);
        final var encodedModel = URLEncoder.encode(params.model);
        final var uri = baseUriResolver.apply(params).resolve(DEFAULT_API_PATH_TEMPLATE.formatted(encodedModel));
        return new BedrockImplementation(name(), metricsName(), uri, httpService, params, requestHandler());
    }

    public interface RequestHandler {
        Map<String, Object> payload(String resource);

        VectorValue parseResponse(InputStream stream);
    }
}

record BedrockImplementation(
        @Override String name,
        @Override String metricsName,
        URI endpoint,
        HttpService httpService,
        BedrockBase.Parameters params,
        BedrockBase.RequestHandler requestHandler)
        implements VectorEmbedding.Provider.Implementation {

    @Override
    public long maxBatchSize() {
        // Bedrock doesn't accept a list of embeddings
        return 0;
    }

    @Override
    public VectorValue encode(String resource) {
        var body = createRequestBody(resource);
        final var endpoint = endpoint();
        return httpService()
                .request(
                        endpoint,
                        builder -> {
                            var intermediate = builder.build();
                            var requestProperties = Multimaps.mutable.list.with("Host", endpoint.getHost());
                            intermediate.headers().map().forEach(requestProperties::putAll);

                            var finalHeaders = new AwsSignatureV4HeaderGenerator(
                                            params().region, endpoint, body, requestProperties)
                                    .generate(params().accessKeyId, params().secretAccessKey);

                            var newBuilder =
                                    HttpRequest.newBuilder(intermediate, (k, v) -> !finalHeaders.containsKey(k));
                            finalHeaders.forEachKeyValue(newBuilder::header);
                            return newBuilder
                                    .POST(HttpRequest.BodyPublishers.ofString(body))
                                    .build();
                        },
                        requestHandler::parseResponse);
    }

    @Override
    public Stream<VectorEmbedding.InternalBatchRow> encodeBatch(
            List<String> resources, int[] nullIndexes, int batchOffset) {
        final MutableInt offset = new MutableInt();
        return IntStream.range(0, resources.size() + nullIndexes.length).mapToObj(index -> {
            // We need to reinsert nulls at the right place
            if (Arrays.binarySearch(nullIndexes, index) >= 0) {
                offset.increment();
                return new VectorEmbedding.InternalBatchRow(index, null, null);
            }
            final int offsetIndex = index - offset.intValue();
            final var resource = resources.get(offsetIndex);
            return new VectorEmbedding.InternalBatchRow(index, resource, encode(resource));
        });
    }

    private String createRequestBody(String resource) {
        try {
            final var payload = Maps.mutable.<String, Object>ofInitialCapacity(
                    params().vendorOptions.size() + 1);
            payload.putAll(params().vendorOptions);
            payload.putAll(requestHandler.payload(resource));
            return JsonUtils.getObjectMapper().writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}

class DefaultBaseUriResolver implements Function<BedrockBase.Parameters, URI> {
    @Override
    public URI apply(BedrockBase.Parameters conf) {
        return URI.create(BedrockBase.DEFAULT_BASE_URL_TEMPLATE.formatted(conf.region));
    }
}
