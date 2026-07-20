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
package org.neo4j.genai.vector.providers;

import static org.neo4j.genai.util.JsonUtils.TYPE_REF_FLOAT_VECTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.JsonUtils;
import org.neo4j.genai.util.MalformedGenAIResponseException;
import org.neo4j.genai.util.aws.AwsSignatureV4HeaderGenerator;
import org.neo4j.genai.vector.DeprecatedVectorEncoding.Provider;

@ServiceProvider
public final class DeprecatedBedrock implements Provider<DeprecatedBedrock.Parameters> {
    public static final String NAME = "Bedrock";
    private static final String ENDPOINT_TEMPLATE =
            "https://bedrock-runtime.${region}.amazonaws.com/model/${model}/invoke";
    static final String DEFAULT_REGION = "us-east-1";
    static final Set<String> SUPPORTED_REGIONS = Set.of(
            // https://docs.aws.amazon.com/bedrock/latest/userguide/models-regions.html
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-northeast-3",
            "ap-south-1",
            "ap-south-2",
            "ap-southeast-1",
            "ap-southeast-2",
            "ca-central-1",
            "eu-central-1",
            "eu-central-2",
            "eu-north-1",
            "eu-south-1",
            "eu-south-2",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "sa-east-1",
            "us-east-1",
            "us-east-2",
            "us-gov-east-1",
            "us-gov-west-1",
            "us-west-2");
    private static final String STRINGIFIED_SUPPORTED_REGIONS =
            SUPPORTED_REGIONS.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ", "[", "]"));
    static final MapIterable<String, Model> KNOWN_MODELS = Maps.immutable.of(
            TitanEmbedTextG1Model.NAME, TitanEmbedTextG1Model.INSTANCE,
            TitanEmbedImageG1Model.NAME, TitanEmbedImageG1Model.INSTANCE,
            TitanEmbedTextV2Model.NAME, TitanEmbedTextV2Model.INSTANCE);
    static final String DEFAULT_MODEL = TitanEmbedTextG1Model.NAME;

    public static class Parameters {
        public String accessKeyId;
        public String secretAccessKey;
        public String model = DEFAULT_MODEL;
        public String region = DEFAULT_REGION;
        public OptionalLong dimensions;
        public Optional<Boolean> normalize;
    }

    @Override
    public Class<Parameters> parameterDeclarations() {
        return Parameters.class;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Provider.Encoder configure(Parameters configuration) {
        if (!SUPPORTED_REGIONS.contains(configuration.region)) {
            throw new IllegalArgumentException("Provided region '%s' is not supported. Supported regions: %s"
                    .formatted(configuration.region, STRINGIFIED_SUPPORTED_REGIONS));
        }
        final var endpoint = URI.create(StringSubstitutor.replace(
                ENDPOINT_TEMPLATE, Map.of("region", configuration.region, "model", configuration.model)));

        final var model = KNOWN_MODELS.getOrDefault(configuration.model, FallbackModel.INSTANCE);
        model.validateConfiguration(configuration);
        return model.encoder(endpoint, configuration);
    }

    private interface Model {
        default void validateConfiguration(Parameters configuration) {}

        Encoder encoder(URI endpoint, Parameters configuration);
    }

    abstract static class Encoder implements Provider.Encoder {
        private final URI endpoint;
        protected final Parameters configuration;

        protected Encoder(URI endpoint, Parameters configuration) {
            this.endpoint = endpoint;
            this.configuration = configuration;
        }

        protected abstract Object buildPayload(String resource);

        @Override
        public float[] encode(HttpService httpService, String resource) {
            try {
                var body = createRequestBody(resource);
                return httpService.request(
                        endpoint,
                        builder -> {
                            var intermediate = builder.build();
                            var requestProperties = Multimaps.mutable.list.with("Host", endpoint.getHost());
                            intermediate.headers().map().forEach(requestProperties::putAll);

                            var finalHeaders = new AwsSignatureV4HeaderGenerator(
                                            configuration.region, endpoint, body, requestProperties)
                                    .generate(configuration.accessKeyId, configuration.secretAccessKey);

                            var newBuilder =
                                    HttpRequest.newBuilder(intermediate, (k, v) -> !finalHeaders.containsKey(k));
                            finalHeaders.forEachKeyValue(newBuilder::header);
                            return newBuilder
                                    .POST(BodyPublishers.ofString(body))
                                    .build();
                        },
                        Encoder::parseResponse);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /*
        relevant part of response:
            {
                "embedding": (vector:List<Double>)
            }
        */
        static float[] parseResponse(InputStream inputStream) throws MalformedGenAIResponseException {
            final JsonNode tree;
            final ObjectMapper objectMapper = JsonUtils.getObjectMapper();
            try {
                tree = objectMapper.readTree(inputStream);
            } catch (IOException e) {
                throw new MalformedGenAIResponseException(
                        "Unexpected error occurred while parsing the API response", e);
            }

            final var embedding = getExpectedFrom(tree, "embedding");
            if (!embedding.isArray()) {
                throw new MalformedGenAIResponseException("Expected embedding to be an array");
            }

            try (final var parser = embedding.traverse(objectMapper)) {
                return parser.readValueAs(TYPE_REF_FLOAT_VECTOR);
            } catch (IOException e) {
                throw new MalformedGenAIResponseException("Unexpected error occurred while parsing the embedding", e);
            }
        }

        private static JsonNode getExpectedFrom(JsonNode json, String property) throws MalformedGenAIResponseException {
            return JsonUtils.getExpectedFrom(NAME, json, property);
        }

        private String createRequestBody(String resource) throws IOException {
            return JsonUtils.getObjectMapper().writeValueAsString(buildPayload(resource));
        }
    }

    private static class FallbackModel implements Model {
        private static final FallbackModel INSTANCE = new FallbackModel();

        private FallbackModel() {}

        @Override
        public Encoder encoder(URI endpoint, Parameters configuration) {
            return new FallbackEncoder(endpoint, configuration);
        }
    }

    private static class FallbackEncoder extends Encoder {
        private FallbackEncoder(URI endpoint, Parameters configuration) {
            super(endpoint, configuration);
        }

        /*
         payload:
            {
               "inputText": (resource:String),
               "normalized": (normalize:boolean)
           }
        */
        @Override
        protected Object buildPayload(String resource) {
            return Maps.mutable.of("inputText", resource);
        }
    }

    private static class TitanEmbedTextG1Model implements Model {
        private static final TitanEmbedTextG1Model INSTANCE = new TitanEmbedTextG1Model();

        private TitanEmbedTextG1Model() {}

        private static final String NAME = "amazon.titan-embed-text-v1";

        @Override
        public Encoder encoder(URI endpoint, Parameters configuration) {
            return new TitanEmbedTextG1Encoder(endpoint, configuration);
        }
    }

    private static class TitanEmbedTextG1Encoder extends Encoder {
        private TitanEmbedTextG1Encoder(URI endpoint, Parameters configuration) {
            super(endpoint, configuration);
        }

        /*
         payload:
            {
               "inputText": (resource:String),
               "normalized": (normalize:boolean)
           }
        */
        @Override
        protected Object buildPayload(String resource) {
            return Maps.mutable.of("inputText", resource);
        }
    }

    private static class TitanEmbedImageG1Model implements Model {
        private static final TitanEmbedImageG1Model INSTANCE = new TitanEmbedImageG1Model();
        private static final String NAME = "amazon.titan-embed-image-v1";
        private static final IntIterable VALID_DIMENSIONS = IntSets.immutable.of(256, 384, 1024);
        private static final String STRINGIFIED_VALID_DIMENSIONS = VALID_DIMENSIONS.makeString("[", ", ", "]");

        private TitanEmbedImageG1Model() {}

        @Override
        public void validateConfiguration(Parameters configuration) {
            if (configuration.dimensions.isPresent()) {
                final long dimensions = configuration.dimensions.getAsLong();
                if (!VALID_DIMENSIONS.contains((int) dimensions)) {
                    throw new IllegalArgumentException(
                            "Provided dimensions '%d' is not supported. Supported dimensions: %s"
                                    .formatted(dimensions, STRINGIFIED_VALID_DIMENSIONS));
                }
            }
        }

        @Override
        public Encoder encoder(URI endpoint, Parameters configuration) {
            return new TitanEmbedImageG1Encoder(endpoint, configuration);
        }
    }

    private static class TitanEmbedImageG1Encoder extends Encoder {
        private TitanEmbedImageG1Encoder(URI endpoint, Parameters configuration) {
            super(endpoint, configuration);
        }

        /*
         payload:
            {
               "inputText": (resource:String),
               "embeddingConfig": { "outputEmbeddingLength": (dimensions:int) }
           }
        */
        @Override
        protected Object buildPayload(String resource) {
            final Map<String, Object> payload = Maps.mutable.of("inputText", resource);
            configuration.dimensions.ifPresent(
                    dimensions -> payload.put("embeddingConfig", Maps.mutable.of("outputEmbeddingLength", dimensions)));
            return payload;
        }
    }

    private static class TitanEmbedTextV2Model implements Model {
        private static final TitanEmbedTextV2Model INSTANCE = new TitanEmbedTextV2Model();
        private static final String NAME = "amazon.titan-embed-text-v2:0";
        private static final IntIterable VALID_DIMENSIONS = IntSets.immutable.of(256, 512, 1024);
        private static final String STRINGIFIED_VALID_DIMENSIONS = VALID_DIMENSIONS.makeString("[", ", ", "]");

        private TitanEmbedTextV2Model() {}

        @Override
        public void validateConfiguration(Parameters configuration) {
            if (configuration.dimensions.isPresent()) {
                final long dimensions = configuration.dimensions.getAsLong();
                if (!VALID_DIMENSIONS.contains((int) dimensions)) {
                    throw new IllegalArgumentException(
                            "Provided dimensions '%d' is not supported. Supported dimensions: %s"
                                    .formatted(dimensions, STRINGIFIED_VALID_DIMENSIONS));
                }
            }
        }

        @Override
        public Encoder encoder(URI endpoint, Parameters configuration) {
            return new TitanEmbedTextV2Encoder(endpoint, configuration);
        }
    }

    private static class TitanEmbedTextV2Encoder extends Encoder {
        private TitanEmbedTextV2Encoder(URI endpoint, Parameters configuration) {
            super(endpoint, configuration);
        }

        /*
         payload:
            {
               "inputText": (resource:String),
               "dimensions": (dimensions:int),
               "normalized": (normalize:boolean)
           }
        */
        @Override
        protected Object buildPayload(String resource) {
            final Map<String, Object> payload = Maps.mutable.of("inputText", resource);
            configuration.dimensions.ifPresent(dimensions -> payload.put("dimensions", dimensions));
            configuration.normalize.ifPresent(normalize -> payload.put("normalize", normalize));
            return payload;
        }
    }
}
