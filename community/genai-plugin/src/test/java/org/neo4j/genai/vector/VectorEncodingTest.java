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
package org.neo4j.genai.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.util.ParametersTest;
import org.neo4j.genai.vector.VectorEncoding.InternalBatchRow;
import org.neo4j.genai.vector.VectorEncoding.Provider;
import org.neo4j.genai.vector.providers.AzureOpenAI;
import org.neo4j.genai.vector.providers.Bedrock;
import org.neo4j.genai.vector.providers.OpenAI;
import org.neo4j.genai.vector.providers.TestProvider;
import org.neo4j.genai.vector.providers.VertexAI;
import org.neo4j.values.storable.Value;

class VectorEncodingTest {
    private static final VectorEncoding VECTOR_ENCODING = new VectorEncoding();

    @Nested
    class Providers {
        @ParameterizedTest(name = "{0} --> {1}")
        @MethodSource
        void expectedProviders(ExpectedProvider expectedProvider) {
            final var provider = VectorEncoding.getProvider(expectedProvider.name());
            assertThat(provider).isExactlyInstanceOf(expectedProvider.cls());
        }

        @Test
        void listProvidersListsExpectedProviders() {
            final var actual = VECTOR_ENCODING
                    .listEncodingProviders()
                    .map(VectorEncoding.ProviderRow::name)
                    .toList();
            final var expected = expectedProviders().map(ExpectedProvider::name).toList();
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        }

        static Stream<ExpectedProvider> expectedProviders() {
            return Stream.of(
                    new ExpectedProvider(TestProvider.NAME, TestProvider.class),
                    new ExpectedProvider(AzureOpenAI.NAME, AzureOpenAI.class),
                    new ExpectedProvider(Bedrock.NAME, Bedrock.class),
                    new ExpectedProvider(OpenAI.NAME, OpenAI.class),
                    new ExpectedProvider(VertexAI.NAME, VertexAI.class));
        }

        record ExpectedProvider(String name, Class<? extends Provider> cls) {}

        @Test
        void shouldThrowOnUnsupportedProvider() {
            final var unsupported = "unsupported";
            assertThatThrownBy(() -> VectorEncoding.getProvider(unsupported))
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessageContainingAll("Vector encoding provider not supported", unsupported);
        }
    }

    @Nested
    class EncodeArguments extends VectorEncodingArgumentBase {
        @Override
        Value single(String resource, String provider, Map<String, ?> configuration) {
            final var config = configuration != null ? ParametersTest.from(configuration) : NO_VALUE;
            return VECTOR_ENCODING.encode(resource, provider, config);
        }

        @Override
        List<Value> batch(List<String> resources, String provider, Map<String, ?> configuration) {
            final var config = configuration != null ? ParametersTest.from(configuration) : NO_VALUE;
            return VECTOR_ENCODING
                    .encode(resources, provider, config)
                    .map(InternalBatchRow::vector)
                    .toList();
        }
    }
}
