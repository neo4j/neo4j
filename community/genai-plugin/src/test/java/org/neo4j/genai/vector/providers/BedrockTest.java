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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.vector.VectorEncoding.BatchRow;
import org.neo4j.genai.vector.providers.Bedrock.Encoder;

class BedrockTest {
    private static final Bedrock PROVIDER = new Bedrock();

    @Nested
    class Configuration extends ConfigurationTestBase<Bedrock.Parameters> {
        protected Configuration() {
            super(
                    PROVIDER,
                    List.of(
                            new RequiredSetting("accessKeyId", String.class, "STRING", "FAKEACCESSKEYID", 123),
                            new RequiredSetting("secretAccessKey", String.class, "STRING", "FaKe/ACc3ss/kEy", 123)),
                    List.of(
                            new OptionalSetting(
                                    "region",
                                    String.class,
                                    "STRING",
                                    Bedrock.DEFAULT_REGION,
                                    123,
                                    Optional.of(Bedrock.DEFAULT_REGION)),
                            new OptionalSetting(
                                    "model",
                                    String.class,
                                    "STRING",
                                    Bedrock.DEFAULT_MODEL,
                                    123,
                                    Optional.of(Bedrock.DEFAULT_MODEL))),
                    new Models("model", String.class, Bedrock.SUPPORTED_MODELS, List.of("fake-model")));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void supportedRegions(Object region) {
            config.put("region", region);
            assertThatCode(() -> configure(config)).as("supported region").doesNotThrowAnyException();
        }

        private Stream<String> supportedRegions() {
            return Bedrock.SUPPORTED_REGIONS.stream();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void unsupportedRegions(Object region) {
            config.put("region", region);
            assertThatThrownBy(() -> configure(config)).isInstanceOf(IllegalArgumentException.class);
        }

        private Stream<Object> unsupportedRegions() {
            return Stream.of("fake-region", "eve.example.org/?");
        }
    }

    private record Regions(String setting, Class<?> type, Collection<?> supported, Collection<?> unsupported) {}

    @Nested
    class Parsing extends ParsingTestBase {
        protected Parsing() {
            super((resources, stream, nullIndexes) ->
                    Stream.of(new BatchRow(0, resources.get(0), Encoder.parseResponse(stream))));
        }

        @Override
        Collection<Arguments> parseResponse() {
            return List.of(
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "embedding": [1.0, 2.0, 3.0]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "embedding": [1, 2, 3]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "embedding": ["1.0", "2.0", "3.0"]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "Unexpected error occurred while parsing the API response"),
                            """
                            {
                                "embedding": ['1.0', '2.0', '3.0']
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "embedding", "is expected to exist in the response from Bedrock"),
                            """
                            {
                                "vector": [1.0, 2.0, 3.0]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected embedding to be an array"),
                            """
                            {
                                "embedding": 123
                            }
                            """));
        }
    }
}
