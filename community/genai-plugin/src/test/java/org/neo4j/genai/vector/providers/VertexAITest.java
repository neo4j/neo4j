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
import org.neo4j.genai.vector.providers.VertexAI.Encoder;

class VertexAITest {
    private static final VertexAI PROVIDER = new VertexAI();

    @Nested
    class Configuration extends ConfigurationTestBase<VertexAI.Parameters> {
        protected Configuration() {
            super(
                    PROVIDER,
                    List.of(
                            new RequiredSetting("token", String.class, "STRING", "FAke-t0k3n", 123),
                            new RequiredSetting("projectId", String.class, "STRING", "fake-project", 123)),
                    List.of(
                            new OptionalSetting(
                                    "region",
                                    String.class,
                                    "STRING",
                                    VertexAI.DEFAULT_REGION,
                                    123,
                                    Optional.of(VertexAI.DEFAULT_REGION)),
                            new OptionalSetting(
                                    "model",
                                    String.class,
                                    "STRING",
                                    VertexAI.DEFAULT_MODEL,
                                    123,
                                    Optional.of(VertexAI.DEFAULT_MODEL)),
                            new OptionalSetting(
                                    "taskType", String.class, "STRING", "RETRIEVAL_DOCUMENT", 123, Optional.empty()),
                            new OptionalSetting(
                                    "title", String.class, "STRING", "A Short Tale", 123, Optional.empty())),
                    new Models("model", String.class, VertexAI.SUPPORTED_MODELS, List.of("fake-model")));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void supportedRegions(Object region) {
            config.put("region", region);
            assertThatCode(() -> configure(config)).as("supported region").doesNotThrowAnyException();
        }

        private Stream<String> supportedRegions() {
            return VertexAI.SUPPORTED_REGIONS.stream();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void unsupportedRegions(Object region) {
            config.put("region", region);
            assertThatThrownBy(() -> configure(config), "unsupported region")
                    .isInstanceOf(IllegalArgumentException.class);
        }

        private Stream<Object> unsupportedRegions() {
            return Stream.of("fake-region", "eve.example.org/?");
        }
    }

    @Nested
    class Parsing extends ParsingTestBase {
        protected Parsing() {
            super(Encoder::parseResponse);
        }

        @Override
        Collection<Arguments> parseResponse() {
            return List.of(
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": [1.0, 2.0, 3.0]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": [1, 2, 3]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": ["1.0", "2.0", "3.0"]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(
                                    List.of("hello", "olleh"),
                                    List.of(new float[] {1.f, 2.f, 3.f}, new float[] {3.f, 2.f, 1.f})),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": ["1.0", "2.0", "3.0"]
                                    }
                                },{
                                    "embeddings": {
                                        "values": ["3.0", "2.0", "1.0"]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "Unexpected error occurred while parsing the API response"),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": ['1.0', '2.0', '3.0']
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"),
                                    "predictions",
                                    "is expected to exist in the response from VertexAI"),
                            """
                            {
                                "data": [{
                                    "embeddings": {
                                        "values": [1.0, 2.0, 3.0]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected response to contain an array of embeddings"),
                            """
                            {
                                "predictions": 123
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected to receive 1", "however got 2"),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": ["1.0", "2.0", "3.0"]
                                    }
                                },{
                                    "embeddings": {
                                        "values": ["3.0", "2.0", "1.0"]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"),
                                    "embeddings",
                                    "is expected to exist in the response from VertexAI"),
                            """
                            {
                                "predictions": [{
                                    "vectors": {
                                        "values": [1.0, 2.0, 3.0]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected 'embeddings' to be an object"),
                            """
                            {
                                "predictions": [{
                                    "embeddings": 123
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "values", "is expected to exist in the response from VertexAI"),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "vector": [1.0, 2.0, 3.0]
                                    }
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected embedding to be an array"),
                            """
                            {
                                "predictions": [{
                                    "embeddings": {
                                        "values": 123
                                    }
                                }]
                            }
                            """));
        }
    }
}
