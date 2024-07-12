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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.genai.vector.providers.OpenAI.Encoder;

class OpenAITest {
    private static final OpenAI PROVIDER = new OpenAI();
    private static final List<String> KNOWN_MODELS =
            List.of("text-embedding-ada-002", "text-embedding-3-small", "text-embedding-3-large");

    @Nested
    class Configuration extends ConfigurationTestBase<OpenAI.Parameters> {
        protected Configuration() {
            super(
                    PROVIDER,
                    List.of(new RequiredSetting("token", String.class, "STRING", "FAke-t0k3n", 123)),
                    List.of(
                            new OptionalSetting(
                                    "model",
                                    String.class,
                                    "STRING",
                                    OpenAI.DEFAULT_MODEL,
                                    123,
                                    Optional.of(OpenAI.DEFAULT_MODEL)),
                            new OptionalSetting("dimensions", Long.class, "INTEGER", 1024, "1024", Optional.empty())),
                    new Models("model", String.class, KNOWN_MODELS, List.of()));
        }
    }

    @Nested
    class Parsing extends ParsingTestBase {
        protected Parsing() {
            super((resources, inputStream, nullIndexes) ->
                    Encoder.parseResponse(OpenAI.NAME, resources, inputStream, nullIndexes));
        }

        @Override
        Collection<Arguments> parseResponse() {
            return List.of(
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "data": [{
                                    "embedding": [1.0, 2.0, 3.0]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "data": [{
                                    "embedding": [1, 2, 3]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(List.of("hello"), List.of(new float[] {1.f, 2.f, 3.f})),
                            """
                            {
                                "data": [{
                                    "embedding": ["1.0", "2.0", "3.0"]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedVectors(
                                    List.of("hello", "olleh"),
                                    List.of(new float[] {1.f, 2.f, 3.f}, new float[] {3.f, 2.f, 1.f})),
                            """
                            {
                                "data": [{
                                    "embedding": [1.0, 2.0, 3.0]
                                },{
                                    "embedding": [3.0, 2.0, 1.0]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "Unexpected error occurred while parsing the API response"),
                            """
                            {
                                "data": [{
                                    "embedding": ['1.0', '2.0', '3.0']
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "data", "is expected to exist in the response from OpenAI"),
                            """
                            {
                                "output": [{
                                    "embedding": [1.0, 2.0, 3.0]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected response to contain an array of embeddings"),
                            """
                            {
                                "data": 123
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected to receive 1", "however got 2"),
                            """
                            {
                                "data": [{
                                    "embedding": [1.0, 2.0, 3.0]
                                },{
                                    "embedding": [3.0, 2.0, 1.0]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "embedding", "is expected to exist in the response from OpenAI"),
                            """
                            {
                                "data": [{
                                    "vector": [1.0, 2.0, 3.0]
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(List.of("hello"), "Expected embedding to be an array"),
                            """
                            {
                                "data": [{
                                    "embedding": 123
                                }]
                            }
                            """),
                    Arguments.of(
                            new ExpectedError(
                                    List.of("hello"), "Unexpected error occurred while parsing the embedding"),
                            """
                            {
                                "data": [{
                                    "embedding": ["one", "two", "three"]
                                }]
                            }
                            """));
        }
    }
}
