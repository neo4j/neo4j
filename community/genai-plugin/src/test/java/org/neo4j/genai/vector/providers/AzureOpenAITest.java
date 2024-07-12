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
import org.neo4j.genai.vector.providers.AzureOpenAI.Encoder;

class AzureOpenAITest {
    private static final AzureOpenAI PROVIDER = new AzureOpenAI();

    @Nested
    class Configuration extends ConfigurationTestBase<AzureOpenAI.Parameters> {
        protected Configuration() {
            super(
                    PROVIDER,
                    List.of(
                            new RequiredSetting("token", String.class, "STRING", "FAke-t0k3n", 123),
                            new RequiredSetting("resource", String.class, "STRING", "a-valid-resource", 123),
                            new RequiredSetting("deployment", String.class, "STRING", "some-valid-deployment", 123)),
                    List.of(new OptionalSetting("dimensions", Long.class, "INTEGER", 1024, "1024", Optional.empty())),
                    // TODO: for Azure OpenAI, the model is configured when setting up the deployment.
                    //   The test model here was built with the assumption that we will always specify a model.
                    //   This has been shoehorned into that model for now, but that assumption should be revisited.
                    Models.IMPLICIT);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void validResources(String resource) {
            config.put("resource", resource);
            assertThatCode(() -> configure(config)).as("valid resource").doesNotThrowAnyException();
        }

        private Stream<String> validResources() {
            return Stream.of(
                    "ab", // shortest
                    "valid-resource-123",
                    "CapItAls",
                    "supercalifragilisticexpialidocious-is-a-veeeeeeeeeeery-long-word" // longest
                    );
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void invalidResources(String resource) {
            config.put("resource", resource);
            assertThatThrownBy(() -> configure(config))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Provided resource '%s' is invalid. It must consist of".formatted(resource));
        }

        private Stream<String> invalidResources() {
            return Stream.of(
                    "x", // one too short
                    "xsupercalifragilisticexpialidocious-is-a-veeeeeeeeeeery-long-word", // one too long
                    "-cant-start-on-hyphen",
                    "cant-end-on-hyphen-",
                    "cant.have.periods",
                    "sneaky-injection/",
                    "sneaky-injection?",
                    "sneaky-injection#",
                    "sneaky/injection",
                    "sneaky?injection",
                    "sneaky#injection");
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void validDeployments(String deployment) {
            config.put("deployment", deployment);
            assertThatCode(() -> configure(config)).as("valid deployment").doesNotThrowAnyException();
        }

        private Stream<String> validDeployments() {
            return Stream.of(
                    "ab", // shortest
                    "valid-resource-123",
                    "123_Valid_Resource",
                    "_can_start_with_underscore",
                    "-can-start-with-hyphen",
                    "CapItAls",
                    "supercalifragilisticexpialidocious-is-a-veeeeeeeeeeery-long-word" // longest
                    );
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource
        void invalidDeployments(String deployment) {
            config.put("deployment", deployment);
            assertThatThrownBy(() -> configure(config))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Provided deployment '%s' is invalid. It must consist of".formatted(deployment));
        }

        private Stream<String> invalidDeployments() {
            // The name can only include alphanumeric characters, _ character and - character. Can't end with '_' or
            // '-'.
            return Stream.of(
                    "x", // one too short
                    "xsupercalifragilisticexpialidocious-is-a-veeeeeeeeeeery-long-word", // one too long
                    "cant-end-on-hyphen-",
                    "cant_end_on_underscore_",
                    "cant.have.periods",
                    "sneaky-injection/",
                    "sneaky-injection?",
                    "sneaky-injection#",
                    "sneaky/injection",
                    "sneaky?injection",
                    "sneaky#injection");
        }
    }

    @Nested
    class Parsing extends ParsingTestBase {
        protected Parsing() {
            super((resources, inputStream, nullIndexes) ->
                    Encoder.parseResponse(AzureOpenAI.NAME, resources, inputStream, nullIndexes));
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
                                    List.of("hello"), "data", "is expected to exist in the response from AzureOpenAI"),
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
                                    List.of("hello"),
                                    "embedding",
                                    "is expected to exist in the response from AzureOpenAI"),
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
