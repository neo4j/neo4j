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
package org.neo4j.genai.util;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.genai.util.CheckedAccessors.Json.getExpectedFrom;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.genai.vector.MalformedGenAIResponseException;
import org.neo4j.genai.vector.providers.TestProvider;

class CheckedAccessorsTest {

    @Nested
    class Json {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Test
        void shouldGetExpected() throws IOException {
            final var json = OBJECT_MAPPER.readTree("""
                    { "exists": true }
                    """);

            assertThatCode(() -> getExpectedFrom(TestProvider.NAME, json, "exists"))
                    .as("expected exists")
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowIfMissing() throws IOException {
            final var json = OBJECT_MAPPER.readTree("{}");

            // assumed structure:
            //     { "exists": true }
            assertThatThrownBy(() -> getExpectedFrom(TestProvider.NAME, json, "exists"), "expected missing")
                    .isInstanceOf(MalformedGenAIResponseException.class)
                    .hasMessageContainingAll("exists", "is expected to exist in the response from", TestProvider.NAME);
        }

        @Test
        void shouldThrowIfNotObject() throws IOException {
            final var json = OBJECT_MAPPER.readTree("""
                    { "nested": 123 }
                    """);

            // assumed structure:
            //     { "nested": { "exists": true } }
            final var nested = json.get("nested");
            assertThatThrownBy(() -> getExpectedFrom(TestProvider.NAME, nested, "exists"), "expected missing")
                    .isInstanceOf(MalformedGenAIResponseException.class)
                    .hasMessageContainingAll("Expected", "provided json node", "to be an object");
        }

        @Test
        void shouldGetExpectedNested() throws IOException {
            final var json = OBJECT_MAPPER.readTree(
                    """
                    { "nested": { "exists" : true } }
                    """);

            assertThatCode(() -> getExpectedFrom(TestProvider.NAME, json, "nested", "exists"))
                    .as("expected exists with nesting")
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowIfDeeplyNestedNotObject() throws IOException {
            final var json = OBJECT_MAPPER.readTree("""
                    { "deeply": 123 }
                    """);

            // assumed structure:
            //     { "deeply": { "nested": { "exists": true } } }
            final var deeply = json.get("deeply");
            assertThatThrownBy(() -> getExpectedFrom(TestProvider.NAME, deeply, "nested", "exists"), "expected missing")
                    .isInstanceOf(MalformedGenAIResponseException.class)
                    .hasMessageContainingAll("Expected", "provided json node", "to be an object");
        }

        @Test
        void shouldThrowIfNestedNotObject() throws IOException {
            final var json = OBJECT_MAPPER.readTree("""
                    { "nested": 123 }
                    """);

            // assumed structure:
            //     { "nested": { "exists": true } }
            assertThatThrownBy(() -> getExpectedFrom(TestProvider.NAME, json, "nested", "exists"), "expected missing")
                    .isInstanceOf(MalformedGenAIResponseException.class)
                    .hasMessageContainingAll("Expected", "nested", "to be an object");
        }
    }
}
