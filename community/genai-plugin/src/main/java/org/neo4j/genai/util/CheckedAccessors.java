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

import com.fasterxml.jackson.databind.JsonNode;
import org.neo4j.genai.vector.MalformedGenAIResponseException;

public class CheckedAccessors {
    private CheckedAccessors() {}

    public static class Json {
        private Json() {}

        public static JsonNode getExpectedFrom(String provider, JsonNode json, String property)
                throws MalformedGenAIResponseException {
            try {
                if (!json.isObject()) {
                    throw isNotObjectNode("provided json node");
                }
                return json.required(property);
            } catch (IllegalArgumentException e) {
                throw doesNotExist(provider, property, e);
            }
        }

        public static JsonNode getExpectedFrom(String provider, JsonNode json, String... properties)
                throws MalformedGenAIResponseException {
            var parent = "provided json node";
            for (final var property : properties) {
                if (!json.isObject()) {
                    throw isNotObjectNode(parent);
                }

                json = getExpectedFrom(provider, json, property);
                parent = "'" + property + "'";
            }
            return json;
        }

        private static MalformedGenAIResponseException isNotObjectNode(String parent) {
            return new MalformedGenAIResponseException("Expected %s to be an object".formatted(parent));
        }

        private static MalformedGenAIResponseException doesNotExist(String provider, String property, Throwable cause) {
            return new MalformedGenAIResponseException(
                    "'%s' is expected to exist in the response from %s".formatted(property, provider), cause);
        }
    }
}
