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
package org.neo4j.server.http.cypher.integration;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

/**
 * Matchers and assertion methods for the transactional endpoint.
 */
public final class TransactionConditions {
    private TransactionConditions() {}

    static Condition<String> validRFCTimestamp() {
        return new Condition<>(
                value -> {
                    try {
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                },
                "Valid RFC1134 timestamp.");
    }

    public static Consumer<HTTP.Response> containsNoErrors() {
        return hasErrors();
    }

    public static Consumer<HTTP.Response> hasErrors(final Status... expectedErrors) {
        return response -> {
            try {
                Iterator<JsonNode> errors = response.get("errors").iterator();
                Iterator<Status> expected = iterator(expectedErrors);

                while (expected.hasNext()) {
                    assertThat(errors.hasNext()).isTrue();
                    assertThat(errors.next().get("code").asText())
                            .isEqualTo(expected.next().code().serialize());
                }
                if (errors.hasNext()) {
                    JsonNode error = errors.next();
                    Assertions.fail("Expected no more errors, but got " + error.get("code") + " - '"
                            + error.get("message") + "'.");
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> hasOneErrorOf(final Status... expectedErrors) {
        return response -> {
            try {
                // we just need to compare single error but we could expand this later.
                var error = response.get("errors").get(0);
                Iterator<Status> expected = iterator(expectedErrors);

                var errorFound = false;

                while (!errorFound && expected.hasNext()) {
                    errorFound = error.get("code")
                            .asText()
                            .equals(expected.next().code().serialize());
                }

                if (errorFound == false) {
                    Assertions.fail("Error " + errorFound + " does not match any of the following expected errors "
                            + Arrays.stream(expectedErrors)
                                    .map(s -> s.code().serialize())
                                    .toList());
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    static JsonNode getJsonNodeWithName(HTTP.Response response, String name) throws JsonParseException {
        return response.get("results").get(0).get("data").get(0).get(name);
    }

    public static Consumer<HTTP.Response> rowContainsDeletedEntities(final int nodes, final int rels) {
        return response -> {
            try {
                Iterator<JsonNode> meta = getJsonNodeWithName(response, "meta").iterator();

                int nodeCounter = 0;
                int relCounter = 0;
                for (int i = 0; i < nodes + rels; ++i) {
                    assertThat(meta.hasNext()).isTrue();
                    JsonNode node = meta.next();
                    assertThat(node.get("deleted").asBoolean()).isEqualTo(Boolean.TRUE);
                    String type = node.get("type").asText();
                    switch (type) {
                        case "node":
                            ++nodeCounter;
                            break;
                        case "relationship":
                            ++relCounter;
                            break;
                        default:
                            Assertions.fail("Unexpected type: " + type);
                            break;
                    }
                }
                assertThat(nodes).isEqualTo(nodeCounter);
                assertThat(rels).isEqualTo(relCounter);
                while (meta.hasNext()) {
                    JsonNode node = meta.next();
                    assertThat(node.get("deleted").asBoolean()).isEqualTo(Boolean.FALSE);
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsDeletedEntitiesInPath(final int nodes, final int rels) {
        return response -> {
            try {
                Iterator<JsonNode> meta = getJsonNodeWithName(response, "meta").iterator();

                int nodeCounter = 0;
                int relCounter = 0;
                assertThat(meta.hasNext())
                        .describedAs("Expected to find a JSON node, but there was none")
                        .isTrue();
                JsonNode node = meta.next();
                assertThat(node.isArray())
                        .describedAs("Expected the node to be a list (for a path)")
                        .isTrue();
                for (JsonNode inner : node) {
                    String type = inner.get("type").asText();
                    switch (type) {
                        case "node":
                            if (inner.get("deleted").asBoolean()) {
                                ++nodeCounter;
                            }
                            break;
                        case "relationship":
                            if (inner.get("deleted").asBoolean()) {
                                ++relCounter;
                            }
                            break;
                        default:
                            Assertions.fail("Unexpected type: " + type);
                            break;
                    }
                }
                assertThat(nodes).isEqualTo(nodeCounter);
                assertThat(rels).isEqualTo(relCounter);
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsMetaNodesAtIndex(int... indexes) {
        return response -> assertElementAtMetaIndex(response, indexes, "node");
    }

    public static Consumer<HTTP.Response> rowContainsMetaRelsAtIndex(int... indexes) {
        return response -> assertElementAtMetaIndex(response, indexes, "relationship");
    }

    private static void assertElementAtMetaIndex(HTTP.Response response, int[] indexes, String element) {
        try {
            Iterator<JsonNode> meta = getJsonNodeWithName(response, "meta").iterator();

            int i = 0;
            for (int metaIndex = 0; meta.hasNext() && i < indexes.length; metaIndex++) {
                JsonNode node = meta.next();
                if (!node.isNull()) {
                    String type = node.get("type").asText();
                    if (type.equals(element)) {
                        assertThat(indexes[i])
                                .describedAs("Expected " + element + " to be at indexes " + Arrays.toString(indexes)
                                        + ", but found it at " + metaIndex)
                                .isEqualTo(metaIndex);
                        ++i;
                    } else {
                        assertThat(indexes[i])
                                .describedAs("Expected " + element + " at index " + metaIndex + ", but found " + type)
                                .isNotEqualTo(metaIndex);
                    }
                }
            }
            assertThat(indexes.length).isEqualTo(i);
        } catch (JsonParseException e) {
            assertThat(e).isNull();
        }
    }

    public static Consumer<HTTP.Response> rowContainsAMetaListAtIndex(int index) {
        return response -> {
            try {
                Iterator<JsonNode> meta = getJsonNodeWithName(response, "meta").iterator();

                for (int metaIndex = 0; meta.hasNext(); metaIndex++) {
                    JsonNode node = meta.next();
                    if (metaIndex == index) {
                        assertThat(node.isArray()).isTrue();
                    }
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> restContainsDeletedEntities(final int amount) {
        return response -> {
            try {
                Iterator<JsonNode> entities =
                        getJsonNodeWithName(response, "rest").iterator();

                for (int i = 0; i < amount; ++i) {
                    assertThat(entities.hasNext()).isTrue();
                    JsonNode node = entities.next();
                    assertThat(node.get("metadata").get("deleted").asBoolean()).isEqualTo(Boolean.TRUE);
                }
                if (entities.hasNext()) {
                    Assertions.fail("Expected no more entities");
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsDeletedNodes(final int amount) {
        return response -> {
            try {
                Iterator<JsonNode> nodes =
                        getJsonNodeWithName(response, "graph").get("nodes").iterator();
                int deleted = 0;
                while (nodes.hasNext()) {
                    JsonNode node = nodes.next();
                    if (node.get("deleted") != null) {
                        assertThat(node.get("deleted").asBoolean()).isTrue();
                        deleted++;
                    }
                }
                assertThat(amount)
                        .describedAs(
                                format("Expected to see %d deleted elements but %d was encountered.", amount, deleted))
                        .isEqualTo(deleted);
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsNoDeletedEntities() {
        return response -> {
            try {
                for (JsonNode node : getJsonNodeWithName(response, "graph").get("nodes")) {
                    assertThat(node.get("deleted")).isNull();
                }
                for (JsonNode node : getJsonNodeWithName(response, "graph").get("relationships")) {
                    assertThat(node.get("deleted")).isNull();
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsNoDeletedEntities() {
        return response -> {
            try {
                for (JsonNode node : getJsonNodeWithName(response, "meta")) {
                    assertThat(node.get("deleted").asBoolean()).isFalse();
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> restContainsNoDeletedEntities() {
        return response -> {
            try {
                for (JsonNode node : getJsonNodeWithName(response, "rest")) {
                    assertThat(node.get("metadata").get("deleted")).isNull();
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsDeletedRelationships(final int amount) {
        return response -> {
            try {
                Iterator<JsonNode> relationships = getJsonNodeWithName(response, "graph")
                        .get("relationships")
                        .iterator();

                for (int i = 0; i < amount; ++i) {
                    assertThat(relationships.hasNext()).isTrue();
                    JsonNode node = relationships.next();
                    assertThat(node.get("deleted").asBoolean()).isEqualTo(Boolean.TRUE);
                }
                if (relationships.hasNext()) {
                    JsonNode node = relationships.next();
                    Assertions.fail("Expected no more nodes, but got a node with id " + node.get("id"));
                }
            } catch (JsonParseException e) {
                assertThat(e).isNull();
            }
        };
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public static long countNodes(GraphDatabaseService graphdb) {
        try (Transaction transaction = graphdb.beginTx()) {
            return Iterables.count(transaction.getAllNodes());
        }
    }

    public static Condition<? super HTTP.Response> containsNoStackTraces() {
        return new Condition<>(
                response -> {
                    Map<String, Object> content = response.content();
                    var errors = (List<Map<String, Object>>) content.get("errors");

                    for (Map<String, Object> error : errors) {
                        if (error.containsKey("stackTrace")) {
                            return false;
                        }
                    }
                    return true;
                },
                "Contains stack traces.");
    }
}
