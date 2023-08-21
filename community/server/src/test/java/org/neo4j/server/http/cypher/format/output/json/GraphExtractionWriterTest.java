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
package org.neo4j.server.http.cypher.format.output.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.Link.link;
import static org.neo4j.test.mockito.mock.Property.property;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.http.cypher.entity.HttpNode;
import org.neo4j.server.http.cypher.entity.HttpRelationship;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.mockito.mock.Property;

class GraphExtractionWriterTest {
    private final Node n1 = new HttpNode("17", 17, List.of(label("Foo")), Map.of("name", "n1"), false);
    private final Node n2 = new HttpNode("666", 666, emptyList(), Map.of("name", "n2"), false);
    private final Node n3 = new HttpNode("42", 42, List.of(label("Foo"), label("Bar")), Map.of("name", "n3"), false);

    private final Map<Long, Node> nodes = Map.of(n1.getId(), n1, n2.getId(), n2, n3.getId(), n3);

    private final Relationship r1 =
            new HttpRelationship("7", 7, "17", 17, "666", 666, "ONE", Map.of("name", "r1"), false, this::getNodeById);
    private final Relationship r2 =
            new HttpRelationship("8", 8, "17", 17, "42", 42, "TWO", Map.of("name", "r2"), false, this::getNodeById);
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    void shouldExtractNodesFromRow() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put("n1", n1);
        row.put("n2", n2);
        row.put("n3", n3);
        row.put("other.thing", "hello");
        row.put("some.junk", 0x0099cc);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertEquals(0, result.get("graph").get("relationships").size(), "there should be no relationships");
    }

    @Test
    void shouldExtractRelationshipsFromRowAndNodesFromRelationships() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put("r1", r1);
        row.put("r2", r2);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    @Test
    void shouldExtractPathFromRowAndExtractNodesAndRelationshipsFromPath() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put("p", path(n2, link(r1, n1), link(r2, n3)));

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    @Test
    void shouldExtractGraphFromMapInTheRow() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        row.put("map", map);
        map.put("r1", r1);
        map.put("r2", r2);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    @Test
    void shouldExtractGraphFromListInTheRow() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        List<Object> list = new ArrayList<>();
        row.put("list", list);
        list.add(r1);
        list.add(r2);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    @Test
    void shouldExtractGraphFromListInMapInTheRow() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        List<Object> list = new ArrayList<>();
        map.put("list", list);
        row.put("map", map);
        list.add(r1);
        list.add(r2);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    @Test
    void shouldExtractGraphFromMapInListInTheRow() throws Exception {
        // given
        Map<String, Object> row = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        List<Object> list = new ArrayList<>();
        list.add(map);
        row.put("list", list);
        map.put("r1", r1);
        map.put("r2", r2);

        // when
        JsonNode result = write(row);

        // then
        assertNodes(result);
        assertRelationships(result);
    }

    // The code under test

    private JsonNode write(Map<String, Object> row) throws IOException, JsonParseException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator json = jsonFactory.createGenerator(out);
        json.writeStartObject();
        try {
            RecordEvent recordEvent = new RecordEvent(new ArrayList<>(row.keySet()), row::get);
            new GraphExtractionWriter().write(json, recordEvent);
        } finally {
            json.writeEndObject();
            json.flush();
        }
        return JsonHelper.jsonNode(out.toString(UTF_8.name()));
    }

    // The expected format of the result

    private static void assertNodes(JsonNode result) {
        JsonNode nodes = result.get("graph").get("nodes");
        assertEquals(3, nodes.size(), "there should be 3 nodes");
        assertNode("17", "17", nodes, asList("Foo"), property("name", "n1"));
        assertNode("666", "666", nodes, Arrays.asList(), property("name", "n2"));
        assertNode("42", "42", nodes, asList("Foo", "Bar"), property("name", "n3"));
    }

    private static void assertRelationships(JsonNode result) {
        JsonNode relationships = result.get("graph").get("relationships");
        assertEquals(2, relationships.size(), "there should be 2 relationships");
        assertRelationship("7", "7", relationships, "17", "17", "ONE", "666", "666", property("name", "r1"));
        assertRelationship("8", "8", relationships, "17", "17", "TWO", "42", "42", property("name", "r2"));
    }

    // Helpers

    private static void assertNode(
            String id, String elementId, JsonNode nodes, List<String> labels, Property... properties) {
        JsonNode node = get(nodes, elementId);
        assertThat(node.get("id").asText()).isEqualTo(id);
        assertThat(node.get("elementId").asText()).isEqualTo(elementId);
        assertListEquals("Node[" + id + "].labels", labels, node.get("labels"));
        JsonNode props = node.get("properties");
        assertEquals(properties.length, props.size(), "length( Node[" + id + "].properties )");
        for (Property property : properties) {
            assertJsonEquals(
                    "Node[" + id + "].properties[" + property.key() + "]", property.value(), props.get(property.key()));
        }
    }

    private static void assertRelationship(
            String id,
            String elementId,
            JsonNode relationships,
            String startNodeId,
            String startNodeElementId,
            String type,
            String endNodeId,
            String endNodeElementId,
            Property... properties) {
        JsonNode relationship = get(relationships, elementId);
        assertThat(relationship.get("id").asText()).isEqualTo(id);
        assertThat(relationship.get("elementId").asText()).isEqualTo(elementId);
        assertThat(relationship.get("startNode").asText()).isEqualTo(startNodeId);
        assertThat(relationship.get("startNodeElementId").asText()).isEqualTo(startNodeElementId);
        assertThat(relationship.get("endNode").asText()).isEqualTo(endNodeId);
        assertThat(relationship.get("endNodeElementId").asText()).isEqualTo(endNodeElementId);
        assertEquals(type, relationship.get("type").asText(), "Relationship[" + id + "].labels");
        assertEquals(startNodeId, relationship.get("startNode").asText(), "Relationship[" + id + "].startNode");
        assertEquals(endNodeId, relationship.get("endNode").asText(), "Relationship[" + id + "].endNode");
        JsonNode props = relationship.get("properties");
        assertEquals(properties.length, props.size(), "length( Relationship[" + id + "].properties )");
        for (Property property : properties) {
            assertJsonEquals(
                    "Relationship[" + id + "].properties[" + property.key() + "]",
                    property.value(),
                    props.get(property.key()));
        }
    }

    private static void assertJsonEquals(String message, Object expected, JsonNode actual) {
        if (expected == null) {
            Assertions.assertTrue(actual == null || actual.isNull(), message);
        } else if (expected instanceof String) {
            assertEquals(expected, actual.asText(), message);
        } else if (expected instanceof Number) {
            assertEquals(expected, actual.asInt(), message);
        } else {
            Assertions.fail(message + " - unexpected type - " + expected);
        }
    }

    private static void assertListEquals(String what, List<String> expected, JsonNode jsonNode) {
        Assertions.assertTrue(jsonNode.isArray(), what + " - should be a list");
        List<String> actual = new ArrayList<>(jsonNode.size());
        for (JsonNode node : jsonNode) {
            actual.add(node.asText());
        }
        assertEquals(expected, actual, what);
    }

    private static JsonNode get(Iterable<JsonNode> jsonNodes, String elementId) {
        for (JsonNode jsonNode : jsonNodes) {
            if (elementId.equals(jsonNode.get("elementId").asText())) {
                return jsonNode;
            }
        }
        return null;
    }

    private Optional<Node> getNodeById(Long id, Boolean isDeleted) {
        return Optional.ofNullable(nodes.getOrDefault(id, null));
    }
}
