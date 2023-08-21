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
package org.neo4j.server.http.cypher.format.jolt.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.neo4j.server.http.cypher.format.jolt.JoltBasicTypesSerializerTest.assertValidJSON;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(MockitoExtension.class)
class JoltV1SerializerTest {

    private final ObjectMapper objectMapper;

    JoltV1SerializerTest() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(JoltModuleV1.STRICT.getInstance());
    }

    void shouldSerializeNode(@Mock Node node) throws JsonProcessingException {
        when(node.getId()).thenReturn(4711L);
        when(node.getLabels()).thenReturn(List.of(Label.label("A"), Label.label("B")));
        when(node.getAllProperties()).thenReturn(new TreeMap<>(Map.of("prop1", 1, "prop2", "Peng")));
        var result = objectMapper.writeValueAsString(node);
        assertValidJSON(result);
        assertThat(result)
                .isEqualTo("{\"()\":[4711,[\"A\",\"B\"],{\"prop1\":{\"Z\":\"1\"},\"prop2\":{\"U\":\"Peng\"}}]}");
    }

    @Test
    void shouldSerializeNodeWithoutLabelsOrProperties(@Mock Node node) throws JsonProcessingException {
        when(node.getId()).thenReturn(4711L);
        when(node.getLabels()).thenReturn(List.of());
        var result = objectMapper.writeValueAsString(node);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"()\":[4711,[],{}]}");
    }

    @Test
    void shouldSerializeRelationship(@Mock Relationship relationship) throws JsonProcessingException {
        when(relationship.getId()).thenReturn(4711L);
        when(relationship.getType()).thenReturn(RelationshipType.withName("KNOWS"));
        when(relationship.getStartNodeId()).thenReturn(123L);
        when(relationship.getEndNodeId()).thenReturn(124L);
        when(relationship.getAllProperties()).thenReturn(Map.of("since", 1999));

        var result = objectMapper.writeValueAsString(relationship);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"->\":[4711,123,\"KNOWS\",124,{\"since\":{\"Z\":\"1999\"}}]}");
    }

    @Test
    void shouldSerializePath(@Mock Path path, @Mock Node start, @Mock Relationship rel, @Mock Node end)
            throws JsonProcessingException {
        when(start.getId()).thenReturn(111L);
        when(start.getElementId()).thenReturn("111");
        when(start.getLabels()).thenReturn(List.of());

        when(end.getId()).thenReturn(222L);
        when(end.getElementId()).thenReturn("222");
        when(end.getLabels()).thenReturn(List.of());

        when(rel.getId()).thenReturn(9090L);
        when(rel.getType()).thenReturn(RelationshipType.withName("KNOWS"));
        when(rel.getStartNodeId()).thenReturn(111L);
        when(rel.getStartNode()).thenReturn(start);
        when(rel.getEndNodeId()).thenReturn(222L);
        when(rel.getAllProperties()).thenReturn(Map.of("since", 1999));

        List<Entity> pathList = List.of(start, rel, end);

        when(path.iterator()).thenReturn(pathList.iterator());

        var result = objectMapper.writeValueAsString(path);
        assertValidJSON(result);
        assertThat(result)
                .isEqualTo("{\"..\":[" + "{\"()\":[111,[],{}]},"
                        + "{\"->\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]},"
                        + "{\"()\":[222,[],{}]}]}");
    }

    @Test
    void shouldSerializeReversedPath(@Mock Path path, @Mock Node start, @Mock Relationship rel, @Mock Node end)
            throws JsonProcessingException {
        when(start.getId()).thenReturn(111L);
        when(start.getElementId()).thenReturn("111");
        when(start.getLabels()).thenReturn(List.of());

        when(end.getId()).thenReturn(222L);
        when(end.getElementId()).thenReturn("222");
        when(end.getLabels()).thenReturn(List.of());

        when(rel.getId()).thenReturn(9090L);
        when(rel.getType()).thenReturn(RelationshipType.withName("KNOWS"));
        when(rel.getStartNodeId()).thenReturn(222L);
        when(rel.getStartNode()).thenReturn(end);
        when(rel.getEndNodeId()).thenReturn(111L);
        when(rel.getEndNode()).thenReturn(start);
        when(rel.getAllProperties()).thenReturn(Map.of("since", 1999));

        List<Entity> pathList = List.of(start, rel, end);

        when(path.iterator()).thenReturn(pathList.iterator());

        var result = objectMapper.writeValueAsString(path);
        assertValidJSON(result);
        assertThat(result)
                .isEqualTo("{\"..\":[" + "{\"()\":[111,[],{}]},"
                        + "{\"<-\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]},"
                        + "{\"()\":[222,[],{}]}]}");
    }

    @Test
    void shouldSerializeLongPath(
            @Mock Path path,
            @Mock Node start,
            @Mock Relationship relA,
            @Mock Node middle,
            @Mock Relationship relB,
            @Mock Node end)
            throws JsonProcessingException {
        when(start.getId()).thenReturn(111L);
        when(start.getElementId()).thenReturn("111");
        when(start.getLabels()).thenReturn(List.of());
        when(middle.getId()).thenReturn(222L);
        when(middle.getElementId()).thenReturn("222");
        when(middle.getLabels()).thenReturn(List.of());
        when(end.getId()).thenReturn(333L);
        when(end.getElementId()).thenReturn("333");
        when(end.getLabels()).thenReturn(List.of());

        when(relA.getId()).thenReturn(9090L);
        when(relA.getType()).thenReturn(RelationshipType.withName("KNOWS"));
        when(relA.getStartNodeId()).thenReturn(111L);
        when(relA.getStartNode()).thenReturn(start);
        when(relA.getEndNodeId()).thenReturn(222L);
        when(relA.getAllProperties()).thenReturn(Map.of("since", 1999));

        when(relB.getId()).thenReturn(9090L);
        when(relB.getType()).thenReturn(RelationshipType.withName("KNOWS"));
        when(relB.getStartNodeId()).thenReturn(333L);
        when(relB.getStartNode()).thenReturn(end);
        when(relB.getEndNodeId()).thenReturn(222L);
        when(relB.getEndNode()).thenReturn(middle);
        when(relB.getAllProperties()).thenReturn(Map.of("since", 1990));

        List<Entity> pathList = List.of(start, relA, middle, relB, end);

        when(path.iterator()).thenReturn(pathList.iterator());

        var result = objectMapper.writeValueAsString(path);
        assertValidJSON(result);
        assertThat(result)
                .isEqualTo("{\"..\":[" + "{\"()\":[111,[],{}]},"
                        + "{\"->\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]},"
                        + "{\"()\":[222,[],{}]},"
                        + "{\"<-\":[9090,222,\"KNOWS\",333,{\"since\":{\"Z\":\"1990\"}}]},"
                        + "{\"()\":[333,[],{}]}]}");
    }
}
