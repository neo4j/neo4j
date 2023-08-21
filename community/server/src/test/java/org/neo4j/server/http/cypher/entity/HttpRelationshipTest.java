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
package org.neo4j.server.http.cypher.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.graphdb.Node;

class HttpRelationshipTest {
    private static final long START_NODE_ID = 1245L;
    private static final String START_NODE_ELEMENT_ID = String.valueOf(START_NODE_ID);
    private static final long END_NODE_ID = 54321L;
    private static final String END_NODE_ELEMENT_ID = String.valueOf(END_NODE_ID);

    @Test
    void getStartNodeId_shouldReturnStartNodeIdWithoutCallGetStartNode() {
        var subject = setupSubject();

        var startNodeId = subject.getStartNodeId();

        assertEquals(START_NODE_ID, startNodeId);
        verify(subject, Mockito.never()).getStartNode();
    }

    @Test
    void getEndNodeId_shouldReturnEndNodeIdWithoutCallGetEndNode() {
        var subject = setupSubject();

        var endNodeId = subject.getEndNodeId();

        assertEquals(END_NODE_ID, endNodeId);
        verify(subject, Mockito.never()).getEndNode();
    }

    @Test
    void getStartNode_whenSupplierReturnsEmpty_shouldReturnNodeCreatedOnlyWithNodeId() {
        var subject = setupSubject();
        var expectedNode = new HttpNode(START_NODE_ELEMENT_ID, START_NODE_ID);

        var startNode = subject.getStartNode();

        assertEquals(expectedNode, startNode);
    }

    @Test
    void getStartNode_whenSupplierReturnsTheNode_shouldReturnIt() {
        var expectedNode = new HttpNode(START_NODE_ELEMENT_ID, START_NODE_ID, List.of(), Map.of(), false);
        var subject = setupSubject((ignoredA, ignoredB) -> Optional.of(expectedNode));

        var startNode = subject.getStartNode();

        assertSame(expectedNode, startNode);
    }

    @Test
    void getEndNode_whenSupplierReturnsEmpty_shouldReturnNodeCreatedOnlyWithNodeId() {
        var subject = setupSubject();
        var expectedNode = new HttpNode(END_NODE_ELEMENT_ID, END_NODE_ID);

        var endNode = subject.getEndNode();

        assertEquals(expectedNode, endNode);
    }

    @Test
    void getEndNode_whenSupplierReturnsTheNode_shouldReturnIt() {
        var expectedNode = new HttpNode(END_NODE_ELEMENT_ID, END_NODE_ID, List.of(), Map.of(), false);
        var subject = setupSubject((ignoredA, ignoredB) -> Optional.of(expectedNode));

        var endNode = subject.getEndNode();

        assertSame(expectedNode, endNode);
    }

    private HttpRelationship setupSubject() {
        return setupSubject((ignoredA, ignoredB) -> Optional.empty());
    }

    private HttpRelationship setupSubject(BiFunction<Long, Boolean, Optional<Node>> getNodeById) {
        var httpRelationship = new HttpRelationship(
                "1",
                1,
                START_NODE_ELEMENT_ID,
                START_NODE_ID,
                END_NODE_ELEMENT_ID,
                END_NODE_ID,
                "KNOWS",
                Map.of(),
                false,
                getNodeById);
        return spy(httpRelationship);
    }
}
