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
package org.neo4j.cypher.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.PathReference;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

class PathValueBuilderTest {
    private static class RelatedTo {
        private final VirtualNodeValue start;
        private final VirtualNodeValue end;

        private RelatedTo(VirtualNodeValue start, VirtualNodeValue end) {
            this.start = start;
            this.end = end;
        }
    }

    private Map<Long, RelatedTo> relations = new HashMap<>();

    @BeforeEach
    void setup() {
        relations.clear();
    }

    @Test
    void shouldComplainOnEmptyPath() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PathValueBuilder(mock(DbAccess.class), mock(RelationshipScanCursor.class)).build());
    }

    @Test
    void shouldHandleSingleNode() {
        // Given
        VirtualNodeValue node = node(42);
        PathValueBuilder builder = builder(node);

        // When
        builder.addNode(node);

        // Then
        assertEquals(path(node), builder.build());
    }

    @Test
    void shouldHandleLongerPath() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue n1 = node(42);
        VirtualNodeValue n2 = node(43);
        VirtualNodeValue n3 = node(44);
        VirtualRelationshipValue r1 = relationship(1337, n2, n1);
        VirtualRelationshipValue r2 = relationship(1338, n2, n3);
        PathValueBuilder builder = builder(n1, n2, n3, r1, r2);

        // When (n1)<--(n2)--(n3)
        builder.addNode(n1);
        builder.addIncoming(r1);
        builder.addUndirected(r2);

        // Then
        assertEquals(path(n1, r1, n2, r2, n3), builder.build());
    }

    @Test
    void shouldHandleEmptyMultiRel() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue n1 = node(42);
        PathValueBuilder builder = builder(n1);

        // When (n1)<--(n2)--(n3)
        builder.addNode(n1);
        builder.addMultipleUndirected(EMPTY_LIST);

        // Then
        assertEquals(path(n1), builder.build());
    }

    @Test
    void shouldHandleLongerPathWithMultiRel() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue n1 = node(42);
        VirtualNodeValue n2 = node(43);
        VirtualNodeValue n3 = node(44);
        VirtualRelationshipValue r1 = relationship(1337, n2, n1);
        VirtualRelationshipValue r2 = relationship(1338, n2, n3);
        PathValueBuilder builder = builder(n1, n2, n3, r1, r2);

        // When (n1)<--(n2)--(n3)
        builder.addNode(n1);
        builder.addMultipleUndirected(list(r1, r2));

        // Then
        assertEquals(path(n1, r1, n2, r2, n3), builder.build());
    }

    @Test
    void shouldHandleNoValue() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue node = node(42);
        VirtualRelationshipValue relationship = relationship(1337, node(43), node);
        PathValueBuilder builder = builder(node, relationship);

        // When (n1)<--(n2)--(n3)
        builder.addNode(node);
        builder.addIncoming(relationship);
        builder.addUndirected(NO_VALUE);

        // Then
        assertEquals(NO_VALUE, builder.build());
    }

    @Test
    void shouldHandleNoValueInMultiRel() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue node1 = node(42);
        VirtualNodeValue node2 = node(43);
        VirtualNodeValue node3 = node(44);
        VirtualRelationshipValue relationship1 = relationship(1337, node2, node1);
        VirtualRelationshipValue relationship2 = relationship(1338, node2, node3);
        PathValueBuilder builder = builder(node1, node2, node3, relationship1, relationship2);

        // When (n1)<--(n2)--(n3)
        builder.addNode(node1);
        builder.addMultipleUndirected(list(relationship1, relationship2, NO_VALUE));

        // Then
        assertEquals(NO_VALUE, builder.build());
    }

    @Test
    void shouldHandleLongerPathWithMultiRelWhereEndNodeIsKnown() {
        // Given  (n1)<--(n2)-->(n3)
        VirtualNodeValue n1 = node(42);
        VirtualNodeValue n2 = node(43);
        VirtualNodeValue n3 = node(44);
        VirtualRelationshipValue r1 = relationship(1337, n2, n1);
        VirtualRelationshipValue r2 = relationship(1338, n2, n3);
        PathValueBuilder builder = builder(n1, n2, n3, r1, r2);

        // When (n1)<--(n2)--(n3)
        builder.addNode(n1);
        builder.addMultipleUndirected(list(r1, r2), n3);

        // Then
        assertEquals(path(n1, r1, n2, r2, n3), builder.build());
    }

    private VirtualNodeValue node(long id) {
        return VirtualValues.node(id);
    }

    private VirtualRelationshipValue relationship(long id, VirtualNodeValue from, VirtualNodeValue to) {
        relations.put(id, new RelatedTo(from, to));
        return VirtualValues.relationship(id);
    }

    private PathReference path(AnyValue... nodeAndRel) {
        VirtualNodeValue[] nodes = new VirtualNodeValue[nodeAndRel.length / 2 + 1];
        VirtualRelationshipValue[] rels = new VirtualRelationshipValue[nodeAndRel.length / 2];
        for (int i = 0; i < nodeAndRel.length; i++) {
            if (i % 2 == 0) {
                nodes[i / 2] = (VirtualNodeValue) nodeAndRel[i];
            } else {
                rels[i / 2] = (VirtualRelationshipValue) nodeAndRel[i];
            }
        }
        return VirtualValues.pathReference(Arrays.asList(nodes), Arrays.asList(rels));
    }

    private PathValueBuilder builder(AnyValue... values) {
        DbAccess dbAccess = mock(DbAccess.class);
        RelationshipScanCursor cursors = mock(RelationshipScanCursor.class);
        for (AnyValue value : values) {
            if (value instanceof VirtualNodeValue nodeValue) {
                when(dbAccess.nodeById(nodeValue.id())).thenReturn(nodeValue);
            } else if (value instanceof VirtualRelationshipValue relationshipValue) {
                when(dbAccess.relationshipById(relationshipValue.id())).thenReturn(relationshipValue);
            } else {
                throw new AssertionError("invalid input");
            }

            Mockito.doAnswer((Answer<Void>) invocationOnMock -> {
                        long id = invocationOnMock.getArgument(0);
                        RelationshipScanCursor cursor = invocationOnMock.getArgument(1);
                        RelatedTo relatedTo = relations.get(id);
                        if (relatedTo != null) {
                            when(cursor.next()).thenReturn(true);
                            when(cursor.sourceNodeReference()).thenReturn(relatedTo.start.id());
                            when(cursor.targetNodeReference()).thenReturn(relatedTo.end.id());
                        }
                        return null;
                    })
                    .when(dbAccess)
                    .singleRelationship(anyLong(), any(RelationshipScanCursor.class));
        }
        return new PathValueBuilder(dbAccess, cursors);
    }
}
