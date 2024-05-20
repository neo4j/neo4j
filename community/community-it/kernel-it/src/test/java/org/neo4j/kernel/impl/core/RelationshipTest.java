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
package org.neo4j.kernel.impl.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.RelationshipType.withName;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.token.api.NamedToken;

@ExtendWith(RandomExtension.class)
public class RelationshipTest extends EntityTest {

    @Inject
    RandomSupport random;

    @Override
    protected long createEntity(Transaction tx) {
        return tx.createNode()
                .createRelationshipTo(tx.createNode(), withName("FOO"))
                .getId();
    }

    @Override
    protected Entity lookupEntity(Transaction transaction, long id) {
        return transaction.getRelationshipById(id);
    }

    @Test
    void shouldBeAbleToReferToIdsBeyondMaxInt() {
        // GIVEN
        var transaction = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        when(transaction.newNodeEntity(anyLong())).then(invocation -> nodeWithId(invocation.getArgument(0)));
        when(transaction.getRelationshipTypeById(anyInt()))
                .then(invocation -> new NamedToken("whatever", invocation.getArgument(0)));

        long[] ids = new long[] {
            1437589437, 2047587483, 2147496246L, 2147342921, 3276473721L, 4762746373L, 57587348738L, 59892898932L
        };
        int[] types = new int[] {0, 10, 101, 3024, 20123, 45008};

        // WHEN/THEN
        for (int i = 0; i < ids.length - 2; i++) {
            long id = ids[i];
            long nodeId1 = ids[i + 1];
            long nodeId2 = ids[i + 2];
            int type = types[i];
            verifyIds(transaction, id, nodeId1, type, nodeId2);
            verifyIds(transaction, id, nodeId2, type, nodeId1);
        }
    }

    @Test
    void shouldPrintCypherEsqueRelationshipToString() {
        // GIVEN
        Node start;
        Node end;
        RelationshipType type = RelationshipType.withName("NICE");
        Relationship relationship;
        try (Transaction tx = db.beginTx()) {
            // GIVEN
            start = tx.createNode();
            end = tx.createNode();
            relationship = start.createRelationshipTo(end, type);
            tx.commit();

            // WHEN
            String toString = relationship.toString();

            // THEN
            assertEquals(
                    "(" + start.getId() + ")-[" + type + "," + relationship.getId() + "]->(" + end.getId() + ")",
                    toString);
        }
    }

    @Test
    void relationshipNotFoundByElementId() {
        String elementId;
        try (Transaction transaction = db.beginTx()) {
            Node node = transaction.createNode();
            Relationship relationship = node.createRelationshipTo(transaction.createNode(), withName("test"));
            elementId = relationship.getElementId();
            transaction.rollback();
        }
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.getRelationshipByElementId(elementId);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining(elementId + " not found")
                .hasRootCauseInstanceOf(EntityNotFoundException.class)
                .rootCause()
                .hasMessageContaining("Unable to load RELATIONSHIP " + elementId);
    }

    @Test
    void createDropRelationshipLongStringProperty(TestInfo testInfo) {
        Label markerLabel = Label.label("marker_" + testInfo.getTestMethod());
        String testPropertyKey = "testProperty";
        String propertyValue = RandomStringUtils.randomAscii(255);

        String relationshipId;
        try (Transaction tx = db.beginTx()) {
            Node start = tx.createNode(markerLabel);
            Node end = tx.createNode(markerLabel);
            Relationship relationship = start.createRelationshipTo(end, withName("type"));
            relationshipId = relationship.getElementId();
            relationship.setProperty(testPropertyKey, propertyValue);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            assertEquals(propertyValue, relationship.getProperty(testPropertyKey));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            relationship.removeProperty(testPropertyKey);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            assertFalse(relationship.hasProperty(testPropertyKey));
            tx.commit();
        }
    }

    @Test
    void createDropRelationshipLongArrayProperty(TestInfo testInfo) {
        Label markerLabel = Label.label("marker_" + testInfo.getTestMethod());
        String testPropertyKey = "testProperty";
        byte[] propertyValue = random.nextBytes(1024);

        String relationshipId;
        try (Transaction tx = db.beginTx()) {
            Node start = tx.createNode(markerLabel);
            Node end = tx.createNode(markerLabel);
            Relationship relationship = start.createRelationshipTo(end, withName("type"));
            relationshipId = relationship.getElementId();
            relationship.setProperty(testPropertyKey, propertyValue);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            assertArrayEquals(propertyValue, (byte[]) relationship.getProperty(testPropertyKey));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            relationship.removeProperty(testPropertyKey);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship relationship = tx.getRelationshipByElementId(relationshipId);
            assertFalse(relationship.hasProperty(testPropertyKey));
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToForceTypeChangeOfProperty() {
        // Given
        Relationship relationship;
        try (Transaction tx = db.beginTx()) {
            relationship = tx.createNode().createRelationshipTo(tx.createNode(), withName("R"));
            relationship.setProperty("prop", 1337);
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            tx.getRelationshipById(relationship.getId()).setProperty("prop", 1337.0);
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.getRelationshipById(relationship.getId()).getProperty("prop"))
                    .isInstanceOf(Double.class);
        }
    }

    @Test
    void shouldThrowCorrectExceptionOnPropertyKeyTokensExceeded() throws KernelException {
        // given
        var transaction = mockedTransactionWithDepletedTokens();
        RelationshipEntity relationshipEntity = new RelationshipEntity(transaction, 5);

        // when
        assertThrows(ConstraintViolationException.class, () -> relationshipEntity.setProperty("key", "value"));
    }

    @Test
    void shouldThrowCorrectExceptionOnTokensTransientFailureSetProperty() throws KernelException {
        RelationshipEntity relationshipEntity =
                new RelationshipEntity(transactionWithTransientlyFailingTokenWrite(), 5);
        assertThrows(TransientTransactionFailureException.class, () -> relationshipEntity.setProperty("key", "value"));
    }

    private static void verifyIds(
            InternalTransaction transaction, long relationshipId, long nodeId1, int typeId, long nodeId2) {
        RelationshipEntity proxy = new RelationshipEntity(transaction, relationshipId, nodeId1, typeId, nodeId2);
        assertEquals(relationshipId, proxy.getId());
        // our mock above is known to return RelationshipTypeToken
        assertEquals(nodeId1, proxy.getStartNode().getId());
        assertEquals(nodeId1, proxy.getStartNodeId());
        assertEquals(nodeId2, proxy.getEndNode().getId());
        assertEquals(nodeId2, proxy.getEndNodeId());
        assertEquals(nodeId2, proxy.getOtherNode(nodeWithId(nodeId1)).getId());
        assertEquals(nodeId2, proxy.getOtherNodeId(nodeId1));
        assertEquals(nodeId1, proxy.getOtherNode(nodeWithId(nodeId2)).getId());
        assertEquals(nodeId1, proxy.getOtherNodeId(nodeId2));
    }

    private static Node nodeWithId(long id) {
        NodeEntity node = mock(NodeEntity.class);
        when(node.getId()).thenReturn(id);
        return node;
    }
}
