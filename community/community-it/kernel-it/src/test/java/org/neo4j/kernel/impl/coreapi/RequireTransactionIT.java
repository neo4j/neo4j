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
package org.neo4j.kernel.impl.coreapi;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class RequireTransactionIT {
    @Inject
    private GraphDatabaseAPI databaseAPI;

    private Transaction transaction;

    @BeforeEach
    void setUp() {
        transaction = databaseAPI.beginTx();
    }

    @AfterEach
    void tearDown() {
        if (transaction != null) {
            transaction.close();
        }
    }

    @Test
    void requireTransactionForNodeCreation() {
        Executable executable = transaction::createNode;
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeCreationWithLabels() {
        Executable executable = () -> transaction.createNode(Label.label("label"));
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupById() {
        Node node;
        try (Transaction tx = databaseAPI.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }
        Executable executable = () -> transaction.getNodeById(node.getId());
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipLookupById() {
        Relationship relationship;
        try (Transaction tx = databaseAPI.beginTx()) {
            relationship = tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("type"));
            tx.commit();
        }
        Executable executable = () -> transaction.getRelationshipById(relationship.getId());
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForBidirectionalTraversal() {
        Executable executable = () -> transaction.bidirectionalTraversalDescription();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForTraversal() {
        Executable executable = () -> transaction.traversalDescription();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForQueryExecution() {
        Executable executable = () -> transaction.execute("MATCH (n) RETURN count(n)");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForQueryExecutionWithParameters() {
        Executable executable = () -> transaction.execute("MATCH (n) RETURN count(n)", emptyMap());
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllUsedLabelsLookup() {
        Executable executable = () -> transaction.getAllLabelsInUse();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllUsedRelationshipTypesLookup() {
        Executable executable = () -> transaction.getAllRelationshipTypesInUse();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllLabelsLookup() {
        Executable executable = () -> transaction.getAllLabels();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllRelationshipTypesLookup() {
        Executable executable = () -> transaction.getAllRelationshipTypes();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllPropertyKeysLookup() {
        Executable executable = () -> transaction.getAllPropertyKeys();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabelPropertyTemplate() {
        Executable executable = () -> transaction.findNodes(Label.label("label"), "a", "aa", StringSearchMode.CONTAINS);
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabelPropertyValues() {
        Executable executable = () -> transaction.findNodes(Label.label("label"), Map.of("a", "b", "c", "d"));
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabelPropertyValuesPairs3() {
        Executable executable = () -> transaction.findNodes(Label.label("label"), "a", "b", "c", "d", "e", "f");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabelPropertyValuesPairs2() {
        Executable executable = () -> transaction.findNodes(Label.label("label"), "a", "b", "c", "d");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabelPropertyValuesPair() {
        Executable executable = () -> transaction.findNode(Label.label("label"), "a", "b");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodesLookupByLabelPropertyValuesPair() {
        Executable executable = () -> transaction.findNodes(Label.label("label"), "a", "b");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForNodeLookupByLabel() {
        Executable executable = () -> transaction.findNodes(Label.label("label"));
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipsLookupByTypePropertyTemplate() {
        Executable executable = () ->
                transaction.findRelationships(RelationshipType.withName("type"), "a", "aa", StringSearchMode.CONTAINS);
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipsLookupByTypePropertyValues() {
        Executable executable =
                () -> transaction.findRelationships(RelationshipType.withName("type"), Map.of("a", "b", "c", "d"));
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipsLookupByTypePropertyValuesPairs3() {
        Executable executable =
                () -> transaction.findRelationships(RelationshipType.withName("type"), "a", "b", "c", "d", "e", "f");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipsLookupByTypePropertyValuesPairs2() {
        Executable executable =
                () -> transaction.findRelationships(RelationshipType.withName("type"), "a", "b", "c", "d");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipLookupByTypePropertyValuesPair() {
        Executable executable = () -> transaction.findRelationship(RelationshipType.withName("type"), "a", "b");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipsLookupByTypePropertyValuesPair() {
        Executable executable = () -> transaction.findRelationships(RelationshipType.withName("type"), "a", "b");
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForRelationshipLookupByType() {
        Executable executable = () -> transaction.findRelationships(RelationshipType.withName("type"));
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void terminateCanBeCalledOnAnyTransaction() {
        transaction.terminate();

        transaction.close();

        assertDoesNotThrow(() -> transaction.terminate());
    }

    @Test
    void closeCanBeCalledOnAnyTransaction() {
        assertDoesNotThrow(() -> transaction.close());
        assertDoesNotThrow(() -> transaction.close());
        assertDoesNotThrow(() -> transaction.close());
        assertDoesNotThrow(() -> transaction.close());
    }

    @Test
    void requireTransactionForRollback() {
        assertDoesNotThrow(() -> transaction.rollback());
        assertDoesNotThrow(() -> transaction.rollback());
        assertDoesNotThrow(() -> transaction.rollback());
        assertDoesNotThrow(() -> transaction.rollback());
    }

    @Test
    void requireTransactionForCommit() {
        Executable executable = () -> transaction.commit();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllNodesLookup() {
        Executable executable = () -> transaction.getAllNodes().close();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForAllRelationshipsLookup() {
        Executable executable = () -> transaction.getAllRelationships().close();
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForWriteLockAcquisition() {
        Node node;
        try (Transaction tx = databaseAPI.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }
        Executable executable = () -> transaction.acquireWriteLock(node);
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForReadLockAcquisition() {
        Node node;
        try (Transaction tx = databaseAPI.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }
        Executable executable = () -> transaction.acquireReadLock(node);
        checkTransactionRequirement(transaction, executable);
    }

    @Test
    void requireTransactionForSchemaAccess() {
        Executable executable = () -> transaction.schema();
        checkTransactionRequirement(transaction, executable);
    }

    private static void checkTransactionRequirement(Transaction transaction, Executable executable) {
        try (transaction) {
            checkDoesNotThrow(executable);
        }
        checkThrowNotInTransaction(executable);
    }

    static void checkDoesNotThrow(Executable executable) {
        assertDoesNotThrow(executable);
    }

    static void checkThrowNotInTransaction(Executable executable) {
        var e = assertThrows(Exception.class, executable);
        assertThat(getRootCause(e)).isInstanceOf(NotInTransactionException.class);
    }
}
