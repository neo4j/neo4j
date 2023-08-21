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
package org.neo4j.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class TestTransactionEventDeadlocks {
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService graphdb;

    @Test
    void canAvoidDeadlockThatWouldHappenIfTheRelationshipTypeCreationTransactionModifiedData() {
        Node node;
        try (Transaction tx = graphdb.beginTx()) {
            node = tx.createNode();
            node.setProperty("counter", 0L);
            tx.commit();
        }

        try (Transaction tx = graphdb.beginTx()) {
            var txNode = tx.getNodeById(node.getId());
            managementService.registerTransactionEventListener(
                    DEFAULT_DATABASE_NAME, new RelationshipCounterTransactionEventListener(txNode));
            txNode.setProperty("state", "not broken yet");
            txNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("TEST"));
            txNode.removeProperty("state");
            tx.commit();
        }

        try (Transaction transaction = graphdb.beginTx()) {
            var n = transaction.getNodeById(node.getId());
            assertEquals(1L, n.getProperty("counter"));
        }
    }

    private static class RelationshipCounterTransactionEventListener extends TransactionEventListenerAdapter<Void> {
        private final Node node;

        RelationshipCounterTransactionEventListener(Node node) {
            this.node = node;
        }

        @SuppressWarnings("boxing")
        @Override
        public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            if (Iterables.count(data.createdRelationships()) == 0) {
                return null;
            }
            node.setProperty("counter", ((Long) node.removeProperty("counter")) + 1);
            return null;
        }
    }
}
