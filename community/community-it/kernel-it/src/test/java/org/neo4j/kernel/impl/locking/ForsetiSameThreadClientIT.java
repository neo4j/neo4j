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
package org.neo4j.kernel.impl.locking;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension()
class ForsetiSameThreadClientIT {
    @Inject
    GraphDatabaseAPI database;

    @Inject
    DatabaseManagementService managementService;

    @Test
    void shouldDetectDeadlockForCommitListeners() {
        // Given
        long nodeId;
        try (Transaction tx = database.beginTx()) {
            nodeId = tx.createNode().getId();
            tx.commit();
        }

        managementService.registerTransactionEventListener(
                database.databaseName(), new TransactionEventListenerAdapter<Void>() {
                    @Override
                    public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {
                        try (Transaction tx = database.beginTx()) {
                            tx.acquireReadLock(tx.getNodeById(nodeId));
                        }
                    }
                });

        // When
        try (Transaction tx = database.beginTx()) {
            tx.createNode(); // Make it a write tx to trigger afterCommit events
            tx.acquireWriteLock(tx.getNodeById(nodeId));

            // Then
            assertThatThrownBy(tx::commit)
                    .isInstanceOf(DeadlockDetectedException.class)
                    .hasMessageContaining("committing on the same thread");
        }
    }
}
