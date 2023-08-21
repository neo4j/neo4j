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
package org.neo4j.graphdb.schema;

import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
abstract class UpdateDeletedNodeIndexBase {
    @Inject
    protected GraphDatabaseAPI db;

    protected static final String KEY = "key";
    private static final int NODES = 100;

    @RepeatedTest(5)
    void shouldHandleCreateNodeConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, nodeId) -> tx.createNode(LABEL_ONE).setProperty(KEY, nodeId));
    }

    @RepeatedTest(5)
    void shouldHandleRemovalOfLabelConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, nodeId) -> tx.getNodeById(nodeId).removeLabel(LABEL_ONE));
    }

    @RepeatedTest(5)
    void shouldHandleDeleteNodeConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, nodeId) -> tx.getNodeById(nodeId).delete());
    }

    @RepeatedTest(5)
    void shouldHandleRemovePropertyConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, nodeId) -> tx.getNodeById(nodeId).removeProperty(KEY));
    }

    @RepeatedTest(5)
    void shouldHandleNodeDetachDeleteConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation((tx, nodeId) -> {
            ((InternalTransaction) tx).kernelTransaction().dataWrite().nodeDetachDelete(nodeId);
        });
    }

    private void shouldHandleIndexDropConcurrentlyWithOperation(NodeOperation operation) throws Throwable {
        // given
        long[] nodes = createNodes();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant(
                () -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema().getIndexByName(indexDefinition.getName()).drop();
                        tx.commit();
                    }
                },
                1);
        for (int i = 0; i < NODES; i++) {
            final long nodeId = nodes[i];
            race.addContestant(throwing(() -> {
                try (Transaction tx = db.beginTx()) {
                    operation.run(tx, nodeId);
                    tx.commit();
                }
            }));
        }

        // then
        race.go();
    }

    private long[] createNodes() {
        long[] nodes = new long[NODES];
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NODES; i++) {
                Node node = tx.createNode(LABEL_ONE);
                node.setProperty(KEY, i);
                nodes[i] = node.getId();
            }
            tx.commit();
        }
        return nodes;
    }

    private IndexDefinition createIndex() {
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NODES; i++) {
                tx.createNode(LABEL_ONE).setProperty(KEY, i);
            }
            tx.commit();
        }
        IndexDefinition indexDefinition;
        indexDefinition = indexCreate();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
        return indexDefinition;
    }

    protected abstract IndexDefinition indexCreate();

    private interface NodeOperation {
        void run(Transaction tx, long nodeId) throws Exception;
    }
}
