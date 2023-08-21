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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.count;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
class RelationshipGroupStoreIT {
    private static final int RELATIONSHIP_COUNT = 20;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private RecordStorageEngine storageEngine;

    @ExtensionCallback
    static void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
        builder.setConfig(GraphDatabaseSettings.dense_node_threshold, 1);
    }

    @Test
    void shouldCreateAllTheseRelationshipTypes() {
        shiftHighId();

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            for (int i = 0; i < RELATIONSHIP_COUNT; i++) {
                node.createRelationshipTo(tx.createNode(), type(i));
            }
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            node = tx.getNodeById(node.getId());
            for (int i = 0; i < RELATIONSHIP_COUNT; i++) {
                assertEquals(
                        1,
                        count(node.getRelationships(type(i))),
                        "Should be possible to get relationships of type with id in unsigned short range.");
            }
        }
    }

    @Test
    void shouldDeleteDenseNodeIfContainingEmptyGroupsFromPreviousContendedRelationshipDeletions()
            throws ExecutionException, InterruptedException {
        // given
        long nodeId;
        RelationshipType typeA = RelationshipType.withName("A");
        RelationshipType typeB = RelationshipType.withName("B");
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            nodeId = node.getId();
            for (int i = 0; i < 200; i++) {
                node.createRelationshipTo(tx.createNode(), i % 2 == 0 ? typeA : typeB);
            }
            tx.commit();
        }

        // when starting a transaction that creates a relationship of type B and halting it before apply
        Barrier.Control barrier;
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2")) {
            barrier = new Barrier.Control();
            Future<Object> t2Future = t2.executeDontWait(() -> {
                try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
                    tx.getNodeById(nodeId).createRelationshipTo(tx.createNode(), typeB);
                    tx.commit(KernelTransaction.KernelTransactionMonitor.withBeforeApply(barrier::reached));
                }
                return null;
            });

            barrier.awaitUninterruptibly();

            // and another transaction which deletes all relationships of type A, and let it commit
            try (Transaction tx = db.beginTx()) {
                Iterables.forEach(tx.getNodeById(nodeId).getRelationships(typeA), Relationship::delete);
                tx.commit();
            }
            // and letting the first transaction complete
            barrier.release();
            t2Future.get();
        }

        // then deleting the node should remove the empty group A, even if it's only deleting relationships of type B
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeById(nodeId);
            Iterables.forEach(node.getRelationships(), rel -> {
                assertThat(rel.isType(typeB)).isTrue();
                rel.delete();
            });
            node.delete();
            tx.commit();
        }
    }

    @Test
    void shouldDeleteDenseNodeIfContainingEmptyGroups() throws Exception {
        // given
        long nodeId;
        RelationshipType typeA = RelationshipType.withName("A");
        RelationshipType typeB = RelationshipType.withName("B");
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            nodeId = node.getId();
            node.createRelationshipTo(tx.createNode(), typeA);
            for (int i = 0; i < 200; i++) {
                node.createRelationshipTo(
                        tx.createNode(), typeA); // Type A is created first and will get a lower type ID
            }
            tx.getNodeById(nodeId).createRelationshipTo(tx.createNode(), typeB);
            tx.commit();
        }

        // when starting a transaction that creates a relationship of type A and halting it before apply
        Barrier.Control barrier;
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2")) {
            barrier = new Barrier.Control();
            Future<Object> t2Future = t2.executeDontWait(() -> {
                try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
                    tx.getNodeById(nodeId).createRelationshipTo(tx.createNode(), typeA);
                    tx.commit(KernelTransaction.KernelTransactionMonitor.withBeforeApply(barrier::reached));
                }
                return null;
            });

            barrier.awaitUninterruptibly();

            // and another transaction which deletes all relationships of type B, and let it commit
            try (Transaction tx = db.beginTx()) {
                Iterables.forEach(tx.getNodeById(nodeId).getRelationships(typeB), Relationship::delete);
                tx.commit(); // This will fail to delete the group B since it can not get exlusive locks
            }
            // and letting the first transaction complete
            barrier.release();
            t2Future.get();

            // then removing the remaining relationships of A
            try (Transaction tx = db.beginTx()) {
                Iterables.forEach(tx.getNodeById(nodeId).getRelationships(typeA), Relationship::delete);
                tx.commit(); // this should only delete group A since B has a higher typeId than A (later in
                // group-chain)
            }
        }

        // then deleting the node should also remove the empty group B
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(nodeId).delete();
            assertThatCode(tx::commit).doesNotThrowAnyException();
        }
    }

    private void shiftHighId() {
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        neoStores.getRelationshipTypeTokenStore().setHighId(Short.MAX_VALUE - RELATIONSHIP_COUNT / 2);
    }

    private static RelationshipType type(int i) {
        return RelationshipType.withName("TYPE_" + i);
    }
}
