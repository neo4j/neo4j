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
package org.neo4j.kernel.impl.newapi.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.filter;
import static org.neo4j.internal.helpers.collection.Iterables.single;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureKernelTransaction;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ExecutionContextProcedureTransactionIT {

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void testGettingAllNodes() {
        List<String> nodeIds = new ArrayList<>();
        try (var tx = db.beginTx()) {
            nodeIds.add(tx.createNode().getElementId());
            nodeIds.add(tx.createNode().getElementId());
            nodeIds.add(tx.createNode().getElementId());
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            var retrievedIds = executionContextTransaction(tx).getAllNodes().stream()
                    .map(Entity::getElementId)
                    .collect(Collectors.toList());
            assertThat(retrievedIds).containsExactlyInAnyOrderElementsOf(nodeIds);
        }
    }

    @Test
    void testGettingAllRelationships() {
        List<String> relIds = new ArrayList<>();

        try (var tx = db.beginTx()) {
            var node = tx.createNode();
            relIds.add(node.createRelationshipTo(node, withName("T1")).getElementId());
            relIds.add(node.createRelationshipTo(node, withName("T2")).getElementId());
            relIds.add(node.createRelationshipTo(node, withName("T3")).getElementId());
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            var retrievedIds = executionContextTransaction(tx).getAllRelationships().stream()
                    .map(Entity::getElementId)
                    .collect(Collectors.toList());
            assertThat(retrievedIds).containsExactlyInAnyOrderElementsOf(relIds);
        }
    }

    @Test
    void shouldListLabels() {
        Label label1 = Label.label("label1");
        Label label2 = Label.label("label2");
        String nodeId;
        try (var tx = db.beginTx()) {
            nodeId = tx.createNode(label1, label2).getElementId();
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllLabels()).containsExactlyInAnyOrder(label1, label2);
            assertThat(executionContextTransaction(tx).getAllLabelsInUse()).containsExactlyInAnyOrder(label1, label2);
        }

        try (var tx = db.beginTx()) {
            tx.getNodeByElementId(nodeId).removeLabel(label1);
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllLabels()).containsExactlyInAnyOrder(label1, label2);
            assertThat(executionContextTransaction(tx).getAllLabelsInUse()).containsExactlyInAnyOrder(label2);
        }
    }

    @Test
    void shouldListRelationshipTypes() {
        RelationshipType type1 = RelationshipType.withName("TYPE1");
        RelationshipType type2 = RelationshipType.withName("TYPE2");
        String relId;
        try (var tx = db.beginTx()) {
            var node = tx.createNode();
            relId = node.createRelationshipTo(node, type1).getElementId();
            node.createRelationshipTo(node, type2);
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllRelationshipTypes())
                    .containsExactlyInAnyOrder(type1, type2);
            assertThat(executionContextTransaction(tx).getAllRelationshipTypesInUse())
                    .containsExactlyInAnyOrder(type1, type2);
        }

        try (var tx = db.beginTx()) {
            tx.getRelationshipByElementId(relId).delete();
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllRelationshipTypes())
                    .containsExactlyInAnyOrder(type1, type2);
            assertThat(executionContextTransaction(tx).getAllRelationshipTypesInUse())
                    .containsExactlyInAnyOrder(type2);
        }
    }

    @Test
    void shouldListPropertyKeys() {
        String key1 = "key1";
        String key2 = "key2";
        String nodeId;
        try (var tx = db.beginTx()) {
            var node = tx.createNode();
            nodeId = node.getElementId();
            node.setProperty(key1, "foo");
            node.setProperty(key2, "bar");
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllPropertyKeys()).containsExactlyInAnyOrder(key1, key2);
        }

        try (var tx = db.beginTx()) {
            tx.getNodeByElementId(nodeId).removeProperty(key1);
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            assertThat(executionContextTransaction(tx).getAllPropertyKeys()).containsExactlyInAnyOrder(key1, key2);
        }
    }

    @Test
    void shouldReadIndexesAndConstraints() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label("L"))
                    .on("prop")
                    .withName("MyIndex")
                    .create();
            tx.schema()
                    .constraintFor(Label.label("M"))
                    .withName("MyConstraint")
                    .assertPropertyIsUnique("prop")
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            Schema schema = executionContextTransaction(tx).schema();
            assertThat(count(filter((i) -> i.getName().equals("MyIndex"), schema.getIndexes())))
                    .isEqualTo(1L);
            assertThat(single(schema.getIndexByName("MyIndex").getPropertyKeys()))
                    .isEqualTo("prop");
            assertThat(single(schema.getIndexes(Label.label("L"))).getName()).isEqualTo("MyIndex");
            assertThat(count(filter((i) -> i.getName().equals("MyConstraint"), schema.getConstraints())))
                    .isEqualTo(1L);
            assertThat(single(schema.getConstraintByName("MyConstraint").getPropertyKeys()))
                    .isEqualTo("prop");
            assertThat(single(schema.getConstraints(Label.label("M"))).getName())
                    .isEqualTo("MyConstraint");
        }
    }

    @Test
    void shouldAwaitIndexesToComeOnline() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label("L"))
                    .on("prop")
                    .withName("MyIndex")
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            Schema schema = executionContextTransaction(tx).schema();
            schema.awaitIndexesOnline(1, TimeUnit.MINUTES);
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            var schema = tx.schema();
            assertThat(single(schema.getIndexByName("MyIndex").getPropertyKeys()))
                    .isEqualTo("prop");
        }
    }

    @Test
    void shouldNotBeAbleToCreateNewIndexes() {
        try (var tx = db.beginTx()) {
            Schema schema = executionContextTransaction(tx).schema();
            assertThatThrownBy(
                            () -> schema.indexFor(Label.label("L")).on("prop").create())
                    .isInstanceOf(UnsupportedOperationException.class);
            tx.commit();
        }
    }

    private static Transaction executionContextTransaction(Transaction tx) {
        var internalTx = ((InternalTransaction) tx);
        var ktx = internalTx.kernelTransaction();
        internalTx.registerCloseableResource(ktx.acquireStatement());
        var executionContext = ktx.createExecutionContext();
        internalTx.registerCloseableResource(() -> {
            executionContext.complete();
            executionContext.close();
        });
        return new ExecutionContextProcedureTransaction(
                new ExecutionContextProcedureKernelTransaction(ktx, executionContext), null);
    }
}
