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
package synchronization;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ConcurrentChangesOnEntitiesTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private GraphDatabaseService db;

    @Inject
    private DatabaseManagementService managementService;

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final AtomicReference<Exception> ex = new AtomicReference<>();

    @Test
    void addConcurrentlySameLabelToANode() throws Throwable {
        final var nodeId = initWithNode(db);

        final var t1 = newThreadForNodeAction(nodeId, node -> node.addLabel(Label.label("A")));
        final var t2 = newThreadForNodeAction(nodeId, node -> node.addLabel(Label.label("A")));

        startAndWait(t1, t2);

        managementService.shutdown();

        assertDatabaseConsistent();
    }

    @Test
    void setConcurrentlySamePropertyWithDifferentValuesOnANode() throws Throwable {
        final var nodeId = initWithNode(db);

        final var t1 = newThreadForNodeAction(nodeId, node -> node.setProperty("a", 0.788));
        final var t2 = newThreadForNodeAction(nodeId, node -> node.setProperty("a", new double[] {0.999, 0.77}));

        startAndWait(t1, t2);

        managementService.shutdown();

        assertDatabaseConsistent();
    }

    @Test
    void setConcurrentlySamePropertyWithDifferentValuesOnARelationship() throws Throwable {
        final var relId = initWithRel(db);

        final var t1 = newThreadForRelationshipAction(relId, relationship -> relationship.setProperty("a", 0.788));
        final var t2 = newThreadForRelationshipAction(
                relId, relationship -> relationship.setProperty("a", new double[] {0.999, 0.77}));

        startAndWait(t1, t2);

        managementService.shutdown();

        assertDatabaseConsistent();
    }

    private static String initWithNode(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            final var theNode = tx.createNode();
            final var id = theNode.getElementId();
            tx.commit();
            return id;
        }
    }

    private static String initWithRel(GraphDatabaseService db) {
        try (var tx = db.beginTx()) {
            final var node = tx.createNode();
            node.setProperty("a", "prop");
            final var rel = node.createRelationshipTo(tx.createNode(), RelationshipType.withName("T"));
            final var id = rel.getElementId();
            tx.commit();
            return id;
        }
    }

    private Thread newThreadForNodeAction(final String nodeId, final Consumer<Node> nodeConsumer) {
        return new Thread(() -> {
            try (var tx = db.beginTx()) {
                final var node = tx.getNodeByElementId(nodeId);
                barrier.await();
                nodeConsumer.accept(node);
                tx.commit();
            } catch (Exception e) {
                ex.set(e);
            }
        });
    }

    private Thread newThreadForRelationshipAction(
            final String relationshipId, final Consumer<Relationship> relConsumer) {
        return new Thread(() -> {
            try (var tx = db.beginTx()) {
                final var relationship = tx.getRelationshipByElementId(relationshipId);
                barrier.await();
                relConsumer.accept(relationship);
                tx.commit();
            } catch (Exception e) {
                ex.set(e);
            }
        });
    }

    private void startAndWait(Thread t1, Thread t2) throws Exception {
        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if (ex.get() != null) {
            throw ex.get();
        }
    }

    private void assertDatabaseConsistent() {
        assertDoesNotThrow(() -> {
            final var result = new ConsistencyCheckService(databaseLayout).runFullConsistencyCheck();
            Assertions.assertTrue(result.isSuccessful());
        });
    }
}
