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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
class DbmsDeadlockDetectionTest {
    private final OtherThreadExecutor t2 =
            new OtherThreadExecutor("T2-" + getClass().getName());

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService database;

    @AfterEach
    public void tearDown() {
        t2.close();
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
    }

    @Test
    void shouldLetDetectedDeadlocksDuringCommitBeThrownInTheirOriginalForm() throws Exception {
        // GIVEN a database with a couple of entities:
        // (n1) --> (r1) --> (r2) --> (r3)
        // (n2)
        Node n1 = createNode(database);
        Node n2 = createNode(database);
        Relationship r3 = createRelationship(database, n1);
        Relationship r2 = createRelationship(database, n1);
        Relationship r1 = createRelationship(database, n1);

        // Nodes to lock for deadlock strategy to close expected lock client.
        // Since we use ABORT_YOUNG strategy by default we need expected client to hold less locks.
        var emptyNodes = List.of(
                createNode(database),
                createNode(database),
                createNode(database),
                createNode(database),
                createNode(database),
                createNode(database),
                createNode(database),
                createNode(database));

        // WHEN creating a deadlock scenario where the final deadlock would have happened due to locks
        //      acquired during linkage of relationship records
        //
        // (r1) <-- (t1)
        //   |       ^
        //   v       |
        // (t2) --> (n2)
        Transaction t1Tx = database.beginTx();
        Transaction t2Tx = t2.executeDontWait(beginTx(database)).get();
        // (t1) <-- (n2)
        t1Tx.getNodeById(n2.getId()).setProperty("locked", "indeed");
        // (t2) <-- (r1)
        t2.executeDontWait(setProperty(t2Tx.getRelationshipById(r1.getId()), "locked", "absolutely"))
                .get();
        // dummy locks
        for (Node emptyNode : emptyNodes) {
            t2.executeDontWait(setProperty(t2Tx.getNodeById(emptyNode.getId()), "locked", "absolutely"))
                    .get();
        }

        // (t2) --> (n2)
        Future<Void> t2n2Wait = t2.executeDontWait(setProperty(t2Tx.getNodeById(n2.getId()), "locked", "In my dreams"));
        t2.waitUntilWaiting();
        // (t1) --> (r1) although delayed until commit, this is accomplished by deleting an adjacent
        //               relationship so that its surrounding relationships are locked at commit time.
        t1Tx.getRelationshipById(r2.getId()).delete();
        assertThrows(DeadlockDetectedException.class, t1Tx::commit);

        t2n2Wait.get();
        t2.executeDontWait(close(t2Tx)).get();
    }

    private static Callable<Transaction> beginTx(final GraphDatabaseService db) {
        return db::beginTx;
    }

    private static Callable<Void> setProperty(final Entity entity, final String key, final String value) {
        return () -> {
            entity.setProperty(key, value);
            return null;
        };
    }

    private static Callable<Void> close(final Transaction tx) {
        return () -> {
            tx.close();
            return null;
        };
    }

    private static Relationship createRelationship(GraphDatabaseService db, Node node) {
        try (Transaction tx = db.beginTx()) {
            Relationship rel = tx.getNodeById(node.getId()).createRelationshipTo(node, MyRelTypes.TEST);
            tx.commit();
            return rel;
        }
    }

    private static Node createNode(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            tx.commit();
            return node;
        }
    }
}
