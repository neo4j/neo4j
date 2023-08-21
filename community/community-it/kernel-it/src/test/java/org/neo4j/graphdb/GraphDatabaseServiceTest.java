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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.shutdown_transaction_end_timeout;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
public class GraphDatabaseServiceTest {
    private final OtherThreadExecutor t2 =
            new OtherThreadExecutor("T2-" + getClass().getName());
    private final OtherThreadExecutor t3 =
            new OtherThreadExecutor("T3-" + getClass().getName());

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService database;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(shutdown_transaction_end_timeout, Duration.ofSeconds(10));
    }

    @AfterEach
    public void tearDown() {
        t2.close();
        t3.close();
    }

    @Test
    void givenShutdownDatabaseWhenBeginTxThenExceptionIsThrown() {
        managementService.shutdown();

        assertThrows(DatabaseShutdownException.class, () -> database.beginTx());
    }

    @Test
    void givenDatabaseAndStartedTxWhenShutdownThenWaitForTxToFinish() throws Exception {
        Barrier.Control barrier = new Barrier.Control();
        Future<Object> txFuture = t2.executeDontWait(() -> {
            try (Transaction tx = database.beginTx()) {
                barrier.reached();
                tx.createNode();
                tx.commit();
            }
            return null;
        });

        barrier.await();

        Future<Object> shutdownFuture = t3.executeDontWait(() -> {
            managementService.shutdown();
            return null;
        });
        t3.waitUntilWaiting(location -> location.isAt(DatabaseAvailability.class, "stop"));
        barrier.release();
        assertDoesNotThrow((ThrowingSupplier<Object>) txFuture::get);
        shutdownFuture.get();
    }

    @Test
    void terminateTransactionThrowsExceptionOnNextOperation() {
        try (Transaction tx = database.beginTx()) {
            tx.terminate();
            assertThrows(TransactionTerminatedException.class, tx::createNode);
        }
    }

    @Test
    void givenDatabaseAndStartedTxWhenShutdownAndStartNewTxThenBeginTxTimesOut() throws Exception {
        Barrier.Control barrier = new Barrier.Control();
        t2.executeDontWait(() -> {
            try (Transaction tx = database.beginTx()) {
                barrier.reached(); // <-- this triggers t3 to start a managementService.shutdown()
            }
            return null;
        });

        barrier.await();
        Future<Object> shutdownFuture = t3.executeDontWait(() -> {
            managementService.shutdown();
            return null;
        });
        t3.waitUntilWaiting(location -> location.isAt(DatabaseAvailability.class, "stop"));
        barrier.release(); // <-- this triggers t2 to continue its transaction
        shutdownFuture.get();

        assertThrows(DatabaseShutdownException.class, database::beginTx);
    }

    /**
     * GitHub issue #5996
     */
    @Test
    void terminationOfClosedTransactionDoesNotInfluenceNextTransaction() {
        try (Transaction tx = database.beginTx()) {
            tx.createNode();
            tx.commit();
        }

        Transaction transaction = database.beginTx();
        try (Transaction tx = transaction) {
            tx.createNode();
            tx.commit();
        }
        transaction.terminate();

        try (Transaction tx = database.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            assertThat(allNodes).hasSize(2);
            tx.commit();
        }
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
