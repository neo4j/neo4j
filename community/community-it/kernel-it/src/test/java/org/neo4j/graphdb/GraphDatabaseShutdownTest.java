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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.locking.LockCountVisitor;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class GraphDatabaseShutdownTest {
    private final OtherThreadExecutor t2 = new OtherThreadExecutor("T2");
    private final OtherThreadExecutor t3 = new OtherThreadExecutor("T3");

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private LockManager locks;

    @AfterEach
    void tearDown() {
        closeAllUnchecked(t2, t3);
    }

    @Test
    void transactionShouldReleaseLocksWhenGraphDbIsBeingShutdown() {
        assertEquals(0, lockCount(locks));

        assertThrows(TransactionTerminatedException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode();
                tx.acquireWriteLock(node);
                assertThat(lockCount(locks)).isGreaterThanOrEqualTo(1);

                managementService.shutdown();

                tx.createNode();
                tx.commit();
            }
        });

        assertFalse(db.isAvailable());
        assertEquals(0, lockCount(locks));
    }

    @Test
    void shouldBeAbleToShutdownWhenThereAreTransactionsWaitingForLocks() {
        // GIVEN
        final Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        final CountDownLatch nodeLockedLatch = new CountDownLatch(1);
        final CountDownLatch shutdownCalled = new CountDownLatch(1);

        // WHEN
        // one thread locks previously created node and initiates graph db shutdown
        Future<Void> shutdownFuture = t2.executeDontWait(() -> {
            try (Transaction tx = db.beginTx()) {
                tx.getNodeById(node.getId()).addLabel(label("ABC"));
                nodeLockedLatch.countDown();

                // Wait for T3 to start waiting for this node write lock
                t3.waitUntilWaiting(details -> details.isAt(ForsetiClient.class, "acquireExclusive"));

                managementService.shutdown();

                shutdownCalled.countDown();
                tx.commit();
            }
            return null;
        });

        // other thread tries to lock the same node while it has been locked and graph db is being shutdown
        Future<Void> secondTxResult = t3.executeDontWait(() -> {
            try (Transaction tx = db.beginTx()) {
                nodeLockedLatch.await();

                // T2 awaits this thread to get into a waiting state for this node write lock
                tx.getNodeById(node.getId()).addLabel(label("DEF"));

                shutdownCalled.await();
                tx.commit();
            }
            return null;
        });

        // start waiting when the trap has been triggered
        assertThatThrownBy(() -> secondTxResult.get(60, SECONDS))
                .rootCause()
                .isInstanceOf(TransactionTerminatedException.class);

        assertThatThrownBy(shutdownFuture::get).rootCause().isInstanceOf(TransactionTerminatedException.class);
    }

    private static int lockCount(LockManager locks) {
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept(lockCountVisitor);
        return lockCountVisitor.getLockCount();
    }
}
