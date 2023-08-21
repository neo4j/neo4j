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
package org.neo4j.kernel.impl.transaction;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.kernel.database.NoOpSystemGraphInitializer.noOpSystemGraphInitializer;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class CommitContentionTest {
    @Inject
    private TestDirectory testDirectory;

    private final Semaphore semaphore1 = new Semaphore(1);
    private final Semaphore semaphore2 = new Semaphore(1);
    private final AtomicReference<Exception> reference = new AtomicReference<>();
    private final AtomicBoolean interceptorEnabled = new AtomicBoolean();

    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() throws Exception {
        db = createDb();
        semaphore1.acquire();
        semaphore2.acquire();
    }

    @AfterEach
    void after() {
        managementService.shutdown();
    }

    @Test
    void shouldNotContendOnCommitWhenPushingUpdates() throws Exception {
        interceptorEnabled.set(true);

        Thread thread = startFirstTransactionWhichBlocksDuringPushUntilSecondTransactionFinishes();
        runAndFinishSecondTransaction();
        thread.join();
        assertNoFailures();
    }

    private void assertNoFailures() {
        Exception e = reference.get();

        if (e != null) {
            throw new AssertionError(e);
        }
    }

    private void runAndFinishSecondTransaction() {
        createNode();

        signalSecondTransactionFinished();
    }

    private void createNode() {
        try (Transaction transaction = db.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
    }

    private Thread startFirstTransactionWhichBlocksDuringPushUntilSecondTransactionFinishes()
            throws InterruptedException {
        Thread thread = new Thread(this::createNode);

        thread.start();

        waitForFirstTransactionToStartPushing();

        return thread;
    }

    private GraphDatabaseService createDb() {
        Config cfg = Config.newBuilder()
                .set(neo4j_home, testDirectory.absolutePath())
                .build();
        managementService = new DatabaseManagementServiceFactory(
                        DbmsInfo.COMMUNITY, globalModule -> new CommunityEditionModule(globalModule) {
                            @Override
                            public DatabaseTransactionStats.Factory getTransactionMonitorFactory() {
                                return SkipTransactionDatabaseStats::new;
                            }
                        })
                .build(
                        cfg,
                        false,
                        GraphDatabaseDependencies.newDependencies().dependencies(noOpSystemGraphInitializer(cfg)));
        return managementService.database(cfg.get(GraphDatabaseSettings.initial_default_database));
    }

    private void waitForFirstTransactionToStartPushing() throws InterruptedException {
        if (!semaphore1.tryAcquire(10, SECONDS)) {
            throw new IllegalStateException("First transaction never started pushing");
        }
    }

    private void signalFirstTransactionStartedPushing() {
        semaphore1.release();
    }

    private void signalSecondTransactionFinished() {
        semaphore2.release();
    }

    private void waitForSecondTransactionToFinish() {
        try {
            boolean acquired = semaphore2.tryAcquire(10, SECONDS);

            if (!acquired) {
                reference.set(new IllegalStateException("Second transaction never finished"));
            }
        } catch (InterruptedException e) {
            reference.set(e);
        }
    }

    private class SkipTransactionDatabaseStats extends DatabaseTransactionStats {
        boolean skip;

        @Override
        public void transactionFinished(boolean committed, boolean write) {
            super.transactionFinished(committed, write);

            if (committed && interceptorEnabled.get()) {
                // skip signal and waiting for second transaction
                if (skip) {
                    return;
                }
                skip = true;

                signalFirstTransactionStartedPushing();

                waitForSecondTransactionToFinish();
            }
        }
    }
}
