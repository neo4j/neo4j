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
package org.neo4j.consistency;

import static java.lang.Integer.max;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

/**
 * This is a test for triggering a race which was found in and around {@link RecordStorageEngine#apply(StorageEngineTransaction, TransactionApplicationMode)}
 * where e.g. a transaction A which did CREATE NODE N and transaction B which did DELETE NODE N would have a chance to be applied to the
 * label index in the reverse order, i.e. transaction B before transaction A, resulting in outdated label data remaining in the label index.
 */
@DbmsExtension(configurationCallback = "configure")
class LabelScanStoreTxApplyRaceIT {
    // === CONTROL PANEL ===
    private static final int NUMBER_OF_DELETORS = 2;
    private static final int NUMBER_OF_CREATORS =
            max(2, Runtime.getRuntime().availableProcessors() - NUMBER_OF_DELETORS);
    private static final float CHANCE_LARGE_TX = 0.1f;
    private static final float CHANCE_TO_DELETE_BY_SAME_THREAD = 0.9f;
    private static final int LARGE_TX_SIZE = 3_000;

    private static final Label[] LABELS =
            new Label[] {TestLabels.LABEL_ONE, TestLabels.LABEL_TWO, TestLabels.LABEL_THREE};

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private TestDirectory testDirectory;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, PageAligned.LATEST_NAME);
    }

    /**
     * The test case is basically loads of concurrent CREATE/DELETE NODE or sometimes just CREATE, keeping the created node in an array
     * for dedicated deleter threads to pick up and delete as fast as they can see them. This concurrently with large creation transactions.
     */
    @Test
    void shouldStressIt() throws Throwable {
        // given
        Race race = new Race().withMaxDuration(5, TimeUnit.SECONDS);
        AtomicReferenceArray<Node> nodeHeads = new AtomicReferenceArray<>(NUMBER_OF_CREATORS);
        for (int i = 0; i < NUMBER_OF_CREATORS; i++) {
            race.addContestant(creator(nodeHeads, i));
        }
        race.addContestants(NUMBER_OF_DELETORS, deleter(nodeHeads));

        // when
        race.go();

        // then
        RecordDatabaseLayout dbLayout = (RecordDatabaseLayout) db.databaseLayout();
        managementService.shutdown();

        assertTrue(new ConsistencyCheckService(dbLayout)
                .with(defaults(GraphDatabaseSettings.neo4j_home, testDirectory.homePath()))
                .with(ConsistencyFlags.ALL)
                .runFullConsistencyCheck()
                .isSuccessful());
    }

    private Runnable creator(AtomicReferenceArray<Node> nodeHeads, int guy) {
        return new Runnable() {
            private final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run() {
                if (random.nextFloat() < CHANCE_LARGE_TX) {
                    // Few large transactions
                    try (Transaction tx = db.beginTx()) {
                        for (int i = 0; i < LARGE_TX_SIZE; i++) {
                            // Nodes are created with properties here. Whereas the properties don't have a functional
                            // impact on this test they do affect timings so that the issue is (was) triggered more
                            // often
                            // and therefore have a positive effect on this test.
                            tx.createNode(randomLabels())
                                    .setProperty("name", randomUUID().toString());
                        }
                        tx.commit();
                    }
                } else {
                    // Many small create/delete transactions
                    Node node;
                    try (Transaction tx = db.beginTx()) {
                        node = tx.createNode(randomLabels());
                        nodeHeads.set(guy, node);
                        tx.commit();
                    }
                    if (random.nextFloat() < CHANCE_TO_DELETE_BY_SAME_THREAD) {
                        // Most of the time delete in this thread
                        if (nodeHeads.getAndSet(guy, null) != null) {
                            try (Transaction tx = db.beginTx()) {
                                tx.getNodeById(node.getId()).delete();
                                tx.commit();
                            }
                        }
                        // Otherwise there will be other threads sitting there waiting for these nodes and deletes them
                        // if they can
                    }
                }
            }

            private Label[] randomLabels() {
                Label[] labels = new Label[LABELS.length];
                int cursor = 0;
                for (int i = 0; i < labels.length; i++) {
                    if (random.nextBoolean()) {
                        labels[cursor++] = LABELS[i];
                    }
                }
                if (cursor == 0) {
                    // at least one
                    labels[cursor++] = LABELS[0];
                }
                return Arrays.copyOf(labels, cursor);
            }
        };
    }

    private Runnable deleter(AtomicReferenceArray<Node> nodeHeads) {
        return new Runnable() {
            final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run() {
                int guy = random.nextInt(NUMBER_OF_CREATORS);
                Node node = nodeHeads.getAndSet(guy, null);
                if (node != null) {
                    try (Transaction tx = db.beginTx()) {
                        tx.getNodeById(node.getId()).delete();
                        tx.commit();
                    } catch (NotFoundException e) {
                        // This is OK in this test
                    }
                }
            }
        };
    }
}
