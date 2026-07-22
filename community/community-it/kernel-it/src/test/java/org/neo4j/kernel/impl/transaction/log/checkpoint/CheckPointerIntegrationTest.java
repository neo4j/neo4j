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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static java.lang.Math.toIntExact;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.logging.LogAssertions.greaterThan;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class CheckPointerIntegrationTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private TestDatabaseManagementServiceBuilder builder;

    @BeforeEach
    void setup() {
        builder = new TestDatabaseManagementServiceBuilder(databaseLayout).setFileSystem(fs);
    }

    @Test
    void databaseShutdownDuringConstantCheckPointing() throws InterruptedException {
        try (DatabaseManagementService managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }
            Thread.sleep(10);
        }
    }

    @Test
    void latestKernelVersionInCheckpointByDefault() throws Exception {
        try (DatabaseManagementService managementService = builder.build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            getCheckPointer(db).forceCheckPoint(new SimpleTriggerInfo("test"));
            List<CheckpointInfo> checkpointInfos = checkPointsInTxLog(db);
            assertEquals(
                    LatestVersions.LATEST_KERNEL_VERSION,
                    checkpointInfos.getLast().kernelVersion());
        }
    }

    @Test
    void shouldCheckPointBasedOnTime() throws Throwable {
        // given
        long millis = 200;
        try (DatabaseManagementService managementService = builder.setConfig(
                        check_point_interval_time, ofMillis(millis))
                .setConfig(check_point_interval_tx, 10000)
                .build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

            // when
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            // The scheduled job checking whether or not checkpoints are needed runs more frequently
            // now that we've set the time interval so low, so we can simply wait for it here
            long endTime = currentTimeMillis() + SECONDS.toMillis(30);
            while (checkPointsInTxLog(db).isEmpty()) {
                Thread.sleep(millis);
                assertTrue(currentTimeMillis() < endTime, "Took too long to produce a checkpoint");
            }
        }

        try (DatabaseManagementService managementService = builder.build()) {
            // then - 2 check points have been written in the log
            List<CheckpointInfo> checkPoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));

            assertTrue(
                    checkPoints.size() >= 2,
                    "Expected at least two (at least one for time interval and one for shutdown), was " + checkPoints);
        }
    }

    @Test
    void shouldCheckPointBasedOnTxCount() throws Throwable {
        // given
        int counter;
        try (DatabaseManagementService managementService = builder.setConfig(check_point_interval_time, ofMillis(300))
                .setConfig(check_point_interval_tx, 1)
                .build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

            // when
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            // Instead of waiting 10s for the background job to do this check, perform the check right here
            triggerCheckPointAttempt(db);

            List<CheckpointInfo> checkpoints = checkPointsInTxLog(db);
            assertThat(checkpoints).isNotEmpty();
            counter = checkpoints.size();
        }

        try (DatabaseManagementService managementService = builder.build()) {
            // then - checkpoints + shutdown checkpoint have been written in the log
            var checkpointInfos = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));

            // Use greater-than-or-equal-to in order to accommodate the following data-race:
            // Since the `threshold.isCheckPointingNeeded()` call in CheckPointerImpl is done outside of the
            // `mutex.checkPoint()` lock,
            // and also the `check_point_interval_time` is 300 milliseconds, it means that our direct
            // `triggerCheckPointAttempt( db )` call
            // can race with the scheduled checkpoints, and both can decide that a checkpoint is needed. They will then
            // coordinate via the
            // lock to do two checkpoints, one after the other. If our direct call wins the race and goes first, then
            // the scheduled
            // checkpoint will race with our `checkPointInTxLog( db )` call, which can then count only one checkpoint in
            // the log when there
            // are actually two.
            assertThat(checkpointInfos.size()).isGreaterThanOrEqualTo(counter + 1);
        }
    }

    @Test
    void shouldNotCheckPointWhenThereAreNoCommits() throws Throwable {
        // given
        int checkPointsBefore;
        try (DatabaseManagementService managementService = builder.setConfig(
                        check_point_interval_time, Duration.ofSeconds(1))
                .setConfig(check_point_interval_tx, 10000)
                .build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("given"));

            checkPointsBefore = checkPointsInTxLog(db).size();
            // when

            // nothing happens

            triggerCheckPointAttempt(db);
            assertThat(checkPointsInTxLog(db)).hasSize(checkPointsBefore);
        }

        try (DatabaseManagementService managementService = builder.build()) {
            // then - 1 check point has been written in the log
            var checkPoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));
            assertEquals(checkPointsBefore + 1, checkPoints.size());
        }
    }

    @Test
    void shouldBeAbleToStartAndShutdownMultipleTimesTheDBWithoutCommittingTransactions() throws Throwable {
        // given
        TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder = builder.setConfig(
                        check_point_interval_time, Duration.ofMinutes(300))
                .setConfig(check_point_interval_tx, 10000);

        // when
        int initialCheckpoints;
        try (DatabaseManagementService managementService = databaseManagementServiceBuilder.build()) {
            initialCheckpoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME))
                    .size();
        }
        try (DatabaseManagementService managementService = databaseManagementServiceBuilder.build()) {
            // Just starting and shutting down
        }

        // then - 2 check points have been written in the log + 1 checkpoint after init on db creation
        try (DatabaseManagementService managementService = builder.build()) {
            var checkpoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));
            assertEquals(initialCheckpoints + 2, checkpoints.size());
        }
    }

    @Test
    void readTransactionInfoFromCheckpointRecord() throws IOException {
        try (var managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .build()) {
            var databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            for (int i = 0; i < 10; i++) {
                try (Transaction transaction = databaseAPI.beginTx()) {
                    transaction.createNode();
                    transaction.commit();
                }
            }
            var closedTxMetadata = getMetadataProvider(databaseAPI).getHighestGapFreeClosedTransaction();
            var lastClosedTxId = closedTxMetadata.transactionId();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("test"));
            var checkpointInfos = checkPointsInTxLog(databaseAPI);
            TransactionId lastCheckpointTxId = checkpointInfos.getLast().transactionId();
            assertEquals(lastClosedTxId, lastCheckpointTxId);
        }
    }

    @Test
    void oldestNotVisibleTransactionIsTheSameAsTransactionPositionOnTheDatabaseWithoutExplicitTransactions()
            throws IOException {
        try (DatabaseManagementService managementService = builder.build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            getCheckPointer(db).forceCheckPoint(new SimpleTriggerInfo("test"));
            List<CheckpointInfo> checkpointInfos = checkPointsInTxLog(db);
            CheckpointInfo lastCheckpoint = checkpointInfos.getLast();
            assertEquals(
                    lastCheckpoint.oldestNotVisibleTransactionLogPosition(), lastCheckpoint.transactionLogPosition());
        }
    }

    @Test
    void oldestNotVisibleTransactionIsTheSameAsTransactionPositionOnTheDatabaseAfterTransactions() throws IOException {
        try (var managementService = builder.build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

            for (int i = 0; i < 17; i++) {
                try (Transaction transaction = db.beginTx()) {
                    Node startNode = transaction.createNode();
                    Node endNode = transaction.createNode();
                    startNode.createRelationshipTo(endNode, RelationshipType.withName("foo" + i));
                    transaction.commit();
                }
            }

            List<CheckpointInfo> checkpointInfos = checkPointsInTxLog(db);
            CheckpointInfo lastCheckpoint = checkpointInfos.getLast();
            assertEquals(
                    lastCheckpoint.oldestNotVisibleTransactionLogPosition(), lastCheckpoint.transactionLogPosition());
        }
    }

    @Test
    void tracePageCacheAccessOnCheckpoint() throws Exception {
        try (var managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .build()) {
            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var cacheTracer = databaseAPI.getDependencyResolver().resolveDependency(PageCacheTracer.class);

            long initialFlushes = cacheTracer.flushes();
            long initialBytesWritten = cacheTracer.bytesWritten();
            long initialPins = cacheTracer.pins();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("tracing"));

            assertThat(cacheTracer.flushes()).isGreaterThan(initialFlushes);
            assertThat(cacheTracer.bytesWritten()).isGreaterThan(initialBytesWritten);
            assertThat(cacheTracer.pins()).isGreaterThan(initialPins);
        }
    }

    @Test
    void checkpointMessageWithNotConfiguredIOController() throws IOException {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        try (var managementService = builder.setConfig(check_point_interval_time, ofHours(7))
                .setConfig(check_point_interval_tx, 10_000)
                .setInternalLogProvider(logProvider)
                .build()) {
            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var cacheTracer = databaseAPI.getDependencyResolver().resolveDependency(PageCacheTracer.class);

            long initialFlushes = cacheTracer.flushes();
            long initialBytesWritten = cacheTracer.bytesWritten();
            long initialPins = cacheTracer.pins();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("tracing"));

            assertThat(cacheTracer.flushes()).isGreaterThan(initialFlushes);
            assertThat(cacheTracer.bytesWritten()).isGreaterThan(initialBytesWritten);
            assertThat(cacheTracer.pins()).isGreaterThan(initialPins);

            LogAssertions.assertThat(logProvider)
                    .forClass(CheckPointerImpl.class)
                    .containsMessages(
                            Pattern.compile(
                                    "Checkpoint flushed (\\d+) pages \\(\\d+% of total available pages\\), in \\d+ IOs. Checkpoint performed with IO limit: unlimited, paused in total"),
                            greaterThan(10),
                            greaterThan(0),
                            greaterThan(10));
        }
    }

    @Test
    void checkpointMessageWithDifferentNumberOfIOsWithNotConfiguredIOController() throws IOException {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        try (var managementService = builder.setConfig(check_point_interval_time, ofHours(7))
                .setConfig(check_point_interval_tx, 10_000)
                .setInternalLogProvider(logProvider)
                .build()) {
            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var cacheTracer = databaseAPI.getDependencyResolver().resolveDependency(PageCacheTracer.class);

            String property = RandomStringUtils.insecure().nextAscii((int) kibiBytes(2));
            for (int i = 0; i < 100; i++) {
                try (var transaction = databaseAPI.beginTx()) {
                    Node nodeA = transaction.createNode();
                    Node nodeB = transaction.createNode();
                    nodeA.setProperty("a", property);
                    nodeA.createRelationshipTo(nodeB, RelationshipType.withName("foo"));
                    transaction.commit();
                }
            }

            long initialFlushes = cacheTracer.flushes();
            long initialBytesWritten = cacheTracer.bytesWritten();
            long initialPins = cacheTracer.pins();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("tracing"));

            assertThat(cacheTracer.flushes()).isGreaterThan(initialFlushes);
            assertThat(cacheTracer.bytesWritten()).isGreaterThan(initialBytesWritten);
            assertThat(cacheTracer.pins()).isGreaterThan(initialPins);

            LogAssertions.assertThat(logProvider)
                    .forClass(CheckPointerImpl.class)
                    .containsMessages(
                            Pattern.compile(
                                    "Checkpoint flushed (\\d+) pages \\((\\d+)% of total available pages\\), in (\\d+) IOs. Checkpoint performed with IO limit: unlimited, paused in total"),
                            greaterThan(25),
                            greaterThan(2),
                            greaterThan(25));
        }
    }

    @Test
    void shouldUpdateLowestAvailableCommittedTransactionIdOnPruning() throws IOException {
        // given
        int minNumTransactionsToKeep = 50;
        // when
        try (var dbms = builder.setConfig(check_point_interval_tx, 10)
                .setConfig(logical_log_rotation_threshold, kibiBytes(128))
                .setConfig(keep_logical_logs, minNumTransactionsToKeep + " txs")
                .build()) {
            var db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            var metadataProvider = db.getDependencyResolver().resolveDependency(LogMetadataProvider.class);
            var checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
            long prevLowestTxId = metadataProvider.getLowestAvailableCommittedTransactionId();
            assertThat(prevLowestTxId).isGreaterThan(0);
            int numLowestTxIdBumps = 0;

            for (int i = 0; i < 100; i++) {
                try (var tx = db.beginTx()) {
                    var node = tx.createNode();
                    node.setProperty("foo", "bar".repeat(10_000));
                    tx.commit();
                }
                if (checkPointer.checkPointIfNeeded(new SimpleTriggerInfo("Test")) != -1) {
                    // then
                    long lowestTxId = metadataProvider.getLowestAvailableCommittedTransactionId();
                    if (lowestTxId != prevLowestTxId) {
                        assertThat(lowestTxId).isGreaterThan(prevLowestTxId);
                        int numAvailableTransactions =
                                toIntExact(metadataProvider.getLastCommittedTransactionId() - lowestTxId + 1);
                        assertThat(numAvailableTransactions).isGreaterThanOrEqualTo(minNumTransactionsToKeep);
                        prevLowestTxId = lowestTxId;
                        numLowestTxIdBumps++;
                    }
                }
            }
            assertThat(numLowestTxIdBumps).isGreaterThan(0);
        }
    }

    private static void triggerCheckPointAttempt(GraphDatabaseService db) throws Exception {
        // Simulates triggering the checkpointer background job which runs now and then, checking whether
        // or not there's a need to perform a checkpoint.
        getCheckPointer((GraphDatabaseAPI) db).checkPointIfNeeded(new SimpleTriggerInfo("Test"));
    }

    private LogMetadataProvider getMetadataProvider(GraphDatabaseAPI databaseAPI) {
        return databaseAPI.getDependencyResolver().resolveDependency(LogMetadataProvider.class);
    }

    private static CheckPointer getCheckPointer(GraphDatabaseAPI db) {
        return db.getDependencyResolver().resolveDependency(CheckPointer.class);
    }

    private static List<CheckpointInfo> checkPointsInTxLog(GraphDatabaseService db) throws IOException {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        LogFiles logFiles = dependencyResolver.resolveDependency(LogFiles.class);
        return logFiles.getCheckpointFile().reachableCheckpoints();
    }
}
