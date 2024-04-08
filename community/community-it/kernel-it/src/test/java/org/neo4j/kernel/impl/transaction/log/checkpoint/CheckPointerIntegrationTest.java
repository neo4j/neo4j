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
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.io.ByteUnit.gibiBytes;
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
import org.neo4j.storageengine.api.MetadataProvider;
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
        DatabaseManagementService managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        Thread.sleep(10);
        managementService.shutdown();
    }

    @Test
    void latestKernelVersionInCheckpointByDefault() throws Exception {
        DatabaseManagementService managementService = builder.build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            getCheckPointer(db).forceCheckPoint(new SimpleTriggerInfo("test"));
            List<CheckpointInfo> checkpointInfos = checkPointsInTxLog(db);
            assertEquals(
                    LatestVersions.LATEST_KERNEL_VERSION,
                    checkpointInfos.get(checkpointInfos.size() - 1).kernelVersion());
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void shouldCheckPointBasedOnTime() throws Throwable {
        // given
        long millis = 200;
        DatabaseManagementService managementService = builder.setConfig(check_point_interval_time, ofMillis(millis))
                .setConfig(check_point_interval_tx, 10000)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
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
        managementService.shutdown();

        managementService = builder.build();
        try {
            // then - 2 check points have been written in the log
            List<CheckpointInfo> checkPoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));

            assertTrue(
                    checkPoints.size() >= 2,
                    "Expected at least two (at least one for time interval and one for shutdown), was " + checkPoints);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void shouldCheckPointBasedOnTxCount() throws Throwable {
        // given
        DatabaseManagementService managementService = builder.setConfig(check_point_interval_time, ofMillis(300))
                .setConfig(check_point_interval_tx, 1)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
        int counter;
        try {
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
        } finally {
            managementService.shutdown();
        }

        managementService = builder.build();
        try {
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
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void shouldNotCheckPointWhenThereAreNoCommits() throws Throwable {
        // given
        DatabaseManagementService managementService = builder.setConfig(
                        check_point_interval_time, Duration.ofSeconds(1))
                .setConfig(check_point_interval_tx, 10000)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

        GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("given"));

        var checkPointsBefore = checkPointsInTxLog(db).size();
        // when

        // nothing happens

        triggerCheckPointAttempt(db);
        assertThat(checkPointsInTxLog(db)).hasSize(checkPointsBefore);
        managementService.shutdown();

        managementService = builder.build();
        try {
            // then - 1 check point has been written in the log
            var checkPoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));
            assertEquals(checkPointsBefore + 1, checkPoints.size());
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void shouldBeAbleToStartAndShutdownMultipleTimesTheDBWithoutCommittingTransactions() throws Throwable {
        // given
        TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder = builder.setConfig(
                        check_point_interval_time, Duration.ofMinutes(300))
                .setConfig(check_point_interval_tx, 10000)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1));

        // when
        DatabaseManagementService managementService = databaseManagementServiceBuilder.build();
        managementService.shutdown();
        managementService = databaseManagementServiceBuilder.build();
        managementService.shutdown();

        // then - 2 check points have been written in the log + 1 checkpoint after init on db creation
        managementService = builder.build();
        try {
            var checkpoints = checkPointsInTxLog(managementService.database(DEFAULT_DATABASE_NAME));
            assertEquals(3, checkpoints.size());
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void readTransactionInfoFromCheckpointRecord() throws IOException {
        var managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
        try {
            var databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            for (int i = 0; i < 10; i++) {
                try (Transaction transaction = databaseAPI.beginTx()) {
                    transaction.createNode();
                    transaction.commit();
                }
            }
            var closedTxMetadata = getMetadataProvider(databaseAPI).getLastClosedTransaction();
            var lastClosedTxId = closedTxMetadata.transactionId();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("test"));
            var checkpointInfos = checkPointsInTxLog(databaseAPI);
            TransactionId lastCheckpointTxId =
                    checkpointInfos.get(checkpointInfos.size() - 1).transactionId();
            assertEquals(lastClosedTxId, lastCheckpointTxId);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void tracePageCacheAccessOnCheckpoint() throws Exception {
        var managementService = builder.setConfig(check_point_interval_time, ofMillis(0))
                .setConfig(check_point_interval_tx, 1)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .build();
        try {
            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var cacheTracer = databaseAPI.getDependencyResolver().resolveDependency(PageCacheTracer.class);

            long initialFlushes = cacheTracer.flushes();
            long initialBytesWritten = cacheTracer.bytesWritten();
            long initialPins = cacheTracer.pins();

            getCheckPointer(databaseAPI).forceCheckPoint(new SimpleTriggerInfo("tracing"));

            assertThat(cacheTracer.flushes()).isGreaterThan(initialFlushes);
            assertThat(cacheTracer.bytesWritten()).isGreaterThan(initialBytesWritten);
            assertThat(cacheTracer.pins()).isGreaterThan(initialPins);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void checkpointMessageWithNotConfiguredIOController() throws IOException {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var managementService = builder.setConfig(check_point_interval_time, ofHours(7))
                .setConfig(check_point_interval_tx, 10_000)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .setInternalLogProvider(logProvider)
                .build();
        try {
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
                            greaterThan(30),
                            greaterThan(2),
                            greaterThan(30));

        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void checkpointMessageWithDifferentNumberOfIOsWithNotConfiguredIOController() throws IOException {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var managementService = builder.setConfig(check_point_interval_time, ofHours(7))
                .setConfig(check_point_interval_tx, 10_000)
                .setConfig(logical_log_rotation_threshold, gibiBytes(1))
                .setInternalLogProvider(logProvider)
                .build();
        try {
            GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var cacheTracer = databaseAPI.getDependencyResolver().resolveDependency(PageCacheTracer.class);

            String property = RandomStringUtils.randomAscii((int) kibiBytes(2));
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
                            greaterThan(40),
                            greaterThan(6),
                            greaterThan(50));

        } finally {
            managementService.shutdown();
        }
    }

    private static void triggerCheckPointAttempt(GraphDatabaseService db) throws Exception {
        // Simulates triggering the checkpointer background job which runs now and then, checking whether
        // or not there's a need to perform a checkpoint.
        getCheckPointer((GraphDatabaseAPI) db).checkPointIfNeeded(new SimpleTriggerInfo("Test"));
    }

    private MetadataProvider getMetadataProvider(GraphDatabaseAPI databaseAPI) {
        return databaseAPI.getDependencyResolver().resolveDependency(MetadataProvider.class);
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
