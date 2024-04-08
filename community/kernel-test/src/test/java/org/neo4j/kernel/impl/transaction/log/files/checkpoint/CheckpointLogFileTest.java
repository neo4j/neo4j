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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class CheckpointLogFileTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private final long rotationThreshold = ByteUnit.kibiBytes(1);
    private final DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance());
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, LATEST_KERNEL_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private CheckpointFile checkpointFile;

    @BeforeEach
    void setUp() throws IOException {
        LogFiles logFiles = buildLogFiles();
        life.add(logFiles);
        life.start();
        checkpointFile = logFiles.getCheckpointFile();
    }

    @Test
    void failToWriteCheckpointAfterShutdown() {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        TransactionId transactionId = new TransactionId(5, LATEST_KERNEL_VERSION, 6, 7, 8);
        assertDoesNotThrow(() -> checkpointAppender.checkPoint(
                NULL,
                transactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(1, 2),
                Instant.now(),
                "test"));

        // we can't append checkpoint for null transaction id
        assertThrows(
                NullPointerException.class,
                () -> checkpointAppender.checkPoint(
                        NULL,
                        null,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        new LogPosition(1, 2),
                        Instant.now(),
                        "test"));

        life.shutdown();

        // no attempt is made to write any checkpoint here
        assertDoesNotThrow(() -> checkpointAppender.checkPoint(
                NULL, null, LatestVersions.LATEST_KERNEL_VERSION, new LogPosition(1, 2), Instant.now(), "test"));
    }

    @Test
    void provideMatchedCheckpointFiles() throws IOException {
        Path[] matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(1);
        assertEquals(matchedFiles[0], checkpointFile.getCurrentFile());
    }

    @Test
    void latestCheckpointLookupLastCheckpoint() throws IOException {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertThat(checkpointFile.findLatestCheckpoint()).isEmpty();

        var firstLogPosition = new LogPosition(1, 2);
        var firstTransactionId = new TransactionId(1, LATEST_KERNEL_VERSION, 2, 3, 4);
        checkpointAppender.checkPoint(
                NULL,
                firstTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                firstLogPosition,
                Instant.now(),
                "test");
        assertEquals(
                firstLogPosition,
                checkpointFile.findLatestCheckpoint().orElseThrow().transactionLogPosition());

        var secondLogPosition = new LogPosition(2, 3);
        var secondTransactionId = new TransactionId(2, LATEST_KERNEL_VERSION, 3, 4, 5);
        checkpointAppender.checkPoint(
                NULL,
                secondTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                secondLogPosition,
                Instant.now(),
                "test");
        assertEquals(
                secondLogPosition,
                checkpointFile.findLatestCheckpoint().orElseThrow().transactionLogPosition());

        var thirdLogPosition = new LogPosition(3, 4);
        var thirdTransactionId = new TransactionId(3, LATEST_KERNEL_VERSION, 4, 5, 6);
        checkpointAppender.checkPoint(
                NULL,
                thirdTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                thirdLogPosition,
                Instant.now(),
                "test");
        assertEquals(
                thirdLogPosition,
                checkpointFile.findLatestCheckpoint().orElseThrow().transactionLogPosition());

        var checkpointInfos = checkpointFile.reachableCheckpoints();
        assertThat(checkpointInfos).hasSize(3);
        assertThat(checkpointInfos.get(0)).hasFieldOrPropertyWithValue("transactionLogPosition", firstLogPosition);
        assertThat(checkpointInfos.get(1)).hasFieldOrPropertyWithValue("transactionLogPosition", secondLogPosition);
        assertThat(checkpointInfos.get(2)).hasFieldOrPropertyWithValue("transactionLogPosition", thirdLogPosition);
    }

    @Test
    void reachableCheckpointsShouldBeSorted() throws IOException {
        // Write 6 checkpoints to get one rotation and see that reachableCheckpoints are correctly sorted with several
        // checkpoint files
        var checkpointAppender = checkpointFile.getCheckpointAppender();

        var firstLogPosition = new LogPosition(1, 2);
        var firstTransactionId = new TransactionId(1, LATEST_KERNEL_VERSION, 2, 3, 4);
        checkpointAppender.checkPoint(
                NULL,
                firstTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                firstLogPosition,
                Instant.now(),
                "test");
        var secondLogPosition = new LogPosition(2, 3);
        var secondTransactionId = new TransactionId(2, LATEST_KERNEL_VERSION, 3, 43, 44);
        checkpointAppender.checkPoint(
                NULL,
                secondTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                secondLogPosition,
                Instant.now(),
                "test");
        var thirdLogPosition = new LogPosition(3, 4);
        var thirdTransactionId = new TransactionId(3, LATEST_KERNEL_VERSION, 4, 5, 6);
        checkpointAppender.checkPoint(
                NULL,
                thirdTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                thirdLogPosition,
                Instant.now(),
                "test");
        var fourthLogPosition = new LogPosition(4, 5);
        var fourthTransactionId = new TransactionId(4, LATEST_KERNEL_VERSION, 5, 6, 7);
        checkpointAppender.checkPoint(
                NULL,
                fourthTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                fourthLogPosition,
                Instant.now(),
                "test");
        var fifthLogPosition = new LogPosition(5, 6);
        var fifthTransactionId = new TransactionId(5, LATEST_KERNEL_VERSION, 6, 7, 8);
        checkpointAppender.checkPoint(
                NULL,
                fifthTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                fifthLogPosition,
                Instant.now(),
                "test");
        var sixthLogPosition = new LogPosition(6, 7);
        var sixthTransactionId = new TransactionId(6, LATEST_KERNEL_VERSION, 7, 8, 9);
        checkpointAppender.checkPoint(
                NULL,
                sixthTransactionId,
                LatestVersions.LATEST_KERNEL_VERSION,
                sixthLogPosition,
                Instant.now(),
                "test");

        var checkpointInfos = checkpointFile.reachableCheckpoints();
        assertThat(checkpointInfos).hasSize(6);
        assertThat(checkpointInfos.get(0)).hasFieldOrPropertyWithValue("transactionLogPosition", firstLogPosition);
        assertThat(checkpointInfos.get(1)).hasFieldOrPropertyWithValue("transactionLogPosition", secondLogPosition);
        assertThat(checkpointInfos.get(2)).hasFieldOrPropertyWithValue("transactionLogPosition", thirdLogPosition);
        assertThat(checkpointInfos.get(3)).hasFieldOrPropertyWithValue("transactionLogPosition", fourthLogPosition);
        assertThat(checkpointInfos.get(4)).hasFieldOrPropertyWithValue("transactionLogPosition", fifthLogPosition);
        assertThat(checkpointInfos.get(5)).hasFieldOrPropertyWithValue("transactionLogPosition", sixthLogPosition);
    }

    @Test
    void findAllCheckpoints() throws IOException {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertThat(checkpointFile.reachableCheckpoints()).isEmpty();

        checkpointAppender.checkPoint(
                NULL,
                new TransactionId(1, LATEST_KERNEL_VERSION, 2, 3, 4),
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(1, 2),
                Instant.now(),
                "test");
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(1);

        checkpointAppender.checkPoint(
                NULL,
                new TransactionId(1, LATEST_KERNEL_VERSION, 2, 3, 4),
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(2, 3),
                Instant.now(),
                "test");
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(2);

        checkpointAppender.checkPoint(
                NULL,
                new TransactionId(1, LATEST_KERNEL_VERSION, 2, 3, 4),
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(3, 4),
                Instant.now(),
                "test");
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(3);
    }

    private LogFiles buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withDatabaseHealth(databaseHealth)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }
}
