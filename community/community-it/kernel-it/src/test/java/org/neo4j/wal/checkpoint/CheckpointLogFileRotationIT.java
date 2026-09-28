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
package org.neo4j.wal.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.wal.checkpoint.CheckpointLogSerializationHelper.CHECKPOINT_REASON;
import static org.neo4j.wal.checkpoint.CheckpointLogSerializationHelper.CONFIG_ROTATION_THRESHOLD;
import static org.neo4j.wal.checkpoint.CheckpointLogSerializationHelper.LOG_POSITION;
import static org.neo4j.wal.checkpoint.CheckpointLogSerializationHelper.TRANSACTION_ID;
import static org.neo4j.wal.checkpoint.CheckpointLogSerializationHelper.fillWithCheckpoints;
import static org.neo4j.wal.checkpoint.LogCheckPointEvent.NULL;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.wal.WALService;

@DbmsExtension(configurationCallback = "configure")
public class CheckpointLogFileRotationIT {
    @Inject
    WALService walService;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(checkpoint_logical_log_rotation_threshold, CONFIG_ROTATION_THRESHOLD)
                .setConfig(checkpoint_logical_log_keep_threshold, 100)
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, preallocateLogs());
    }

    @Test
    void rotateCheckpointLogFiles() throws IOException {
        var checkpointFile = walService.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        fillWithCheckpoints(5, checkpointAppender);
        var matchedFiles = checkpointFile.getMatchedFiles();
        assertThat(matchedFiles).hasSize(5); // 5 filled
        for (var fileWithCheckpoints : matchedFiles) {
            assertThat(fileWithCheckpoints.toFile().length())
                    .isLessThanOrEqualTo(CheckpointLogSerializationHelper.getMaxCheckpointFileSize());
        }
    }

    @Test
    void doNotRotateWhileCheckpointsAreFitting() throws IOException {
        var checkpointFile = walService.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        fillWithCheckpoints(1, checkpointAppender);
        assertThat(checkpointFile.getMatchedFiles()).hasSize(1);
        var logFile = checkpointFile.getMatchedFiles()[0];
        assertThat(logFile.toFile().length())
                .isLessThanOrEqualTo(CheckpointLogSerializationHelper.getMaxCheckpointFileSize());
        assertThat(logFile.toFile().length())
                .isGreaterThanOrEqualTo(CheckpointLogSerializationHelper.getMaxCheckpointFileSize()
                        - CheckpointLogSerializationHelper.getCheckpointRecordLengthBytes());
    }

    @Test
    void afterRotationNewFileHaveHeader() throws IOException {
        var checkpointFile = walService.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        fillWithCheckpoints(1, checkpointAppender);
        // Rotation happens on first checkpoint seen after the threshold is passed
        checkpointAppender.checkPoint(
                NULL,
                TRANSACTION_ID,
                TRANSACTION_ID.id() + 77,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                LOG_POSITION,
                LOG_POSITION,
                Instant.now(),
                CHECKPOINT_REASON);

        Path[] matchedFiles = checkpointFile.getMatchedFiles();
        assertThat(matchedFiles).hasSize(2);
        boolean headerFileFound = false;
        for (Path matchedFile : matchedFiles) {
            if (checkpointFile.getLogVersion(matchedFile) == 1) {
                // Should contain header and one checkpoint
                assertThat(matchedFile.toFile()).hasSize(expectedNewFileSize());
                headerFileFound = true;
            }
        }
        assertTrue(headerFileFound);
    }

    protected long expectedNewFileSize() {
        return CheckpointLogSerializationHelper.expectedNewCheckpointFileSize();
    }

    protected boolean preallocateLogs() {
        return false;
    }
}
