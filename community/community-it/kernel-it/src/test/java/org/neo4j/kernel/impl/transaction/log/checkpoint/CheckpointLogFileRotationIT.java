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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.entry.v57.DetachedCheckpointLogEntrySerializerV5_7.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class CheckpointLogFileRotationIT {
    static final long ROTATION_THRESHOLD = kibiBytes(1);

    @Inject
    private GraphDatabaseService database;

    @Inject
    LogFiles logFiles;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(checkpoint_logical_log_rotation_threshold, ROTATION_THRESHOLD)
                .setConfig(checkpoint_logical_log_keep_threshold, 100)
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, preallocateLogs());
    }

    @Test
    void rotateCheckpointLogFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        var logPosition = new LogPosition(1000, 12345);
        var transactionId = new TransactionId(100, 101, 102, 103);
        var reason = "checkpoint for rotation test";
        for (int i = 0; i < 105; i++) {
            checkpointAppender.checkPoint(
                    NULL, transactionId, LatestVersions.LATEST_KERNEL_VERSION, logPosition, Instant.now(), reason);
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(27);
        for (var fileWithCheckpoints : matchedFiles) {
            assertThat(fileWithCheckpoints).satisfies(new Condition<>() {
                @Override
                public boolean matches(Path file) {
                    long length = file.toFile().length();
                    return length < ROTATION_THRESHOLD + RECORD_LENGTH_BYTES;
                }
            });
        }
    }

    @Test
    void doNotRotateWhileCheckpointsAreFitting() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition(1000, 12345);
        var transactionId = new TransactionId(100, 101, 102, 103);
        var reason = "checkpoint for rotation test";
        for (int i = LATEST_LOG_FORMAT.getHeaderSize() + 2 * RECORD_LENGTH_BYTES;
                i < ROTATION_THRESHOLD;
                i += RECORD_LENGTH_BYTES) {
            checkpointAppender.checkPoint(
                    NULL, transactionId, LatestVersions.LATEST_KERNEL_VERSION, logPosition, Instant.now(), reason);
        }
        assertThat(checkpointFile.getDetachedCheckpointFiles()).hasSize(1);
    }

    @Test
    void afterRotationNewFileHaveHeader() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition(1000, 12345);
        var transactionId = new TransactionId(100, 101, 102, 103);
        var reason = "checkpoint for rotation test";
        // there is one post init checkpoint
        for (int i = LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES;
                i < ROTATION_THRESHOLD;
                i += RECORD_LENGTH_BYTES) {
            checkpointAppender.checkPoint(
                    NULL, transactionId, LatestVersions.LATEST_KERNEL_VERSION, logPosition, Instant.now(), reason);
        }
        Path[] matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(2);
        boolean headerFileFound = false;
        for (Path matchedFile : matchedFiles) {
            if (checkpointFile.getDetachedCheckpointLogFileVersion(matchedFile) == 1) {
                assertThat(matchedFile.toFile()).hasSize(expectedNewFileSize());
                headerFileFound = true;
            }
        }
        assertTrue(headerFileFound);
    }

    protected long expectedNewFileSize() {
        return LATEST_LOG_FORMAT.getHeaderSize();
    }

    protected boolean preallocateLogs() {
        return false;
    }
}
