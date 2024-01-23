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
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class CheckpointLogFileRotationIT {
    static final long CONFIG_ROTATION_THRESHOLD = kibiBytes(1);
    // Checkpoint log files should always have space for at least 2 envelop segments, so even if the
    // threshold passed to the DBMS is very small we should be seeing bigger files.
    static final long ACTUAL_ROTATION_THRESHOLD = LogSegments.DEFAULT_LOG_SEGMENT_SIZE * 2L;

    static final String CHECKPOINT_REASON = "checkpoint for rotation test";
    static final LogPosition LOG_POSITION = new LogPosition(1000, 12345);
    static final TransactionId TRANSACTION_ID = new TransactionId(100, 101, 102, 103);

    @Inject
    LogFiles logFiles;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(checkpoint_logical_log_rotation_threshold, CONFIG_ROTATION_THRESHOLD)
                .setConfig(checkpoint_logical_log_keep_threshold, 100)
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, preallocateLogs());
    }

    @Test
    void rotateCheckpointLogFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        fillWithCheckpoints(5, checkpointAppender);
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(6); // 5 filled + 1 new rotation with only headers
        for (var fileWithCheckpoints : matchedFiles) {
            assertThat(fileWithCheckpoints).satisfies(new Condition<>() {
                @Override
                public boolean matches(Path file) {
                    long length = file.toFile().length();
                    return length < ACTUAL_ROTATION_THRESHOLD + RECORD_LENGTH_BYTES;
                }
            });
        }
    }

    @Test
    void doNotRotateWhileCheckpointsAreFitting() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        for (int i = LATEST_LOG_FORMAT.getHeaderSize() + 2 * RECORD_LENGTH_BYTES;
                i < ACTUAL_ROTATION_THRESHOLD;
                i += RECORD_LENGTH_BYTES) {
            checkpointAppender.checkPoint(
                    NULL,
                    TRANSACTION_ID,
                    LatestVersions.LATEST_KERNEL_VERSION,
                    LOG_POSITION,
                    Instant.now(),
                    CHECKPOINT_REASON);
        }
        assertThat(checkpointFile.getDetachedCheckpointFiles()).hasSize(1);
    }

    @Test
    void afterRotationNewFileHaveHeader() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        fillWithCheckpoints(1, checkpointAppender);
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

    /**
     * This method create enough checkpoint entries to fill {@param files} checkpoint files.
     */
    private static void fillWithCheckpoints(int files, CheckpointAppender appender) throws IOException {
        for (int fileCount = 0; fileCount < files; fileCount++) {
            // the first file already contains one initial checkpoint
            int startOffset = LATEST_LOG_FORMAT.getHeaderSize() + (fileCount == 0 ? RECORD_LENGTH_BYTES : 0);
            for (int i = startOffset; i < ACTUAL_ROTATION_THRESHOLD; i += RECORD_LENGTH_BYTES) {
                appender.checkPoint(
                        NULL,
                        TRANSACTION_ID,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        LOG_POSITION,
                        Instant.now(),
                        CHECKPOINT_REASON);
            }
        }
    }

    protected long expectedNewFileSize() {
        return LATEST_LOG_FORMAT.getHeaderSize();
    }

    protected boolean preallocateLogs() {
        return false;
    }
}
