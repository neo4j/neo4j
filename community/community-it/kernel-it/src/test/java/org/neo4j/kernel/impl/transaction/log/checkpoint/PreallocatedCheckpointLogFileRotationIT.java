/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.DetachedCheckpointLogEntryWriter.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.TransactionId;

@EnabledOnOs(OS.LINUX)
class PreallocatedCheckpointLogFileRotationIT extends CheckpointLogFileRotationIT {
    @Override
    protected boolean preallocateLogs() {
        return true;
    }

    @Override
    protected long expectedNewFileSize() {
        return ROTATION_THRESHOLD;
    }

    @Test
    void writeCheckpointsIntoPreallocatedFile() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition(1000, 12345);
        var transactionId = new TransactionId(100, 101, 102);
        var reason = "checkpoints in preallocated file";
        for (int i = 0; i < 3; i++) {
            checkpointAppender.checkPoint(NULL, transactionId, logPosition, Instant.now(), reason);
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(1);
    }

    @Test
    void writeCheckpointsIntoSeveralPreallocatedFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition(1000, 12345);
        var transactionId = new TransactionId(100, 101, 102);
        var reason = "checkpoint in preallocated file";

        for (int fileCount = 1; fileCount < 6; fileCount++) {
            for (int i = CURRENT_FORMAT_LOG_HEADER_SIZE; i < ROTATION_THRESHOLD; i += RECORD_LENGTH_BYTES) {
                assertThat(checkpointFile.getDetachedCheckpointFiles())
                        .hasSize(fileCount)
                        .allMatch(this::sizeEqualsToPreallocatedFile);
                checkpointAppender.checkPoint(NULL, transactionId, logPosition, Instant.now(), reason);
            }
        }

        assertThat(checkpointFile.getDetachedCheckpointFiles()).hasSize(6).allMatch(this::sizeEqualsToPreallocatedFile);
    }

    private boolean sizeEqualsToPreallocatedFile(Path path) {
        try {
            return Files.size(path) < ROTATION_THRESHOLD + RECORD_LENGTH_BYTES;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
