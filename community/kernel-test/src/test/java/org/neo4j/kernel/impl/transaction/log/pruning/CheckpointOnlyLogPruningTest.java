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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.logging.NullLogProvider;

class CheckpointOnlyLogPruningTest {

    private final FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
    private final LogFiles logFiles = mock(LogFiles.class);
    private final Config config = Config.defaults();

    @BeforeEach
    void setUp() {
        when(logFiles.getCheckpointFile()).thenReturn(mock(CheckpointFile.class));
    }

    @Test
    void shouldNeverIndicateLogsToPrune() {
        var pruning =
                new CheckpointOnlyLogPruning(fs, logFiles, NullLogProvider.getInstance(), config, new ReentrantLock());
        assertThat(pruning.mightHaveLogsToPrune(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void shouldDescribeItselfAsCheckpointOnly() {
        var pruning =
                new CheckpointOnlyLogPruning(fs, logFiles, NullLogProvider.getInstance(), config, new ReentrantLock());
        assertThat(pruning.describeCurrentStrategy()).contains("checkpoint-only");
    }

    @Test
    void shouldDeleteOverflowCheckpointLogFiles() throws IOException {
        int keep = config.get(checkpoint_logical_log_keep_threshold);
        CheckpointFile checkpointFile = mock(CheckpointFile.class);
        Path[] checkpointFiles = new Path[keep + 2];
        for (int i = 0; i < checkpointFiles.length; i++) {
            checkpointFiles[i] = Paths.get(String.valueOf(i));
        }
        when(checkpointFile.getMatchedFiles()).thenReturn(checkpointFiles);
        when(checkpointFile.getLogVersion(any())).thenAnswer(invocation -> {
            Path file = invocation.getArgument(0);
            return Long.parseLong(file.getFileName().toString());
        });
        when(checkpointFile.getCurrentLogVersion()).thenReturn((long) checkpointFiles.length - 1);
        when(logFiles.getCheckpointFile()).thenReturn(checkpointFile);

        var pruning =
                new CheckpointOnlyLogPruning(fs, logFiles, NullLogProvider.getInstance(), config, new ReentrantLock());
        pruning.pruneLogs(0);

        verify(fs).deleteFile(checkpointFiles[0]);
        verify(fs).deleteFile(checkpointFiles[1]);
        verify(fs, never()).deleteFile(checkpointFiles[2]);
    }

    @Test
    void shouldNotDeleteWhenWithinThreshold() throws IOException {
        int keep = config.get(checkpoint_logical_log_keep_threshold);
        CheckpointFile checkpointFile = mock(CheckpointFile.class);
        Path[] checkpointFiles = new Path[keep];
        for (int i = 0; i < checkpointFiles.length; i++) {
            checkpointFiles[i] = Paths.get(String.valueOf(i));
        }
        when(checkpointFile.getMatchedFiles()).thenReturn(checkpointFiles);
        when(checkpointFile.getCurrentLogVersion()).thenReturn((long) checkpointFiles.length - 1);
        when(logFiles.getCheckpointFile()).thenReturn(checkpointFile);

        var pruning =
                new CheckpointOnlyLogPruning(fs, logFiles, NullLogProvider.getInstance(), config, new ReentrantLock());
        pruning.pruneLogs(0);

        verify(fs, never()).deleteFile(any());
    }
}
