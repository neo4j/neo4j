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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.IOUtils.uncheckedLongConsumer;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogRangeInfo;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;

class ThresholdBasedPruneConstraintTest {
    private final FileSystemAbstraction fileSystem = mock(FileSystemAbstraction.class);
    private final LogFile logFile = mock(TransactionLogFile.class);
    private final TransactionLogFileInformation logFileInformation = mock(TransactionLogFileInformation.class);

    @BeforeEach
    void setUp() {
        when(logFile.getLogFileForVersion(anyLong())).thenAnswer(invocationOnMock -> {
            long version = invocationOnMock.getArgument(0, Long.class);
            return logFileForVersion(version);
        });
        when(logFileInformation.forVersion(anyLong())).thenAnswer(inv -> {
            long version = inv.getArgument(0, Long.class);
            return new StubLogFileInformation(version);
        });
    }

    @Test
    void shouldNotDeleteAnythingIfThresholdDoesNotAllow() throws IOException {
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(0L, null, 6L, null));

        LogPruneThreshold threshold = state -> current -> false;
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);

        strategy.findLogVersionsToDelete(7L)
                .forEachOrdered(uncheckedLongConsumer(v -> fileSystem.deleteFile(logFile.getLogFileForVersion(v))));

        verify(fileSystem, never()).deleteFile(any(Path.class));
    }

    @Test
    void shouldDeleteJustWhatTheThresholdSays() throws IOException {
        Path fileName1 = logFileForVersion(1);
        Path fileName2 = logFileForVersion(2);
        Path fileName3 = logFileForVersion(3);
        Path fileName4 = logFileForVersion(4);
        Path fileName5 = logFileForVersion(5);
        Path fileName6 = logFileForVersion(6);

        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(1L, null, 6L, null));

        LogPruneThreshold threshold = state -> current -> current.version() <= 3L;
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);

        strategy.findLogVersionsToDelete(7L)
                .forEachOrdered(uncheckedLongConsumer(v -> fileSystem.deleteFile(logFile.getLogFileForVersion(v))));

        verify(fileSystem).deleteFile(fileName1);
        verify(fileSystem).deleteFile(fileName2);
        verify(fileSystem, never()).deleteFile(fileName3);
        verify(fileSystem, never()).deleteFile(fileName4);
        verify(fileSystem, never()).deleteFile(fileName5);
        verify(fileSystem, never()).deleteFile(fileName6);
    }

    @Test
    void minimalAvailableVersionHigherThanRequested() {
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(10L, null, 10L, null));
        LogPruneThreshold threshold = state -> current -> true;

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);

        var anyFound = new MutableBoolean();
        strategy.findLogVersionsToDelete(5).forEachOrdered(value -> anyFound.setTrue());
        assertFalse(anyFound.getValue());
    }

    @Test
    void rangeWithMissingFilesCanBeProduced() {
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(10L, null, 20L, null));
        when(fileSystem.fileExists(any(Path.class))).thenReturn(false);

        LogPruneThreshold threshold = state -> current -> true;
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);

        var versionsToDelete = strategy.findLogVersionsToDelete(15);
        assertThat(versionsToDelete.fromInclusive()).isEqualTo(10);
        assertThat(versionsToDelete.toExclusive()).isEqualTo(15);
    }

    @Test
    void mustHaveToStringOfThreshold() {
        LogPruneThreshold threshold = new LogPruneThreshold() {
            @Override
            public PrunePredicate forCycle(long lastEntryAppendIndex) {
                return current -> false;
            }

            @Override
            public String toString() {
                return "Super-duper threshold";
            }
        };
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);
        assertEquals("Super-duper threshold", strategy.toString());
    }

    @Test
    void shouldHandleSizeThresholdForMissingFile() throws IOException {
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(0L, null, 3L, null));
        when(fileSystem.getFileSize(logFileForVersion(0)))
                .thenThrow(new NoSuchFileException(logFileForVersion(0).toString()));
        when(fileSystem.getFileSize(logFileForVersion(1))).thenReturn(25L);
        when(fileSystem.getFileSize(logFileForVersion(2))).thenReturn(10L);
        when(fileSystem.getFileSize(logFileForVersion(3))).thenReturn(20L);

        try (var logProvider = new AssertableLogProvider()) {
            var threshold = new FileSizeThreshold(fileSystem, 100, logProvider);
            var strategy = new ThresholdBasedPruneStrategy(logFile, threshold, logFileInformation);

            var versionRange = strategy.findLogVersionsToDelete(3);

            assertThat(versionRange.fromInclusive()).isEqualTo(-1);
            assertThat(versionRange.toExclusive()).isEqualTo(-1);
            LogAssertions.assertThat(logProvider).containsMessages("Error on attempt to get file size");
        }
    }

    private static Path logFileForVersion(long version) {
        return Path.of("logical-log.v" + version);
    }

    private record StubLogFileInformation(long version) implements LogFileInformation {
        @Override
        public Path path() {
            return logFileForVersion(version);
        }

        @Override
        public long getPreviousAppendIndexFromHeader() {
            return -1;
        }

        @Override
        public long getFirstStartRecordTimestamp() {
            return -1;
        }
    }
}
