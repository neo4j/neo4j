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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.IOUtils.uncheckedLongConsumer;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;

class ThresholdBasedPruneStrategyTest {
    private final FileSystemAbstraction fileSystem = mock(FileSystemAbstraction.class);
    private final LogFile logFile = mock(TransactionLogFile.class);
    private final Threshold threshold = mock(Threshold.class);

    @BeforeEach
    void setUp() {
        when(logFile.getLogFileForVersion(anyLong())).thenAnswer(invocationOnMock -> {
            long version = invocationOnMock.getArgument(0, Long.class);
            return logFileForVersion(version);
        });
    }

    @Test
    void shouldNotDeleteAnythingIfThresholdDoesNotAllow() throws IOException {
        // Given
        Path fileName0 = logFileForVersion(0);
        Path fileName1 = logFileForVersion(1);
        Path fileName2 = logFileForVersion(2);
        Path fileName3 = logFileForVersion(3);
        Path fileName4 = logFileForVersion(4);
        Path fileName5 = logFileForVersion(5);
        Path fileName6 = logFileForVersion(6);

        when(logFile.getLowestLogVersion()).thenReturn(0L);

        when(fileSystem.fileExists(fileName6)).thenReturn(true);
        when(fileSystem.fileExists(fileName5)).thenReturn(true);
        when(fileSystem.fileExists(fileName4)).thenReturn(true);
        when(fileSystem.fileExists(fileName3)).thenReturn(true);
        when(fileSystem.fileExists(fileName2)).thenReturn(true);
        when(fileSystem.fileExists(fileName1)).thenReturn(true);
        when(fileSystem.fileExists(fileName0)).thenReturn(true);

        when(fileSystem.getFileSize(any(Path.class))).thenReturn(LATEST_LOG_FORMAT.getHeaderSize() + 1L);

        when(threshold.reached(any(), anyLong(), any())).thenReturn(false);

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold);

        // When
        var versionsToDelete = strategy.findLogVersionsToDelete(7L);
        versionsToDelete.forEachOrdered(
                uncheckedLongConsumer(v -> fileSystem.deleteFile(logFile.getLogFileForVersion(v))));

        // Then
        verify(threshold).init();
        verify(fileSystem, never()).deleteFile(any(Path.class));
    }

    @Test
    void shouldDeleteJustWhatTheThresholdSays() throws IOException {
        // Given
        when(threshold.reached(any(), eq(6L), any())).thenReturn(false);
        when(threshold.reached(any(), eq(5L), any())).thenReturn(false);
        when(threshold.reached(any(), eq(4L), any())).thenReturn(false);
        when(threshold.reached(any(), eq(3L), any())).thenReturn(true);

        Path fileName1 = logFileForVersion(1);
        Path fileName2 = logFileForVersion(2);
        Path fileName3 = logFileForVersion(3);
        Path fileName4 = logFileForVersion(4);
        Path fileName5 = logFileForVersion(5);
        Path fileName6 = logFileForVersion(6);

        when(logFile.getLowestLogVersion()).thenReturn(1L);

        when(fileSystem.getFileSize(any(Path.class))).thenReturn(LATEST_LOG_FORMAT.getHeaderSize() + 1L);

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold);

        // When
        var versionsToDelete = strategy.findLogVersionsToDelete(7L);
        versionsToDelete.forEachOrdered(
                uncheckedLongConsumer(v -> fileSystem.deleteFile(logFile.getLogFileForVersion(v))));

        // Then
        verify(threshold).init();
        verify(fileSystem).deleteFile(fileName1);
        verify(fileSystem).deleteFile(fileName2);
        verify(fileSystem, never()).deleteFile(fileName3);
        verify(fileSystem, never()).deleteFile(fileName4);
        verify(fileSystem, never()).deleteFile(fileName5);
        verify(fileSystem, never()).deleteFile(fileName6);
    }

    @Test
    void minimalAvailableVersionHigherThanRequested() {
        when(logFile.getLowestLogVersion()).thenReturn(10L);
        when(threshold.reached(any(), anyLong(), any())).thenReturn(true);

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold);

        var versionsToDelete = strategy.findLogVersionsToDelete(5);
        var anyFound = new MutableBoolean();
        versionsToDelete.forEachOrdered(value -> anyFound.setTrue());
        assertFalse(anyFound.getValue());
    }

    @Test
    void rangeWithMissingFilesCanBeProduced() {
        when(logFile.getLowestLogVersion()).thenReturn(10L);
        when(threshold.reached(any(), anyLong(), any())).thenReturn(true);
        when(fileSystem.fileExists(any(Path.class))).thenReturn(false);

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold);

        var versionsToDelete = strategy.findLogVersionsToDelete(15);
        assertThat(versionsToDelete.fromInclusive()).isEqualTo(10);
        assertThat(versionsToDelete.toExclusive()).isEqualTo(15);
    }

    @Test
    void mustHaveToStringOfThreshold() {
        Threshold threshold = new Threshold() {
            @Override
            public void init() {}

            @Override
            public boolean reached(Path file, long version, LogFileInformation source) {
                return false;
            }

            @Override
            public String toString() {
                return "Super-duper threshold";
            }
        };
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(logFile, threshold);
        assertEquals("Super-duper threshold", strategy.toString());
    }

    @Test
    void shouldHandleSizeThresholdForMissingFile() throws IOException {
        // given
        when(logFile.getLowestLogVersion()).thenReturn(0L);
        when(fileSystem.getFileSize(logFileForVersion(0)))
                .thenThrow(new NoSuchFileException(logFileForVersion(0).toString()));
        setUpFileSizeForLogVersion(1, 25);
        setUpFileSizeForLogVersion(2, 10);
        setUpFileSizeForLogVersion(3, 20);
        try (var logProvider = new AssertableLogProvider()) {
            var threshold = new FileSizeThreshold(fileSystem, 100, logProvider);
            var strategy = new ThresholdBasedPruneStrategy(logFile, threshold);

            // when
            var versionRange = strategy.findLogVersionsToDelete(3);

            // then it didn't reach the limit...
            assertThat(versionRange.fromInclusive()).isEqualTo(-1);
            assertThat(versionRange.toExclusive()).isEqualTo(-1);
            // ... however it didn't fail when reaching v0, merely logged a warning
            LogAssertions.assertThat(logProvider).containsMessages("Error on attempt to get file size");
        }
    }

    private void setUpFileSizeForLogVersion(long version, long size) throws IOException {
        when(fileSystem.getFileSize(logFileForVersion(version))).thenReturn(size);
    }

    private Path logFileForVersion(long version) {
        return Path.of("logical-log.v" + version);
    }
}
