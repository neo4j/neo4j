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
package org.neo4j.kernel.recovery;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.MISSING_LOGS;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;
import static org.neo4j.wal.LastAppendBatchInfoProvider.EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER;
import static org.neo4j.wal.LogTermProvider.UNKNOWN_TERM_PROVIDER;
import static org.neo4j.wal.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider.Monitor;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.wal.CheckpointInfo;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.files.LogRangeInfo;
import org.neo4j.wal.files.LogTailInformation;

class RecoveryStartInformationProviderTest {
    private static final long NO_APPEND_INDEX = -1;
    private final long currentLogVersion = 2L;
    private final WALService walService = mock(WALService.class);
    private final LogFile logFile = mock(LogFile.class);
    private final Monitor monitor = mock(Monitor.class);
    private final KernelVersionProvider kernelProv = mock(KernelVersionProvider.class);

    @BeforeEach
    void setUp() throws IOException {
        var logHeader = LATEST_LOG_FORMAT.newHeader(
                0,
                1,
                ReadableChannel.BASE_TERM,
                null,
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME);
        when(logFile.extractHeader(0)).thenReturn(logHeader);
        when(walService.getLogFile()).thenReturn(logFile);
    }

    @Test
    void shouldReturnUnspecifiedIfThereIsNoNeedForRecovery() {
        // given
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        false,
                        NO_APPEND_INDEX,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER,
                        null));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults())).get();

        // then
        verify(monitor).recoveryNotRequired(null);
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.transactionLogPosition());
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.getCheckpointPosition());
        assertEquals(NO_APPEND_INDEX, recoveryStartInformation.firstAppendIndexAfterLastCheckPoint());
        assertFalse(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void shouldReturnLogPositionToRecoverFromIfNeeded() {
        // given
        LogPosition txPosition = new LogPosition(1L, 4242);
        LogPosition checkpointPosition = new LogPosition(2, 4);
        LogPosition afterCheckpointPosition = new LogPosition(4, 8);
        LogPosition readerPostPosition = new LogPosition(5, 9);
        TransactionId transactionId = new TransactionId(4L, 7L, LATEST_KERNEL_VERSION, 2, 5L, 6L);
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        new CheckpointInfo(
                                txPosition,
                                txPosition,
                                null,
                                checkpointPosition,
                                afterCheckpointPosition,
                                readerPostPosition,
                                LatestVersions.LATEST_KERNEL_VERSION,
                                LatestVersions.LATEST_KERNEL_VERSION.version(),
                                transactionId,
                                transactionId.id() + 8,
                                "test"),
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        null,
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults())).get();

        // then
        verify(monitor).recoveryRequiredAfterLastCheckPoint(txPosition, txPosition, 10L);
        assertEquals(txPosition, recoveryStartInformation.transactionLogPosition());
        assertEquals(checkpointPosition, recoveryStartInformation.getCheckpointPosition());
        assertEquals(10L, recoveryStartInformation.firstAppendIndexAfterLastCheckPoint());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void propagateOldestNotVisibleTransactionPositionFromLogTailToRecoveryStartInfo() {
        LogPosition oldestNotVisibleTransactionPosition = new LogPosition(1L, 4242);
        LogPosition txPosition = new LogPosition(2L, 4242);
        LogPosition checkpointPosition = new LogPosition(2, 4);
        LogPosition afterCheckpointPosition = new LogPosition(4, 8);
        LogPosition readerPostPosition = new LogPosition(5, 9);
        TransactionId transactionId = new TransactionId(4L, 7L, LATEST_KERNEL_VERSION, 2, 5L, 6L);
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        new CheckpointInfo(
                                oldestNotVisibleTransactionPosition,
                                txPosition,
                                null,
                                checkpointPosition,
                                afterCheckpointPosition,
                                readerPostPosition,
                                LatestVersions.LATEST_KERNEL_VERSION,
                                LatestVersions.LATEST_KERNEL_VERSION.version(),
                                transactionId,
                                transactionId.id() + 8,
                                "test"),
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        null,
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER));

        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults())).get();

        verify(monitor).recoveryRequiredAfterLastCheckPoint(txPosition, oldestNotVisibleTransactionPosition, 10L);
        assertEquals(txPosition, recoveryStartInformation.transactionLogPosition());
        assertEquals(
                oldestNotVisibleTransactionPosition, recoveryStartInformation.oldestNotVisibleTransactionLogPosition());
        assertEquals(checkpointPosition, recoveryStartInformation.getCheckpointPosition());
        assertEquals(10L, recoveryStartInformation.firstAppendIndexAfterLastCheckPoint());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void shouldRecoverFromStartOfLogZeroIfThereAreNoCheckPointAndOldestLogIsVersionZero() {
        // given
        KernelVersion kernelVersion = LatestVersions.LATEST_KERNEL_VERSION;
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        kernelVersion.version(),
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER,
                        null));
        when(walService.getLogFile().getLogRangeInfo()).thenReturn(new LogRangeInfo(0, null, 0, null));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults())).get();

        // then
        verify(monitor).noCheckPointFound();
        var startOffset = LATEST_LOG_FORMAT.getDefaultDataStartByteOffset();
        assertEquals(new LogPosition(0, startOffset), recoveryStartInformation.transactionLogPosition());
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.getCheckpointPosition());
        assertEquals(10L, recoveryStartInformation.firstAppendIndexAfterLastCheckPoint());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void detectMissingTransactionLogsInformation() {
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        false,
                        -1,
                        true,
                        -1,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER,
                        null));

        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults())).get();

        assertSame(MISSING_LOGS, recoveryStartInformation);
    }

    @Test
    void shouldFailIfThereAreNoCheckPointsAndOldestLogVersionInNotZero() {
        // given
        long oldestLogVersionFound = 1L;
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(oldestLogVersionFound, null, 100, null));
        when(walService.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        kernelProv,
                        LATEST_LOG_FORMAT_PROVIDER,
                        UNKNOWN_TERM_PROVIDER,
                        EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER,
                        null));

        // when
        final String expectedMessage = "No check point found in any log file and transaction log "
                + "files do not exist from expected version 0. Lowest found log file is 1.";
        RecoveryStartInformationProvider provider =
                new RecoveryStartInformationProvider(walService, monitor, new DatabaseConfig(Config.defaults()));
        assertThatThrownBy(provider::get)
                .isInstanceOf(UnderlyingStorageException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    void shouldRecoverMergedLogStartingAboveVersionZeroWhenLowestHeaderShowsCompleteHistory() throws IOException {
        // given: raft bootstrap advances a merged log's start version without appending anything before it,
        // so the lowest header records no prior append index
        var header = mergedLogHeader(1, 0);
        when(logFile.extractHeader(1)).thenReturn(header);
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(1, null, 100, null));
        when(walService.getTailMetadata()).thenReturn(recoveryRequiredTailWithoutCheckpoint());

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, mergedLogConfig()).get();

        // then
        verify(monitor).noCheckPointFound();
        assertEquals(header.getStartPosition(), recoveryStartInformation.transactionLogPosition());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void shouldFailMergedLogRecoveryWhenLowestHeaderShowsPrunedHistory() throws IOException {
        // given: the lowest header records a real prior append index, meaning earlier files were pruned;
        // recovering without a checkpoint would silently start mid-history
        when(logFile.extractHeader(1)).thenReturn(mergedLogHeader(1, 5));
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(1, null, 100, null));
        when(walService.getTailMetadata()).thenReturn(recoveryRequiredTailWithoutCheckpoint());

        RecoveryStartInformationProvider provider =
                new RecoveryStartInformationProvider(walService, monitor, mergedLogConfig());
        assertThatThrownBy(provider::get)
                .isInstanceOf(UnderlyingStorageException.class)
                .hasMessageContaining("Lowest found log file is 1");
    }

    @Test
    void shouldRecoverMergedLogFromAnyStartVersionWhenLowestHeaderShowsCompleteHistory() throws IOException {
        var header = mergedLogHeader(5, 0);
        when(logFile.extractHeader(5)).thenReturn(header);
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(5, null, 100, null));
        when(walService.getTailMetadata()).thenReturn(recoveryRequiredTailWithoutCheckpoint());

        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(walService, monitor, mergedLogConfig()).get();

        verify(monitor).noCheckPointFound();
        assertEquals(header.getStartPosition(), recoveryStartInformation.transactionLogPosition());
    }

    private static LogHeader mergedLogHeader(long logVersion, long lastAppendIndex) {
        return LogFormat.V11.newHeader(
                logVersion,
                lastAppendIndex,
                ReadableChannel.BASE_TERM,
                StoreIdentifier.newStoreIdentifier(12345),
                LogFormat.V11.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                KernelVersion.GLORIOUS_FUTURE,
                UNSPECIFIED_CREATION_TIME);
    }

    private LogTailInformation recoveryRequiredTailWithoutCheckpoint() {
        return new LogTailInformation(
                true,
                10L,
                false,
                currentLogVersion,
                LatestVersions.LATEST_KERNEL_VERSION.version(),
                kernelProv,
                LATEST_LOG_FORMAT_PROVIDER,
                UNKNOWN_TERM_PROVIDER,
                EMPTY_LAST_APPEND_BATCH_INFO_PROVIDER,
                null);
    }

    private static DatabaseConfig mergedLogConfig() {
        return new DatabaseConfig(Map.of(GraphDatabaseInternalSettings.merged_log, true), Config.defaults());
    }
}
