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
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.MISSING_LOGS;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider.Monitor;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;

class RecoveryStartInformationProviderTest {
    private static final long NO_TRANSACTION_ID = -1;
    private final long currentLogVersion = 2L;
    private final LogFiles logFiles = mock(LogFiles.class);
    private final LogFile logFile = mock(LogFile.class);
    private final Monitor monitor = mock(Monitor.class);
    private final KernelVersionProvider kernelProv = mock(KernelVersionProvider.class);

    @BeforeEach
    void setUp() throws IOException {
        var logHeader = LATEST_LOG_FORMAT.newHeader(
                0, 1, null, UNKNOWN_LOG_SEGMENT_SIZE, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION);
        when(logFile.extractHeader(0)).thenReturn(logHeader);
        when(logFiles.getLogFile()).thenReturn(logFile);
    }

    @Test
    void shouldReturnUnspecifiedIfThereIsNoNeedForRecovery() {
        // given
        when(logFiles.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        false,
                        NO_TRANSACTION_ID,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        kernelProv));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(logFiles, monitor).get();

        // then
        verify(monitor).noCommitsAfterLastCheckPoint(null);
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.getTransactionLogPosition());
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.getCheckpointPosition());
        assertEquals(NO_TRANSACTION_ID, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint());
        assertFalse(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void shouldReturnLogPositionToRecoverFromIfNeeded() {
        // given
        LogPosition txPosition = new LogPosition(1L, 4242);
        LogPosition checkpointPosition = new LogPosition(2, 4);
        LogPosition afterCheckpointPosition = new LogPosition(4, 8);
        LogPosition readerPostPosition = new LogPosition(5, 9);
        TransactionId transactionId = new TransactionId(4L, LATEST_KERNEL_VERSION, 2, 5L, 6L);
        when(logFiles.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        new CheckpointInfo(
                                txPosition,
                                null,
                                checkpointPosition,
                                afterCheckpointPosition,
                                readerPostPosition,
                                LatestVersions.LATEST_KERNEL_VERSION,
                                LatestVersions.LATEST_KERNEL_VERSION.version(),
                                transactionId,
                                "test"),
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        null,
                        kernelProv));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(logFiles, monitor).get();

        // then
        verify(monitor).logsAfterLastCheckPoint(txPosition, 10L);
        assertEquals(txPosition, recoveryStartInformation.getTransactionLogPosition());
        assertEquals(checkpointPosition, recoveryStartInformation.getCheckpointPosition());
        assertEquals(10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void shouldRecoverFromStartOfLogZeroIfThereAreNoCheckPointAndOldestLogIsVersionZero() {
        // given
        KernelVersion kernelVersion = LatestVersions.LATEST_KERNEL_VERSION;
        when(logFiles.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        true, 10L, false, currentLogVersion, kernelVersion.version(), kernelProv));

        // when
        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(logFiles, monitor).get();

        // then
        verify(monitor).noCheckPointFound();
        assertEquals(
                new LogPosition(0, LogFormat.fromKernelVersion(kernelVersion).getHeaderSize()),
                recoveryStartInformation.getTransactionLogPosition());
        assertEquals(LogPosition.UNSPECIFIED, recoveryStartInformation.getCheckpointPosition());
        assertEquals(10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint());
        assertTrue(recoveryStartInformation.isRecoveryRequired());
    }

    @Test
    void detectMissingTransactionLogsInformation() {
        when(logFiles.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        false, -1, true, -1, LatestVersions.LATEST_KERNEL_VERSION.version(), kernelProv));

        RecoveryStartInformation recoveryStartInformation =
                new RecoveryStartInformationProvider(logFiles, monitor).get();

        assertSame(MISSING_LOGS, recoveryStartInformation);
    }

    @Test
    void shouldFailIfThereAreNoCheckPointsAndOldestLogVersionInNotZero() {
        // given
        long oldestLogVersionFound = 1L;
        when(logFile.getLowestLogVersion()).thenReturn(oldestLogVersionFound);
        when(logFiles.getTailMetadata())
                .thenReturn(new LogTailInformation(
                        true,
                        10L,
                        false,
                        currentLogVersion,
                        LatestVersions.LATEST_KERNEL_VERSION.version(),
                        kernelProv));

        // when
        final String expectedMessage = "No check point found in any log file and transaction log "
                + "files do not exist from expected version 0. Lowest found log file is 1.";
        RecoveryStartInformationProvider provider = new RecoveryStartInformationProvider(logFiles, monitor);
        assertThatThrownBy(provider::get)
                .isInstanceOf(UnderlyingStorageException.class)
                .hasMessage(expectedMessage);
    }
}
