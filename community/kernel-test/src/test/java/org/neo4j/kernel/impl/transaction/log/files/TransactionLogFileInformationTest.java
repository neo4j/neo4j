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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;

class TransactionLogFileInformationTest {
    private final LogFiles logFiles = mock(TransactionLogFiles.class);
    private final LogFile logFile = mock(TransactionLogFile.class);
    private final LogHeaderCache logHeaderCache = mock(LogHeaderCache.class);
    private final TransactionLogFilesContext context = mock(TransactionLogFilesContext.class);
    private final StoreId storeId = new StoreId(1, 1, "engine-1", "format-1", 1, 1);

    @BeforeEach
    void setUp() {
        when(logFiles.getLogFile()).thenReturn(logFile);
    }

    @Test
    void shouldReadAndCacheFirstCommittedTransactionIdForAGivenVersionWhenNotCached() throws Exception {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);
        long expected = 5;

        long version = 10L;
        when(logHeaderCache.getLogHeader(version)).thenReturn(null);
        when(logFiles.getLogFile().versionExists(version)).thenReturn(true);
        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                expected - 1L,
                expected + 1L,
                storeId,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION);
        when(logFiles.getLogFile().extractHeader(version)).thenReturn(expectedHeader);

        long firstCommittedTxId = info.getFirstEntryId(version);
        assertEquals(expected, firstCommittedTxId);
        verify(logHeaderCache).putHeader(version, expectedHeader);
    }

    @Test
    void fileWithoutHeaderDoesNotHaveFirstEntry() throws IOException {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);

        int version = 42;
        when(logFiles.getLogFile().versionExists(version)).thenReturn(true);
        when(logFiles.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.getFirstEntryId(version));
        verify(logHeaderCache, never()).putHeader(eq(version), any());
    }

    @Test
    void firstStartRecordTimestampForFileWithoutHeader() throws IOException {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);

        int version = 42;
        when(logFiles.getLogFile().versionExists(version)).thenReturn(true);
        when(logFiles.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.getFirstStartRecordTimestamp(42));
    }

    @Test
    void shouldReadFirstCommittedTransactionIdForAGivenVersionWhenCached() throws Exception {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);
        long expected = 5;

        long version = 10L;
        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                expected - 1L,
                expected + 1L,
                storeId,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION);
        when(logHeaderCache.getLogHeader(version)).thenReturn(expectedHeader);

        long firstCommittedTxId = info.getFirstEntryId(version);
        assertEquals(expected, firstCommittedTxId);
    }

    @Test
    void shouldReadAndCacheFirstCommittedTransactionIdWhenNotCached() throws Exception {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);
        long expected = 5;

        long version = 10L;
        when(logFile.getHighestLogVersion()).thenReturn(version);
        when(logHeaderCache.getLogHeader(version)).thenReturn(null);
        when(logFile.versionExists(version)).thenReturn(true);
        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                expected - 1L,
                expected + 1L,
                storeId,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION);
        when(logFile.extractHeader(version)).thenReturn(expectedHeader);
        when(logFile.hasAnyEntries(version)).thenReturn(true);

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals(expected, firstCommittedTxId);
        verify(logHeaderCache).putHeader(version, expectedHeader);
    }

    @Test
    void shouldReadFirstCommittedTransactionIdWhenCached() throws Exception {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);
        long expected = 5;

        long version = 10L;
        when(logFile.getHighestLogVersion()).thenReturn(version);
        when(logFile.versionExists(version)).thenReturn(true);

        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                expected - 1L,
                expected + 1L,
                storeId,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION);
        when(logHeaderCache.getLogHeader(version)).thenReturn(expectedHeader);
        when(logFile.hasAnyEntries(version)).thenReturn(true);

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals(expected, firstCommittedTxId);
    }

    @Test
    void shouldReturnNothingWhenThereAreNoTransactions() throws Exception {
        TransactionLogFileInformation info = new TransactionLogFileInformation(logFiles, logHeaderCache, context);

        long version = 10L;
        when(logFile.getHighestLogVersion()).thenReturn(version);
        when(logFile.hasAnyEntries(version)).thenReturn(false);

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals(-1, firstCommittedTxId);
    }

    @Test
    void extractLogFileTimeFromChunkStartEntry() throws IOException {
        var logEntryReader = mock(LogEntryReader.class);
        var readableLogChannel = mock(ReadableLogChannel.class);
        when(logEntryReader.readLogEntry(readableLogChannel))
                .thenReturn(
                        new LogEntryChunkStart(LatestVersions.LATEST_KERNEL_VERSION, 42, 1, LogPosition.UNSPECIFIED));
        var fileInfo = new TransactionLogFileInformation(logFiles, logHeaderCache, context, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2, 3, 4, storeId, UNKNOWN_LOG_SEGMENT_SIZE, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION);
        when(logFile.extractHeader(anyLong())).thenReturn(expectedHeader);
        when(logFile.getRawReader(any())).thenReturn(readableLogChannel);
        when(logFile.versionExists(anyLong())).thenReturn(true);

        assertEquals(42, fileInfo.getFirstStartRecordTimestamp(1));
        assertEquals(42, fileInfo.getFirstStartRecordTimestamp(1));
        assertEquals(42, fileInfo.getFirstStartRecordTimestamp(1));

        verify(logFile, times(1)).getRawReader(any());
    }

    @Test
    void doNotReadAgainPreviouslyObservedLogTransactionTime() throws IOException {
        var logEntryReader = mock(LogEntryReader.class);
        var readableLogChannel = mock(ReadableLogChannel.class);
        when(logEntryReader.readLogEntry(readableLogChannel))
                .thenReturn(newStartEntry(
                        LatestVersions.LATEST_KERNEL_VERSION, 1, 1, 1, 1, new byte[] {}, LogPosition.UNSPECIFIED));
        var fileInfo = new TransactionLogFileInformation(logFiles, logHeaderCache, context, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2, 3, 4, storeId, UNKNOWN_LOG_SEGMENT_SIZE, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION);
        when(logFile.extractHeader(anyLong())).thenReturn(expectedHeader);
        when(logFile.getRawReader(any())).thenReturn(readableLogChannel);
        when(logFile.versionExists(anyLong())).thenReturn(true);

        fileInfo.getFirstStartRecordTimestamp(1);
        fileInfo.getFirstStartRecordTimestamp(1);
        fileInfo.getFirstStartRecordTimestamp(1);
        fileInfo.getFirstStartRecordTimestamp(1);
        fileInfo.getFirstStartRecordTimestamp(1);

        verify(logFile, times(1)).getRawReader(any());
    }

    @Test
    void doNotFailRecordTimestampIfVersionDoesNotExist() throws IOException {
        long version = 321;
        when(logFile.versionExists(version)).thenReturn(false);

        var fileInfo = new TransactionLogFileInformation(logFiles, logHeaderCache, context);

        assertEquals(-1, fileInfo.getFirstStartRecordTimestamp(version));
    }
}
