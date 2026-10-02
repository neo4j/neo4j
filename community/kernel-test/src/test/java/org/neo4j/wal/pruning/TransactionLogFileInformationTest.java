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
package org.neo4j.wal.pruning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.wal.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.wal.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.wal.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.LatestVersions;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogEntryReader;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.entry.v520.LogEntryChunkStart;
import org.neo4j.wal.files.TransactionLogFile;
import org.neo4j.wal.files.TransactionLogFiles;

class TransactionLogFileInformationTest {
    private final WALService walService = mock(TransactionLogFiles.class);
    private final LogFile logFile = mock(TransactionLogFile.class);
    private final StoreId storeId = new StoreId(1, 1, "engine-1", "format-1", 1, 1);
    private final CommandReaderFactory commandReaderFactory = mock(CommandReaderFactory.class);

    @BeforeEach
    void setUp() {
        when(walService.getLogFile()).thenReturn(logFile);
    }

    @Test
    void fileWithoutHeaderDoesNotHaveFirstEntry() throws IOException {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(walService, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        int version = 42;
        when(walService.getLogFile().versionExists(version)).thenReturn(true);
        when(walService.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.forVersion(version).getPreviousAppendIndexFromHeader());
    }

    @Test
    void firstStartRecordTimestampForFileWithoutHeader() throws IOException {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(walService, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        int version = 42;
        when(walService.getLogFile().versionExists(version)).thenReturn(true);
        when(walService.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.forVersion(42).getFirstStartRecordTimestamp());
    }

    @Test
    void shouldReadFirstCommittedTransactionIdForAGivenVersion() throws Exception {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(walService, commandReaderFactory, BINARY_VERSIONS, INSTANCE);
        long baseId = 5;
        long expectedAppendIndex = baseId + 2;

        long version = 10L;
        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                baseId + 1L,
                ReadableChannel.BASE_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME,
                UNKNOWN_TX_ID);
        when(logFile.extractHeader(version)).thenReturn(expectedHeader);

        long lastAppendIndexBeforeFile = info.forVersion(version).getPreviousAppendIndexFromHeader();
        assertEquals(expectedAppendIndex - 1, lastAppendIndexBeforeFile);
    }

    @Test
    void extractLogFileTimeFromChunkStartEntry() throws IOException {
        var logEntryReader = mock(LogEntryReader.class);
        var readableLogChannel = mock(ReadableLogChannel.class);
        when(logEntryReader.readLogEntry(readableLogChannel))
                .thenReturn(new LogEntryChunkStart(
                        LatestVersions.LATEST_KERNEL_VERSION,
                        42,
                        1,
                        UNKNOWN_APPEND_INDEX,
                        UNKNOWN_APPEND_INDEX,
                        UNKNOWN_TX_SEQUENCE_NUMBER,
                        encodeLogIndex(42)));
        var fileInfo = new TransactionLogFileInformation(walService, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                4,
                ReadableChannel.BASE_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME,
                UNKNOWN_TX_ID);
        when(logFile.extractHeader(anyLong())).thenReturn(expectedHeader);
        when(logFile.getRawReader(any())).thenReturn(readableLogChannel);
        when(logFile.versionExists(anyLong())).thenReturn(true);

        assertEquals(42, fileInfo.forVersion(1).getFirstStartRecordTimestamp());
        assertEquals(42, fileInfo.forVersion(1).getFirstStartRecordTimestamp());
        assertEquals(42, fileInfo.forVersion(1).getFirstStartRecordTimestamp());

        verify(logFile, times(1)).getRawReader(any());
    }

    @Test
    void doNotReadAgainPreviouslyObservedLogTransactionTime() throws IOException {
        var logEntryReader = mock(LogEntryReader.class);
        var readableLogChannel = mock(ReadableLogChannel.class);
        when(logEntryReader.readLogEntry(readableLogChannel))
                .thenReturn(newStartEntry(
                        LatestVersions.LATEST_KERNEL_VERSION, 1, 1, 1, UNKNOWN_TX_SEQUENCE_NUMBER, 1, new byte[] {}));
        var fileInfo = new TransactionLogFileInformation(walService, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                4,
                ReadableChannel.BASE_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME,
                UNKNOWN_TX_ID);
        when(logFile.extractHeader(anyLong())).thenReturn(expectedHeader);
        when(logFile.getRawReader(any())).thenReturn(readableLogChannel);
        when(logFile.versionExists(anyLong())).thenReturn(true);

        fileInfo.forVersion(1).getFirstStartRecordTimestamp();
        fileInfo.forVersion(1).getFirstStartRecordTimestamp();
        fileInfo.forVersion(1).getFirstStartRecordTimestamp();
        fileInfo.forVersion(1).getFirstStartRecordTimestamp();
        fileInfo.forVersion(1).getFirstStartRecordTimestamp();

        verify(logFile, times(1)).getRawReader(any());
    }

    @Test
    void doNotFailRecordTimestampIfVersionDoesNotExist() throws IOException {
        long version = 321;
        when(logFile.versionExists(version)).thenReturn(false);

        var fileInfo = new TransactionLogFileInformation(walService, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        assertEquals(-1, fileInfo.forVersion(version).getFirstStartRecordTimestamp());
    }
}
