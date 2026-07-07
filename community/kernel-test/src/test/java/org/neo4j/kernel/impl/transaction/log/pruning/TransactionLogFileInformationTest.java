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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.LatestVersions;

class TransactionLogFileInformationTest {
    private final LogFiles logFiles = mock(TransactionLogFiles.class);
    private final LogFile logFile = mock(TransactionLogFile.class);
    private final StoreId storeId = new StoreId(1, 1, "engine-1", "format-1", 1, 1);
    private final CommandReaderFactory commandReaderFactory = mock(CommandReaderFactory.class);

    @BeforeEach
    void setUp() {
        when(logFiles.getLogFile()).thenReturn(logFile);
    }

    @Test
    void fileWithoutHeaderDoesNotHaveFirstEntry() throws IOException {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(logFiles, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        int version = 42;
        when(logFiles.getLogFile().versionExists(version)).thenReturn(true);
        when(logFiles.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.forVersion(version).getPreviousAppendIndexFromHeader());
    }

    @Test
    void firstStartRecordTimestampForFileWithoutHeader() throws IOException {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(logFiles, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        int version = 42;
        when(logFiles.getLogFile().versionExists(version)).thenReturn(true);
        when(logFiles.getLogFile().extractHeader(version)).thenReturn(null);

        assertEquals(-1, info.forVersion(42).getFirstStartRecordTimestamp());
    }

    @Test
    void shouldReadFirstCommittedTransactionIdForAGivenVersion() throws Exception {
        TransactionLogFileInformation info =
                new TransactionLogFileInformation(logFiles, commandReaderFactory, BINARY_VERSIONS, INSTANCE);
        long baseId = 5;
        long expectedAppendIndex = baseId + 2;

        long version = 10L;
        LogHeader expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                baseId + 1L,
                LogHeader.UNKNOWN_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME);
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
        var fileInfo = new TransactionLogFileInformation(logFiles, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                4,
                LogHeader.UNKNOWN_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME);
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
        var fileInfo = new TransactionLogFileInformation(logFiles, () -> logEntryReader);

        var expectedHeader = LATEST_LOG_FORMAT.newHeader(
                2,
                4,
                LogHeader.UNKNOWN_TERM,
                StoreIdentifier.newStoreIdentifier(storeId),
                LATEST_LOG_FORMAT.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME);
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

        var fileInfo = new TransactionLogFileInformation(logFiles, commandReaderFactory, BINARY_VERSIONS, INSTANCE);

        assertEquals(-1, fileInfo.forVersion(version).getFirstStartRecordTimestamp());
    }
}
