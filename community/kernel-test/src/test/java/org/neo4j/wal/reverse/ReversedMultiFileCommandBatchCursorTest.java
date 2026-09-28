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
package org.neo4j.wal.reverse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;
import static org.neo4j.wal.GivenCommandBatchCursor.exhaust;
import static org.neo4j.wal.TestLogEntryReader.logEntryReader;
import static org.neo4j.wal.reverse.ReversedMultiFileCommandBatchCursor.fromLogFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.CompleteCommandBatch;
import org.neo4j.wal.FlushableLogPositionAwareChannel;
import org.neo4j.wal.LogAppendEvent;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.TransactionLogWriter;
import org.neo4j.wal.WALService;
import org.neo4j.wal.files.LogFilesBuilder;

@Neo4jLayoutExtension
@ExtendWith({RandomExtension.class, LifeExtension.class})
class ReversedMultiFileCommandBatchCursorTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private LifeSupport life;

    @Inject
    private RandomSupport random;

    private long txId = BASE_TX_ID;
    private ReverseTransactionCursorLoggingMonitor monitor;
    private LogFile logFile;

    @BeforeEach
    void setUp() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER, LATEST_LOG_FORMAT_PROVIDER)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withConfig(Config.defaults(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(256)))
                .withStoreId(storeId)
                .build();
        life.add(walService);
        logFile = walService.getLogFile();
        monitor = mock(ReverseTransactionCursorLoggingMonitor.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadFromSingleVersion(boolean presketch) throws Exception {
        // given
        writeTransactions(10);

        // when
        var readTransactions = readTransactions(presketch);

        // then
        assertRecovery(presketch, readTransactions, txId, BASE_TX_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadUptoASpecificStartingPosition(boolean presketch) throws Exception {
        // given
        var position = writeTransactions(2);
        writeTransactions(5);

        // when
        var readTransactions = readTransactions(position, presketch);

        // then
        assertRecovery(presketch, readTransactions, txId, BASE_TX_ID + 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadMultipleVersions(boolean presketch) throws Exception {
        // given
        writeTransactions(10);
        logFile.rotate();
        writeTransactions(5);
        logFile.rotate();
        writeTransactions(2);

        // when
        var readTransactions = readTransactions(presketch);

        // then
        assertRecovery(presketch, readTransactions, txId, BASE_TX_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadUptoASpecificStartingPositionFromMultipleVersions(boolean presketch) throws Exception {
        // given
        writeTransactions(10);
        logFile.rotate();
        var position = writeTransactions(5);
        writeTransactions(2);
        logFile.rotate();
        writeTransactions(2);

        // when
        var readTransactions = readTransactions(position, presketch);

        // then
        assertRecovery(presketch, readTransactions, txId, txId - 4);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleEmptyLogsMidStream(boolean presketch) throws Exception {
        // given
        writeTransactions(10);
        logFile.rotate();
        logFile.rotate();
        writeTransactions(2);

        // when
        var readTransactions = readTransactions(presketch);

        // then
        assertRecovery(presketch, readTransactions, txId, BASE_TX_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleEmptyTransactions(boolean presketch) throws Exception {
        // when
        var readTransactions = readTransactions(presketch);

        // then
        assertThat(readTransactions).isEmpty();
    }

    private CommittedCommandBatchRepresentation[] readTransactions(LogPosition position, boolean presketch)
            throws IOException {
        try (CommandBatchCursor cursor = txCursor(position, presketch)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatchRepresentation[] readTransactions(boolean presketch) throws IOException {
        return readTransactions(new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()), presketch);
    }

    private void assertRecovery(
            boolean presketch, CommittedCommandBatchRepresentation[] readTransactions, long highTxId, long lowTxId) {
        if (presketch) {
            verify(monitor).presketchingTransactionLogs();
        } else {
            verify(monitor, never()).presketchingTransactionLogs();
        }
        long expectedTxId = highTxId;
        for (CommittedCommandBatchRepresentation tx : readTransactions) {
            assertEquals(expectedTxId, tx.txId());
            expectedTxId--;
        }
        assertEquals(expectedTxId, lowTxId);
    }

    private CommandBatchCursor txCursor(LogPosition position, boolean presketch) {
        return fromLogFile(logFile, position, logEntryReader(), false, monitor, presketch, LogPosition.UNSPECIFIED);
    }

    private LogPosition writeTransactions(int count) throws IOException {
        TransactionLogWriter writer = (TransactionLogWriter) logFile.getTransactionLogWriter();
        FlushableLogPositionAwareChannel channel = writer.getChannel();
        int previousChecksum = BASE_TX_CHECKSUM;
        for (int i = 0; i < count; i++) {
            long transactionId = ++txId;
            previousChecksum = writer.append(
                    tx(random.intBetween(1, 5)),
                    transactionId,
                    UNKNOWN_CHUNK_ID,
                    transactionId,
                    previousChecksum,
                    UNKNOWN_APPEND_INDEX,
                    LogAppendEvent.NULL);
        }
        channel.prepareForFlush().flush();
        return writer.getCurrentPosition();
    }

    private static CommandBatch tx(int size) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            commands.add(new TestCommand());
        }
        return new CompleteCommandBatch(
                commands,
                UNKNOWN_CONSENSUS_INDEX,
                0,
                0,
                0,
                0,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
    }
}
