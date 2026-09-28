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
package org.neo4j.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.wal.GivenCommandBatchCursor.exhaust;
import static org.neo4j.wal.GivenCommandBatchCursor.given;
import static org.neo4j.wal.TestLogEntryReader.logEntryReader;
import static org.neo4j.wal.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.wal.reverse.EagerlyReversedCommandBatchCursor.eagerlyReverse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
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
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.files.LogRangeInfo;

@Neo4jLayoutExtension
@ExtendWith({RandomExtension.class, LifeExtension.class})
class EagerlyReversedCommandBatchCursorTest {

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private LifeSupport life;

    @Inject
    private RandomSupport random;

    private long txId = BASE_TX_ID;
    private LogFile logFile;
    private WALService walService;

    private static final byte[] LARGE_TX_PADDING = new byte[2 * DEFAULT_LOG_SEGMENT_SIZE];

    @BeforeEach
    void setUp() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fs,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withRotationThreshold(ByteUnit.kibiBytes(512L))
                .withStoreId(storeId)
                .build();
        life.add(walService);
        logFile = walService.getLogFile();
    }

    @Test
    void reverseTransactionsFromSource() throws Exception {
        int transactionsToGenerate = 10;
        writeTransactions(transactionsToGenerate, false);

        int observedTransaction = 0;
        try (ReadableLogChannel reader =
                logFile.getReader(walService.getLogFile().extractHeader(0).getStartPosition())) {
            var source = new CommittedCommandBatchCursor(reader, logEntryReader());
            CommandBatchCursor cursor = eagerlyReverse(source);

            long currentTxId = txId;
            while (cursor.next()) {
                CommittedCommandBatchRepresentation commandBatch = cursor.get();
                assertEquals(currentTxId--, commandBatch.txId());
                observedTransaction++;
            }
        }
        assertEquals(transactionsToGenerate, observedTransaction);
    }

    @Test
    void reverseCursorBatchStartPositions() throws IOException {
        int transactionsToGenerate = 10;
        List<LogPosition> startPositions = writeTransactions(transactionsToGenerate, false);
        Collections.reverse(startPositions);

        int observedTransaction = 0;
        int transaction = 0;
        try (ReadableLogChannel reader =
                logFile.getReader(walService.getLogFile().extractHeader(0).getStartPosition())) {
            var source = new CommittedCommandBatchCursor(reader, logEntryReader());
            CommandBatchCursor cursor = eagerlyReverse(source);

            long currentTxId = txId;
            while (cursor.next()) {
                CommittedCommandBatchRepresentation commandBatch = cursor.get();
                assertEquals(currentTxId--, commandBatch.txId());
                assertEquals(startPositions.get(transaction++), cursor.position());
                observedTransaction++;
            }
        }
        assertEquals(transactionsToGenerate, observedTransaction);
    }

    @Test
    void handleEmptySource() throws Exception {
        // GIVEN
        CommandBatchCursor source = given();
        CommandBatchCursor cursor = eagerlyReverse(source);

        // WHEN
        CommittedCommandBatchRepresentation[] reversed = exhaust(cursor);

        // THEN
        assertEquals(0, reversed.length);
    }

    @Test
    void handleLargeTransactionsThatWithEnvelopesSpanFiles() throws IOException {
        int transactionsToGenerate = 10;
        // write transactions we know will span segments and cross file boundaries
        // when on enveloped files
        List<LogPosition> startPositions = writeTransactions(transactionsToGenerate, true);
        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        assertTrue(logRangeInfo.lowestVersion() < logRangeInfo.highestVersion(), "Failed to roll log files");
        Collections.reverse(startPositions);

        int observedTransaction = 0;
        int transaction = 0;
        try (ReadableLogChannel reader =
                logFile.getReader(walService.getLogFile().extractHeader(0).getStartPosition())) {
            var source = new CommittedCommandBatchCursor(reader, logEntryReader());
            CommandBatchCursor cursor = eagerlyReverse(source);

            long currentTxId = txId;
            while (cursor.next()) {
                CommittedCommandBatchRepresentation commandBatch = cursor.get();
                assertEquals(currentTxId--, commandBatch.txId());
                assertEquals(startPositions.get(transaction++), cursor.position());
                observedTransaction++;
            }
        }
        assertEquals(transactionsToGenerate, observedTransaction);
    }

    private List<LogPosition> writeTransactions(int count, boolean large) throws IOException {
        TransactionLogWriter writer = (TransactionLogWriter) logFile.getTransactionLogWriter();
        FlushableLogPositionAwareChannel channel = writer.getChannel();
        int previousChecksum = BASE_TX_CHECKSUM;
        var positions = new ArrayList<LogPosition>(count);
        for (int i = 0; i < count; i++) {
            long transactionId = ++txId;
            positions.add(writer.getCurrentPosition());
            if (large && logFile.rotationNeeded()) {
                // rotate after position capture to ensure consistency with verify loop
                // should never be required with enveloped log files
                assertTrue(LatestVersions.LATEST_KERNEL_VERSION.isLessThan(
                        KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED));
                logFile.rotate();
            }
            previousChecksum = writer.append(
                    tx(random.intBetween(1, 5), large),
                    transactionId,
                    UNKNOWN_CHUNK_ID,
                    transactionId,
                    previousChecksum,
                    UNKNOWN_APPEND_INDEX,
                    LogAppendEvent.NULL);
        }
        channel.prepareForFlush().flush();
        return positions;
    }

    private static CommandBatch tx(int size, boolean large) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (large) {
                commands.add(new TestCommand(LARGE_TX_PADDING));
            } else {
                commands.add(new TestCommand());
            }
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
