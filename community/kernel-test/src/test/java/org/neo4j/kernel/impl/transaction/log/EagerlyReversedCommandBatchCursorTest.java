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
package org.neo4j.kernel.impl.transaction.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.given;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.reverse.EagerlyReversedCommandBatchCursor.eagerlyReverse;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;

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
    private LogFiles logFiles;

    @BeforeEach
    void setUp() throws IOException {
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        logFiles = LogFilesBuilder.builder(databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
        life.add(logFiles);
        logFile = logFiles.getLogFile();
    }

    @Test
    void reverseTransactionsFromSource() throws Exception {
        int transactionsToGenerate = 10;
        writeTransactions(transactionsToGenerate);

        int observedTransaction = 0;
        try (ReadableLogChannel reader =
                logFile.getReader(logFiles.getLogFile().extractHeader(0).getStartPosition())) {
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
        List<LogPosition> startPositions = writeTransactions(transactionsToGenerate);
        Collections.reverse(startPositions);

        int observedTransaction = 0;
        int transaction = 0;
        try (ReadableLogChannel reader =
                logFile.getReader(logFiles.getLogFile().extractHeader(0).getStartPosition())) {
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

    private List<LogPosition> writeTransactions(int count) throws IOException {
        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int previousChecksum = BASE_TX_CHECKSUM;
        var positions = new ArrayList<LogPosition>(count);
        for (int i = 0; i < count; i++) {
            long transactionId = ++txId;
            positions.add(writer.getCurrentPosition());
            previousChecksum = writer.append(
                    tx(random.intBetween(1, 5)),
                    transactionId,
                    transactionId,
                    UNKNOWN_CHUNK_ID,
                    previousChecksum,
                    UNKNOWN_APPEND_INDEX,
                    LogAppendEvent.NULL);
        }
        channel.prepareForFlush().flush();
        return positions;
    }

    private static CommandBatch tx(int size) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            commands.add(new TestCommand());
        }
        return new CompleteCommandBatch(
                commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, LatestVersions.LATEST_KERNEL_VERSION, ANONYMOUS);
    }
}
