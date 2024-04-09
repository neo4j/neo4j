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
package org.neo4j.kernel.impl.transaction.log.stresstest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.stresstest.workload.Runner;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class TransactionAppenderStressTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void concurrentTransactionAppendingTest() throws Exception {
        int threads = 10;
        Callable<Long> runner = new Builder()
                .with(untilTimeExpired(10, SECONDS))
                .withWorkingDirectory(databaseLayout)
                .withNumThreads(threads)
                .build();

        long appendedTxs = runner.call();

        assertEquals(
                new TransactionIdChecker(databaseLayout.getTransactionLogsDirectory()).parseAllTxLogs(), appendedTxs);
    }

    public static class Builder {
        private BooleanSupplier condition;
        private DatabaseLayout databaseLayout;
        private int threads;

        public Builder with(BooleanSupplier condition) {
            this.condition = condition;
            return this;
        }

        public Builder withWorkingDirectory(DatabaseLayout databaseLayout) {
            this.databaseLayout = databaseLayout;
            return this;
        }

        public Builder withNumThreads(int threads) {
            this.threads = threads;
            return this;
        }

        public Callable<Long> build() {
            return new Runner(databaseLayout, condition, threads);
        }
    }

    public static class TransactionIdChecker {
        private final Path workingDirectory;

        public TransactionIdChecker(Path workingDirectory) {
            this.workingDirectory = workingDirectory;
        }

        public long parseAllTxLogs() throws IOException {
            // Initialize this txId to the BASE_TX_ID because if we don't find any tx log that means that
            // no transactions have been appended in this test and that getLastCommittedTransactionId()
            // will also return this constant. Why this is, is another question - but thread scheduling and
            // I/O spikes on some build machines can be all over the place and also the test duration is
            // configurable.
            long txId = TransactionIdStore.BASE_TX_ID;

            try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                    ReadableLogChannel channel = openLogFile(fs)) {
                LogEntryReader reader = logEntryReader();
                LogEntry logEntry = reader.readLogEntry(channel);
                for (; logEntry != null; logEntry = reader.readLogEntry(channel)) {
                    if (logEntry instanceof LogEntryCommit) {
                        txId = ((LogEntryCommit) logEntry).getTxId();
                    }
                }
            }
            return txId;
        }

        private ReadableLogChannel openLogFile(FileSystemAbstraction fs) throws IOException {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(workingDirectory, fs)
                    .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                    .build();
            final var logFile = logFiles.getLogFile();
            return ReadAheadUtils.newChannel(logFile, 0, ReaderLogVersionBridge.forFile(logFile), INSTANCE);
        }
    }
}
