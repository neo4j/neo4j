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

import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionIdChecker {
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
                ReadableLogChannel channel = openLogFile(fs, 0)) {
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

    private ReadableLogChannel openLogFile(FileSystemAbstraction fs, int version) throws IOException {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(workingDirectory, fs)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .build();
        LogFile logFile = logFiles.getLogFile();
        PhysicalLogVersionedStoreChannel channel = logFile.openForVersion(version);
        return new ReadAheadLogChannel(channel, ReaderLogVersionBridge.forFile(logFile), INSTANCE);
    }
}
