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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v522.LogEntryDetachedCheckpointV5_22;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class TxLogValidationUtils {
    private TxLogValidationUtils() {}

    public static LogHeader assertLogHeaderExpectedVersion(
            FileSystemAbstraction fs, LogFiles logFiles, long logVersion, KernelVersion expectedVersion)
            throws IOException {
        LogHeader logHeader = LogHeaderReader.readLogHeader(
                fs, logFiles.getLogFile().getLogFileForVersion(logVersion), EmptyMemoryTracker.INSTANCE);
        assertThat(logHeader.getKernelVersion()).isEqualTo(expectedVersion);
        return logHeader;
    }

    public static LogHeader assertLogHeaderExpectedVersion(
            FileSystemAbstraction fs,
            LogFiles logFiles,
            long logVersion,
            KernelVersion expectedVersion,
            long lastExpectedAppendIndex)
            throws IOException {
        LogHeader logHeader = assertLogHeaderExpectedVersion(fs, logFiles, logVersion, expectedVersion);
        assertThat(logHeader.getLastAppendIndex()).isEqualTo(lastExpectedAppendIndex);
        return logHeader;
    }

    public static void assertLogHeaderExpectedVersion(
            FileSystemAbstraction fs,
            LogFiles logFiles,
            long logVersion,
            KernelVersion expectedVersion,
            long lastExpectedAppendIndex,
            int checksum)
            throws IOException {
        LogHeader logHeader =
                assertLogHeaderExpectedVersion(fs, logFiles, logVersion, expectedVersion, lastExpectedAppendIndex);
        assertThat(logHeader.getPreviousLogFileChecksum()).isEqualTo(checksum);
    }

    public static void assertCheckpointLogHeaderExpectedVersion(
            FileSystemAbstraction fs, LogFiles logFiles, long logVersion, KernelVersion expectedVersion, int checksum)
            throws IOException {
        LogHeader logHeader = assertCheckpointLogHeaderExpectedVersion(fs, logFiles, logVersion, expectedVersion);
        assertThat(logHeader.getPreviousLogFileChecksum()).isEqualTo(checksum);
    }

    public static LogHeader assertCheckpointLogHeaderExpectedVersion(
            FileSystemAbstraction fs, LogFiles logFiles, long logVersion, KernelVersion expectedVersion)
            throws IOException {
        LogHeader logHeader = LogHeaderReader.readLogHeader(
                fs, logFiles.getCheckpointFile().getLogFileForVersion(logVersion), EmptyMemoryTracker.INSTANCE);
        assertThat(logHeader.getKernelVersion()).isEqualTo(expectedVersion);
        assertThat(logHeader.getLastAppendIndex()).isEqualTo(AppendIndexProvider.UNKNOWN_APPEND_INDEX);
        return logHeader;
    }

    public static void assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
            LogFile logFile,
            long logVersion,
            KernelVersion kernelVersion,
            int expectedNbrTxs,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        assertThat(assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                        logFile, logVersion, kernelVersion, commandReaderFactory))
                .isEqualTo(expectedNbrTxs);
    }

    public static int assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
            LogFile logFile, long logVersion, KernelVersion kernelVersion, CommandReaderFactory commandReaderFactory)
            throws IOException {
        return assertWholeTransactionsIn(
                logFile,
                logVersion,
                startEntry -> assertThat(startEntry.kernelVersion()).isEqualTo(kernelVersion),
                commitEntry -> {},
                commandReaderFactory);
    }

    public static int assertWholeTransactionsIn(
            LogFile logFile,
            long logVersion,
            Consumer<LogEntryStart> extraStartCheck,
            Consumer<LogEntryCommit> extraCommitCheck,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        return assertWholeTransactionsIn(
                logFile,
                logVersion,
                extraStartCheck,
                extraCommitCheck,
                commandReaderFactory,
                LogVersionBridge.NO_MORE_CHANNELS);
    }

    public static int assertWholeTransactionsIn(
            LogFile logFile,
            long logVersion,
            Consumer<LogEntryStart> extraStartCheck,
            Consumer<LogEntryCommit> extraCommitCheck,
            CommandReaderFactory commandReaderFactory,
            LogVersionBridge logVersionBridge)
            throws IOException {
        int transactions = 0;

        try (ReadableLogChannel reader =
                logFile.getReader(logFile.extractHeader(logVersion).getStartPosition(), logVersionBridge)) {
            LogEntryReader entryReader = new VersionAwareLogEntryReader(
                    commandReaderFactory,
                    new BinarySupportedKernelVersions(Config.defaults(
                            GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())),
                    EmptyMemoryTracker.INSTANCE);
            LogEntry entry;
            boolean inTx = false;
            while ((entry = entryReader.readLogEntry(reader)) != null) {
                if (!inTx) // Expects start entry
                {
                    assertInstanceOf(LogEntryStart.class, entry);
                    extraStartCheck.accept((LogEntryStart) entry);
                    inTx = true;
                } else // Expects command/commit entry
                {
                    assertTrue(entry instanceof LogEntryCommand || entry instanceof LogEntryCommit);
                    if (entry instanceof LogEntryCommit commit) {
                        inTx = false;
                        transactions++;
                        extraCommitCheck.accept(commit);
                    }
                }
            }
            assertFalse(inTx);
        }
        return transactions;
    }

    public static int assertReadableCheckpointsInOneFile(
            CheckpointFile checkpointFile,
            long logVersion,
            KernelVersion expectedKernelVersion,
            CommandReaderFactory commandReaderFactory,
            int expectedNbrCheckpoints)
            throws IOException {
        return assertReadableCheckpointsIn(
                checkpointFile,
                logVersion,
                LogVersionBridge.NO_MORE_CHANNELS,
                expectedKernelVersion,
                commandReaderFactory,
                expectedNbrCheckpoints);
    }

    /**
     * Validates the checksum chains, including previous checksum in log header.
     */
    public static void assertReadableCheckpointsFromVersion(
            CheckpointFile checkpointFile,
            long logVersion,
            KernelVersion expectedKernelVersion,
            CommandReaderFactory commandReaderFactory,
            int expectedNbrCheckpoints)
            throws IOException {
        assertReadableCheckpointsIn(
                checkpointFile,
                logVersion,
                ReaderLogVersionBridge.forFile(checkpointFile),
                expectedKernelVersion,
                commandReaderFactory,
                expectedNbrCheckpoints);
    }

    // returns final checksum to feed into next file
    public static int assertReadableCheckpointsIn(
            CheckpointFile checkpointFile,
            long logVersion,
            LogVersionBridge bridge,
            KernelVersion expectedKernelVersion,
            CommandReaderFactory commandReaderFactory,
            int expectedNbrCheckpoints)
            throws IOException {
        LogReadResult read = countReadableCheckpoints(
                checkpointFile, logVersion, bridge, expectedKernelVersion, commandReaderFactory);
        assertThat(read.count).isEqualTo(expectedNbrCheckpoints);
        return read.checksum;
    }

    public static LogReadResult countReadableCheckpoints(
            CheckpointFile checkpointFile,
            long logVersion,
            LogVersionBridge bridge,
            KernelVersion expectedKernelVersion,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        int nbrCheckpoints = 0;
        int checksum;
        try (ReadableLogChannel reader = checkpointFile.getReader(
                checkpointFile.extractHeader(logVersion).getStartPosition(), bridge)) {
            LogEntryReader entryReader = new VersionAwareLogEntryReader(
                    commandReaderFactory,
                    new BinarySupportedKernelVersions(Config.defaults(
                            GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())),
                    EmptyMemoryTracker.INSTANCE);
            LogEntry entry;
            while ((entry = entryReader.readLogEntry(reader)) != null) {
                LogEntryDetachedCheckpointV5_22 logEntry =
                        assertInstanceOf(LogEntryDetachedCheckpointV5_22.class, entry);
                if (expectedKernelVersion != null) {
                    assertThat(logEntry.kernelVersion()).isEqualTo(expectedKernelVersion);
                }
                nbrCheckpoints++;
            }
            checksum = reader.getChecksum();
        }
        return new LogReadResult(nbrCheckpoints, checksum);
    }

    public record LogReadResult(int count, int checksum) {}

    /**
     * The append index and enveloped term of a single log entry, as read back from an enveloped
     * ({@link org.neo4j.kernel.impl.transaction.log.entry.LogFormat#usesSegments() segmented}) transaction log file.
     */
    public record EntryTerm(long appendIndex, long term) {}

    /**
     * Reads the per-envelope {@code term} of every entry in a single enveloped transaction log file version, in log
     * order. Non-enveloped (pre-V10) files carry no envelope terms, so an empty list is returned for those.
     */
    public static List<EntryTerm> readEntryTerms(LogFile logFile, long logVersion) throws IOException {
        List<EntryTerm> entryTerms = new ArrayList<>();
        try (ReadableLogChannel reader = ReadAheadUtils.newChannel(logFile, logVersion, EmptyMemoryTracker.INSTANCE)) {
            if (!reader.supportsEntrySkipping()) {
                // Only enveloped/segmented formats carry a per-entry term.
                return entryTerms;
            }
            try {
                reader.alignWithStartEntry();
                if (reader.getAppendIndex() == LogEnvelopeHeader.UNSPECIFIED_INDEX) {
                    // File only holds a header, no entries.
                    return entryTerms;
                }
                entryTerms.add(new EntryTerm(reader.getAppendIndex(), reader.getTerm()));
                while (true) {
                    reader.goToNextEntry();
                    entryTerms.add(new EntryTerm(reader.getAppendIndex(), reader.getTerm()));
                }
            } catch (ReadPastEndException endOfEntries) {
                // Reached the end of the entries in this file.
            }
        }
        return entryTerms;
    }
}
