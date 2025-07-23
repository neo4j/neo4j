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
package org.neo4j.kernel.impl.util;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeReadChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class TransactionLogChecker {
    private TransactionLogChecker() {}

    /**
     * Verify that the transaction log content and headers are correct in respect to
     * version changes and rotations. Each file should only contain transactions of a single kernel version.
     */
    public static void verifyCorrectTransactionLogUpgrades(
            FileSystemAbstraction fs, DatabaseLayout layout, Config config)
            throws IOException, InconsistentTransactionLogException {
        LogFiles logFiles = LogFilesBuilder.readOnlyBuilder(layout, fs, KernelVersionProvider.THROWING_PROVIDER)
                .build();
        LogFile logFile = logFiles.getLogFile();

        Optional<StorageEngineFactory> storageEngineFactory = StorageEngineFactory.selectStorageEngine(fs, layout);
        CommandReaderFactory commandReaderFactory = storageEngineFactory
                .orElseThrow(() -> new IllegalStateException("Couldn't figure out storage engine from store files"))
                .commandReaderFactory();

        KernelVersion versionSeen = KernelVersion.EARLIEST;
        for (long i = logFile.getLowestLogVersion(); i <= logFile.getHighestLogVersion(); i++) {
            LogHeader logHeader = LogHeaderReader.readLogHeader(
                    fs, logFiles.getLogFile().getLogFileForVersion(i), EmptyMemoryTracker.INSTANCE);
            if (logHeader == null) {
                throw new InconsistentTransactionLogException(
                        "Could not read log header of log file version %d".formatted(i));
            }

            KernelVersion logHeaderKernelVersion = logHeader.getKernelVersion();
            if (versionLessThan(logHeaderKernelVersion, versionSeen)) {
                throw new InconsistentTransactionLogException(
                        "Log file version %d contains entry with lower kernel version (%s) than version seen in file with version %d (%s)"
                                .formatted(i, logHeaderKernelVersion.name(), i - 1, versionSeen.name()));
            }

            versionSeen = verifyVersionInOneFile(
                    logHeader,
                    logFile,
                    logHeaderKernelVersion,
                    versionSeen,
                    commandReaderFactory,
                    config,
                    logFiles.getCheckpointFile());
        }
    }

    private static KernelVersion verifyVersionInOneFile(
            LogHeader logHeader,
            LogFile logFile,
            KernelVersion logHeaderKernelVersion,
            KernelVersion previouslySeenVersion,
            CommandReaderFactory commandReaderFactory,
            Config config,
            CheckpointFile checkpointFile)
            throws IOException {
        KernelVersion versionSeenInFile = logHeader.getLogFormatVersion().usesSegments()
                ? verifyVersionInSegmentedFile(logFile, logHeader, logHeaderKernelVersion)
                : verifyVersionInOldFile(
                        logFile, logHeader, logHeaderKernelVersion, commandReaderFactory, config, checkpointFile);

        // If there was no version in the header we have only checked that the file contains a single version so far.
        // Let's check that the version in the file is at least as great as the version seen in the previous file.
        if (logHeaderKernelVersion == null && versionLessThan(versionSeenInFile, previouslySeenVersion)) {
            throw new InconsistentTransactionLogException(
                    "Log file version %d contains entry with lower kernel version (%s) than version seen in previous file (%s)"
                            .formatted(
                                    logHeader.getLogVersion(), versionSeenInFile.name(), previouslySeenVersion.name()));
        }
        // File with just header is allowed. Keep the latest version we've seen for further comparisons if that happens.
        return versionSeenInFile != null
                ? versionSeenInFile
                : (logHeaderKernelVersion != null ? logHeaderKernelVersion : previouslySeenVersion);
    }

    private static boolean versionLessThan(KernelVersion version, KernelVersion comparable) {
        return version != null && comparable != null && version.isLessThan(comparable);
    }

    public static KernelVersion verifyVersionInSegmentedFile(
            LogFile logFile, LogHeader logHeader, KernelVersion expectedVersion) throws IOException {
        PhysicalLogVersionedStoreChannel logChannel = logFile.openForVersion(logHeader.getLogVersion(), false);
        logChannel.position(logHeader.getStartPosition().getByteOffset());

        try (VersionCheckingEnvelopeReadChannel versionCheckingEnvelopeReadChannel =
                new VersionCheckingEnvelopeReadChannel(
                        logChannel,
                        logHeader.getSegmentBlockSize(),
                        LogVersionBridge.NO_MORE_CHANNELS,
                        INSTANCE,
                        false,
                        expectedVersion)) {

            if (findEnvelopeVersionErrors(versionCheckingEnvelopeReadChannel)) {
                throw new InconsistentTransactionLogException(
                        "Log file version %d malformed, could not read until end".formatted(logHeader.getLogVersion()));
            }
            return versionCheckingEnvelopeReadChannel.expectedVersion;
        }
    }

    private static boolean findEnvelopeVersionErrors(
            VersionCheckingEnvelopeReadChannel versionCheckingEnvelopeReadChannel) throws IOException {
        long prevPos = -1;
        long pos;
        try {
            while (prevPos < (pos = versionCheckingEnvelopeReadChannel.goToNextEntry())) {
                prevPos = pos;
            }
        } catch (ReadPastEndException e) {
            // Got to the end - good
            return false;
        }
        return true;
    }

    public static KernelVersion verifyVersionInOldFile(
            LogFile logFile,
            LogHeader logHeader,
            KernelVersion expectedVersion,
            CommandReaderFactory commandReaderFactory,
            Config config,
            CheckpointFile checkpointFile)
            throws IOException {
        KernelVersion seenVersion = expectedVersion;
        try (ReadableLogChannel reader =
                logFile.getReader(logHeader.getStartPosition(), LogVersionBridge.NO_MORE_CHANNELS)) {
            LogEntryReader entryReader =
                    new VersionAwareLogEntryReader(commandReaderFactory, new BinarySupportedKernelVersions(config));

            LogEntry entry;
            long lastSeenTxId = -1;

            while ((entry = entryReader.readLogEntry(reader)) != null) {
                if (entry instanceof LogEntryStart start) {
                    KernelVersion startVersion = start.kernelVersion();
                    if (seenVersion == null) {
                        seenVersion = startVersion;
                    } else if (seenVersion != startVersion) {
                        if (startVersion.isLessThan(seenVersion)) {
                            throw new InconsistentTransactionLogException(
                                    "Log file version %d contains entry with lower kernel version (%s) than version seen earlier in the file (%s)"
                                            .formatted(
                                                    logHeader.getLogVersion(),
                                                    startVersion.name(),
                                                    seenVersion.name()));
                        }
                        // lastSeenTxId has not been updated for this entry yet,
                        // it is the tx id of the previous entry.
                        // Since this is the first entry with a different version,
                        // the last entry should belong to an upgrade command.

                        // There is an edge case in which tx pull pulls everything up to and
                        // including the upgrade transaction and then starts the store and
                        // fails to rotate the tx log. This is benign though for non-segmented
                        // logs and won't be fixed. In order to minimize false positives,
                        // we skip throwing here if we find that there is a checkpoint exactly
                        // at the tx id of the upgrade transaction. The benign case described
                        // will do recovery before starting the DB, and thus it will always
                        // have a checkpoint with the tx id of the upgrade tx.

                        final var upgradeTxId = lastSeenTxId;
                        if (checkpointFile.reachableCheckpoints().stream()
                                .map(c -> c.transactionId().id())
                                .noneMatch(id -> id == upgradeTxId)) {
                            throw new InconsistentTransactionLogException(
                                    "Log file version %d contains entry with other kernel version (%s) than version seen earlier in the file (%s)"
                                            .formatted(
                                                    logHeader.getLogVersion(),
                                                    startVersion.name(),
                                                    seenVersion.name()));
                        } else {
                            seenVersion = startVersion;
                        }
                    }
                }
                if (entry instanceof LogEntryCommit commit) {
                    lastSeenTxId = commit.getTxId();
                }
            }
        }
        return seenVersion;
    }

    static class VersionCheckingEnvelopeReadChannel extends EnvelopeReadChannel {

        private final long logVersion;
        private KernelVersion expectedVersion;
        private byte expectedVersionByte;

        protected VersionCheckingEnvelopeReadChannel(
                LogVersionedStoreChannel startingChannel,
                int segmentBlockSize,
                LogVersionBridge bridge,
                MemoryTracker memoryTracker,
                boolean raw,
                KernelVersion expectedVersion)
                throws IOException {
            super(startingChannel, segmentBlockSize, bridge, memoryTracker, raw);
            this.logVersion = startingChannel.getLogVersion();
            this.expectedVersion = expectedVersion;
            this.expectedVersionByte = expectedVersion != null ? expectedVersion.version() : -1;
        }

        @Override
        protected void readEnvelopeHeader() throws IOException {
            super.readEnvelopeHeader();
            if (expectedVersion == null) {
                expectedVersion = KernelVersion.getForVersion(payloadVersion);
                expectedVersionByte = payloadVersion;
            } else if (expectedVersionByte != payloadVersion) {
                throw new InconsistentTransactionLogException(
                        "Log file version %d contains entry with other kernel version (%s) than version seen earlier in the file (%s)"
                                .formatted(
                                        logVersion,
                                        KernelVersion.getForVersion(payloadVersion)
                                                .name(),
                                        expectedVersion.name()));
            }
        }
    }
}
