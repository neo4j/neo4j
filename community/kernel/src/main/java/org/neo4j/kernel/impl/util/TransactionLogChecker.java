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

import static org.neo4j.kernel.impl.util.TransactionLogChecker.FileType.CHECKPOINT_LOG;
import static org.neo4j.kernel.impl.util.TransactionLogChecker.FileType.TX_LOG;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.io.IOException;
import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.wal.LogFormatVersionProvider;
import org.neo4j.wal.LogVersionBridge;
import org.neo4j.wal.LogVersionedStoreChannel;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.VersionedFile;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.AbstractDetachedCheckpointLogEntry;
import org.neo4j.wal.entry.AbstractVersionAwareLogEntry;
import org.neo4j.wal.entry.LogEntry;
import org.neo4j.wal.entry.LogEntryCommit;
import org.neo4j.wal.entry.LogEntryReader;
import org.neo4j.wal.entry.LogEntryStart;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.entry.LogHeaderReader;
import org.neo4j.wal.entry.VersionAwareLogEntryReader;
import org.neo4j.wal.entry.v42.LogEntryStartV4_2;
import org.neo4j.wal.entry.v520.LogEntryChunkStart;
import org.neo4j.wal.entry.v520.LogEntryRollback;
import org.neo4j.wal.enveloped.EnvelopeReadChannel;
import org.neo4j.wal.enveloped.InvalidEndOfFileReadException;
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.files.LogRangeInfo;

public class TransactionLogChecker {
    private TransactionLogChecker() {}

    /**
     * Verify that the transaction log content and headers are correct in respect to
     * version changes and rotations. Each file should only contain transactions of a single kernel version.
     */
    public static void verifyCorrectTransactionLogUpgrades(
            FileSystemAbstraction fs, DatabaseLayout layout, Config config)
            throws IOException, InconsistentTransactionLogException {
        WALService walService = LogFilesBuilder.readableBuilder(
                        layout, fs, KernelVersionProvider.THROWING_PROVIDER, LogFormatVersionProvider.THROWING_PROVIDER)
                .build();

        Optional<StorageEngineFactory> storageEngineFactory = StorageEngineFactory.selectStorageEngine(fs, layout);
        CommandReaderFactory commandReaderFactory = storageEngineFactory
                .orElseThrow(() -> new IllegalStateException("Couldn't figure out storage engine from store files"))
                .commandReaderFactory();

        MultipleVersionInOneLogFileAcceptanceChecker txLogFileAcceptanceChecker =
                upgradeTxId -> walService.getCheckpointFile().reachableCheckpoints().stream()
                        .map(c -> c.transactionId().id())
                        .anyMatch(id -> id == upgradeTxId);
        MultipleVersionInOneLogFileAcceptanceChecker checkpointFileAcceptanceChecker = ignored -> false;

        verifyLogFiles(fs, config, walService.getLogFile(), commandReaderFactory, TX_LOG, txLogFileAcceptanceChecker);
        verifyLogFiles(
                fs,
                config,
                walService.getCheckpointFile(),
                commandReaderFactory,
                CHECKPOINT_LOG,
                checkpointFileAcceptanceChecker);
    }

    private static void verifyLogFiles(
            FileSystemAbstraction fs,
            Config config,
            VersionedFile logFile,
            CommandReaderFactory commandReaderFactory,
            FileType fileType,
            MultipleVersionInOneLogFileAcceptanceChecker acceptanceChecker)
            throws IOException {

        LastFileInfo lastFileInfo = new LastFileInfo(KernelVersion.EARLIEST, BASE_APPEND_INDEX);
        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        for (long i = logRangeInfo.lowestVersion(); i <= logRangeInfo.highestVersion(); i++) {
            LogHeader logHeader = LogHeaderReader.readLogHeader(fs, logFile.getLogFileForVersion(i), INSTANCE);
            if (logHeader == null) {
                throw new InconsistentTransactionLogException(
                        "Could not read log header of %s file version %d".formatted(fileType.lowerCase, i));
            }

            KernelVersion logHeaderKernelVersion = logHeader.getKernelVersion();
            if (versionLessThan(logHeaderKernelVersion, lastFileInfo.lastSeenKernelVersion)) {
                throw new InconsistentTransactionLogException(
                        "%s file version %d contains entry with lower kernel version (%s) than version seen in file with version %d (%s)"
                                .formatted(
                                        fileType.capitalized,
                                        i,
                                        logHeaderKernelVersion.name(),
                                        i - 1,
                                        lastFileInfo.lastSeenKernelVersion.name()));
            }
            if (lastSeenIndexDoesntMatchExpected(lastFileInfo, logHeader, fileType)) {
                throw new InconsistentTransactionLogException(
                        "%s file version %d header says last append index should be '%s' but the last append index seen in file with version %d was '%s'"
                                .formatted(
                                        fileType.capitalized,
                                        i,
                                        logHeader.getLastAppendIndex(),
                                        i - 1,
                                        lastFileInfo.lastSeenAppendIndex));
            }

            lastFileInfo = verifyVersionInOneFile(
                    logHeader,
                    logFile,
                    logHeaderKernelVersion,
                    lastFileInfo.lastSeenKernelVersion,
                    logHeader.getLastAppendIndex(),
                    commandReaderFactory,
                    config,
                    fileType,
                    acceptanceChecker);
        }
    }

    private static boolean lastSeenIndexDoesntMatchExpected(
            LastFileInfo lastFileInfo, LogHeader logHeader, FileType fileType) {
        return switch (fileType) {
            case TX_LOG ->
                lastFileInfo.lastSeenAppendIndex != BASE_APPEND_INDEX
                        ? lastFileInfo.lastSeenAppendIndex != logHeader.getLastAppendIndex()
                        : logHeader.getLastAppendIndex() < BASE_APPEND_INDEX;
            case CHECKPOINT_LOG -> logHeader.getLastAppendIndex() != UNKNOWN_APPEND_INDEX;
        };
    }

    private static LastFileInfo verifyVersionInOneFile(
            LogHeader logHeader,
            VersionedFile logFile,
            KernelVersion logHeaderKernelVersion,
            KernelVersion previouslySeenVersion,
            long previouslySeenAppendIndex,
            CommandReaderFactory commandReaderFactory,
            Config config,
            FileType fileType,
            MultipleVersionInOneLogFileAcceptanceChecker acceptanceChecker)
            throws IOException {
        LastFileInfo versionSeenInFile = logHeader.getLogFormatVersion().usesSegments()
                ? verifyVersionInSegmentedFile(
                        logFile, logHeader, logHeaderKernelVersion, previouslySeenAppendIndex, fileType)
                : verifyVersionInOldFile(
                        logFile,
                        logHeader,
                        logHeaderKernelVersion,
                        previouslySeenAppendIndex,
                        commandReaderFactory,
                        config,
                        fileType,
                        acceptanceChecker);

        // If there was no version in the header we have only checked that the file contains a single version so far.
        // Let's check that the version in the file is at least as great as the version seen in the previous file.
        if (logHeaderKernelVersion == null
                && versionLessThan(versionSeenInFile.lastSeenKernelVersion, previouslySeenVersion)) {
            throw new InconsistentTransactionLogException(
                    "%s file version %d contains entry with lower kernel version (%s) than version seen in previous file (%s)"
                            .formatted(
                                    fileType.capitalized,
                                    logHeader.getLogVersion(),
                                    versionSeenInFile.lastSeenKernelVersion.name(),
                                    previouslySeenVersion.name()));
        }
        // File with just header is allowed. Keep the latest version we've seen for further comparisons if that happens.
        if (versionSeenInFile.lastSeenKernelVersion == null) {
            versionSeenInFile = new LastFileInfo(
                    (logHeaderKernelVersion != null ? logHeaderKernelVersion : previouslySeenVersion),
                    versionSeenInFile.lastSeenAppendIndex);
        }
        return versionSeenInFile;
    }

    private static boolean versionLessThan(KernelVersion version, KernelVersion comparable) {
        return version != null && comparable != null && version.isLessThan(comparable);
    }

    private static LastFileInfo verifyVersionInSegmentedFile(
            VersionedFile logFile,
            LogHeader logHeader,
            KernelVersion expectedVersion,
            long previouslySeenAppendIndex,
            FileType fileType)
            throws IOException {
        PhysicalLogVersionedStoreChannel logChannel = logFile.openForVersion(logHeader.getLogVersion());
        logChannel.position(logHeader.getStartPosition().getByteOffset());

        try (VersionCheckingEnvelopeReadChannel versionCheckingEnvelopeReadChannel =
                new VersionCheckingEnvelopeReadChannel(
                        logChannel,
                        logHeader.getSegmentBlockSize(),
                        LogVersionBridge.NO_MORE_CHANNELS,
                        INSTANCE,
                        false,
                        expectedVersion,
                        previouslySeenAppendIndex,
                        fileType)) {

            if (findEnvelopeVersionErrors(versionCheckingEnvelopeReadChannel)) {
                throw new InconsistentTransactionLogException("%s file version %d malformed, could not read until end"
                        .formatted(fileType.capitalized, logHeader.getLogVersion()));
            }
            return new LastFileInfo(
                    versionCheckingEnvelopeReadChannel.expectedVersion,
                    versionCheckingEnvelopeReadChannel.previouslySeenAppendIndex);
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
            // InvalidEndOfFileReadException is fine as we are looking at only a single file
        } catch (InvalidEndOfFileReadException | ReadPastEndException e) {
            // Got to the end - good
            return false;
        }
        return true;
    }

    private static LastFileInfo verifyVersionInOldFile(
            VersionedFile logFile,
            LogHeader logHeader,
            KernelVersion expectedVersion,
            long previouslySeenAppendIndex,
            CommandReaderFactory commandReaderFactory,
            Config config,
            FileType fileType,
            MultipleVersionInOneLogFileAcceptanceChecker acceptanceChecker)
            throws IOException {
        KernelVersion seenVersion = expectedVersion;
        try (ReadableLogChannel reader =
                logFile.getReader(logHeader.getStartPosition(), LogVersionBridge.NO_MORE_CHANNELS)) {
            LogEntryReader entryReader = new VersionAwareLogEntryReader(
                    commandReaderFactory, new BinarySupportedKernelVersions(config), INSTANCE);

            LogEntry entry;
            long lastSeenTxId = -1;

            boolean getIndexFromCommitEntry = false;
            while ((entry = entryReader.readLogEntry(reader)) != null) {
                if (entry instanceof AbstractVersionAwareLogEntry versionedEntry) {
                    KernelVersion startVersion = versionedEntry.kernelVersion();
                    if (seenVersion == null) {
                        seenVersion = startVersion;
                    } else if (seenVersion != startVersion) {
                        if (startVersion.isLessThan(seenVersion)) {
                            throw new InconsistentTransactionLogException(
                                    "%s file version %d contains entry with lower kernel version (%s) than version seen earlier in the file (%s)"
                                            .formatted(
                                                    fileType.capitalized,
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
                        if (!acceptanceChecker.areMultipleVersionsAccepted(upgradeTxId)) {
                            throw new InconsistentTransactionLogException(
                                    "%s file version %d contains entry with other kernel version (%s) than version seen earlier in the file (%s)"
                                            .formatted(
                                                    fileType.capitalized,
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
                if (entry instanceof LogEntryStart startEntry) {
                    if (startEntry instanceof LogEntryStartV4_2) {
                        // Pre 5.20 there was no append index in the start entry - use txId from commit entry instead.
                        getIndexFromCommitEntry = true;
                    } else {
                        previouslySeenAppendIndex = validateExpectedAppendIndex(
                                startEntry.getAppendIndex(), logHeader, previouslySeenAppendIndex, fileType);
                    }
                } else if (getIndexFromCommitEntry && entry instanceof LogEntryCommit commitEntry) {
                    previouslySeenAppendIndex = validateExpectedAppendIndex(
                            commitEntry.getTxId(), logHeader, previouslySeenAppendIndex, fileType);
                    getIndexFromCommitEntry = false;
                } else if (entry instanceof LogEntryChunkStart chunkStart) {
                    previouslySeenAppendIndex = validateExpectedAppendIndex(
                            chunkStart.getAppendIndex(), logHeader, previouslySeenAppendIndex, fileType);
                } else if (entry instanceof LogEntryRollback rollback) {
                    previouslySeenAppendIndex = validateExpectedAppendIndex(
                            rollback.getAppendIndex(), logHeader, previouslySeenAppendIndex, fileType);
                } else if (entry instanceof AbstractDetachedCheckpointLogEntry) {
                    // The old format checkpoint logs never kept track of append indexes for the checkpoint entry.
                    // It did however still keep track in the file headers.
                    // Let's assume each entry we see had its own append index to be able to validate expected header
                    // indexes and the append indexes on the new format.
                    previouslySeenAppendIndex++;
                }
            }
        }
        return new LastFileInfo(seenVersion, previouslySeenAppendIndex);
    }

    private static long validateExpectedAppendIndex(
            long currentAppendIndex, LogHeader logHeader, long previouslySeenAppendIndex, FileType fileType) {
        if (currentAppendIndex != previouslySeenAppendIndex + 1) {
            throw new InconsistentTransactionLogException(
                    "%s file version %d contains entry with out of order append index '%d' seen after '%d'"
                            .formatted(
                                    fileType.capitalized,
                                    logHeader.getLogVersion(),
                                    currentAppendIndex,
                                    previouslySeenAppendIndex));
        }
        return previouslySeenAppendIndex + 1;
    }

    static class VersionCheckingEnvelopeReadChannel extends EnvelopeReadChannel {

        private final long logVersion;
        private KernelVersion expectedVersion;
        private byte expectedVersionByte;
        private long previouslySeenAppendIndex;
        private final FileType fileType;

        protected VersionCheckingEnvelopeReadChannel(
                LogVersionedStoreChannel startingChannel,
                int segmentBlockSize,
                LogVersionBridge bridge,
                MemoryTracker memoryTracker,
                boolean raw,
                KernelVersion expectedVersion,
                long previouslySeenAppendIndex,
                FileType fileType)
                throws IOException {
            super(startingChannel, segmentBlockSize, bridge, memoryTracker, raw);
            this.logVersion = startingChannel.getLogVersion();
            this.expectedVersion = expectedVersion;
            this.expectedVersionByte = expectedVersion != null ? expectedVersion.version() : -1;
            this.previouslySeenAppendIndex = previouslySeenAppendIndex;
            this.fileType = fileType;
        }

        @Override
        protected void readEnvelopeHeader() throws IOException {
            super.readEnvelopeHeader();
            if (expectedVersion == null) {
                expectedVersion = KernelVersion.getForVersion(payloadVersion);
                expectedVersionByte = payloadVersion;
            } else if (expectedVersionByte != payloadVersion) {
                throw new InconsistentTransactionLogException(
                        "%s file version %d contains entry with other kernel version (%s) than version seen earlier in the file (%s)"
                                .formatted(
                                        fileType.capitalized,
                                        logVersion,
                                        KernelVersion.getForVersion(payloadVersion)
                                                .name(),
                                        expectedVersion.name()));
            }

            checkExpectedAppendIndex();
        }

        void checkExpectedAppendIndex() throws IOException {
            if (previouslySeenAppendIndex == UNKNOWN_APPEND_INDEX) {
                previouslySeenAppendIndex = getAppendIndex();
                return;
            }

            switch (payloadType) {
                case FULL, BEGIN -> {
                    long currentAppendIndex = getAppendIndex();
                    if (currentAppendIndex != previouslySeenAppendIndex + 1) {
                        throw new InconsistentTransactionLogException(
                                "%s file version %d contains entry with out of order append index '%d' seen after '%d'"
                                        .formatted(
                                                fileType.capitalized,
                                                logVersion,
                                                currentAppendIndex,
                                                previouslySeenAppendIndex));
                    }
                    previouslySeenAppendIndex++;
                }
                case MIDDLE, END -> {
                    long currentAppendIndex = getAppendIndex();
                    if (currentAppendIndex != previouslySeenAppendIndex) {
                        throw new InconsistentTransactionLogException(
                                "%s file version %d contains continuation entry (MIDDLE/END) with different append index '%d' than start '%d'"
                                        .formatted(
                                                fileType.capitalized,
                                                logVersion,
                                                currentAppendIndex,
                                                previouslySeenAppendIndex));
                    }
                }
            }
        }
    }

    private record LastFileInfo(KernelVersion lastSeenKernelVersion, long lastSeenAppendIndex) {}

    enum FileType {
        TX_LOG("Log", "log"),
        CHECKPOINT_LOG("Checkpoint", "checkpoint");

        final String capitalized;
        final String lowerCase;

        FileType(String capitalized, String lowerCase) {
            this.capitalized = capitalized;
            this.lowerCase = lowerCase;
        }
    }

    @FunctionalInterface
    interface MultipleVersionInOneLogFileAcceptanceChecker {
        boolean areMultipleVersionsAccepted(long upgradeTxId) throws IOException;
    }
}
