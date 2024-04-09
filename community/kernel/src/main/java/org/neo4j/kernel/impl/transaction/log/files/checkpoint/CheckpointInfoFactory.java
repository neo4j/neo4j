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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;

import java.util.Optional;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryDetachedCheckpointV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.storageengine.api.TransactionId;

public final class CheckpointInfoFactory {
    // transaction id - long
    // time written - long
    // checksum - int
    // 2 bytes for version code and entry code
    private static final long COMMIT_ENTRY_OFFSET = 2 * Long.BYTES + Integer.BYTES + 2 * Byte.BYTES;
    // older version of commit entry that do not have checksum as part of entry
    private static final long LEGACY_COMMIT_ENTRY_OFFSET = 2 * Long.BYTES + 2 * Byte.BYTES;

    private CheckpointInfoFactory() {}

    public static CheckpointInfo ofLogEntry(
            LogEntry entry,
            LogPosition checkpointEntryPosition,
            LogPosition channelPositionAfterCheckpoint,
            LogPosition checkpointFilePostReadPosition,
            TransactionLogFilesContext context,
            LogFile logFile) {
        if (entry instanceof LogEntryDetachedCheckpointV4_2 checkpoint42) {
            var transactionId = readTransactionInfoFor4_2(context, logFile, checkpoint42.getLogPosition());
            return new CheckpointInfo(
                    checkpoint42.getLogPosition(),
                    checkpoint42.getStoreId(),
                    checkpointEntryPosition,
                    channelPositionAfterCheckpoint,
                    // we need to use kernel version from transaction command since checkpoints were broken in old
                    // version and used incorrect kernel version
                    checkpointFilePostReadPosition,
                    transactionId.kernelVersion(),
                    transactionId.kernelVersion().version(),
                    transactionId,
                    checkpoint42.getReason());
        } else if (entry instanceof LogEntryDetachedCheckpointV5_0 checkpoint50) {
            return new CheckpointInfo(
                    checkpoint50.getLogPosition(),
                    checkpoint50.getStoreId(),
                    checkpointEntryPosition,
                    channelPositionAfterCheckpoint,
                    checkpointFilePostReadPosition,
                    checkpoint50.kernelVersion(),
                    checkpoint50.kernelVersion().version(),
                    checkpoint50.getTransactionId(),
                    checkpoint50.getReason(),
                    checkpoint50.consensusIndexInCheckpoint());
        } else {
            throw new UnsupportedOperationException(
                    "Expected to observe only checkpoint entries, but: `" + entry + "` was found.");
        }
    }

    private static TransactionId readTransactionInfoFor4_2(
            TransactionLogFilesContext context, LogFile logFile, LogPosition transactionPosition) {
        try (var channel = logFile.openForVersion(transactionPosition.getLogVersion());
                var reader = new ReadAheadLogChannel(new UnclosableChannel(channel), context.getMemoryTracker());
                var logEntryCursor = new LogEntryCursor(
                        new VersionAwareLogEntryReader(
                                context.getCommandReaderFactory(), context.getBinarySupportedKernelVersions()),
                        reader)) {
            LogPosition checkedPosition = null;
            LogEntryStart logEntryStart = null;
            while (logEntryCursor.next()) {
                LogEntry logEntry = logEntryCursor.get();
                if (logEntry instanceof LogEntryStart) {
                    logEntryStart = (LogEntryStart) logEntry;
                }
                checkedPosition = reader.getCurrentLogPosition();
                if (logEntry instanceof LogEntryCommit commit && checkedPosition.equals(transactionPosition)) {
                    if (logEntryStart == null) {
                        throw new IllegalStateException("Transaction commit entry for tx id: " + commit.getTxId()
                                + " was found but transaction start was missing.");
                    }
                    return new TransactionId(
                            commit.getTxId(),
                            logEntryStart.kernelVersion(),
                            commit.getChecksum(),
                            commit.getTimeWritten(),
                            UNKNOWN_CONSENSUS_INDEX);
                }
            }

            // We have a checkpoint on this point but there is no transaction found that match it and log files are
            // corrupted. Database should be restored from the last valid backup or dump in normal circumstances.
            if (!context.getConfig().get(fail_on_corrupted_log_files)) {
                return new TransactionId(
                        UNKNOWN_TRANSACTION_ID.id(),
                        KernelVersion.V4_4,
                        UNKNOWN_TRANSACTION_ID.checksum(),
                        UNKNOWN_TRANSACTION_ID.commitTimestamp(),
                        UNKNOWN_TRANSACTION_ID.consensusIndex());
            }
            throw new IllegalStateException("Checkpoint record pointed to " + transactionPosition
                    + ", but log commit entry not found at that position. Last checked position: " + checkedPosition);
        } catch (IllegalStateException e) {
            Throwable cause = e;
            // We were not able to read last transaction log file one of the reason can be inability to read full logs
            // because of transactions in legacy formats that are present. Here we try to read pre-checkpoint last
            // commit entry and extract our tx info
            try (var fallbackChannel = logFile.openForVersion(transactionPosition.getLogVersion())) {
                fallbackChannel.position(transactionPosition.getByteOffset() - COMMIT_ENTRY_OFFSET);
                // try to read 44 transaction info
                Optional<TransactionId> transactionInfo44 = tryReadTransactionInfo(fallbackChannel, context, false);
                if (transactionInfo44.isPresent()) {
                    return transactionInfo44.get();
                }
                // try to read earlier 4.x transaction info
                fallbackChannel.position(transactionPosition.getByteOffset() - LEGACY_COMMIT_ENTRY_OFFSET);
                Optional<TransactionId> transactionInfo42 = tryReadTransactionInfo(fallbackChannel, context, true);
                if (transactionInfo42.isPresent()) {
                    return transactionInfo42.get();
                }
            } catch (Exception fe) {
                // fallback was not able to get last tx record
                cause = Exceptions.chain(cause, fe);
            }

            throw new RuntimeException(
                    "Unable to find last transaction in log files. Position: " + transactionPosition, cause);
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Unable to find last transaction in log files. Position: " + transactionPosition, t);
        }
    }

    private static Optional<TransactionId> tryReadTransactionInfo(
            PhysicalLogVersionedStoreChannel fallbackChannel,
            TransactionLogFilesContext context,
            boolean skipChecksum) {
        if (fallbackChannel.getLogFormatVersion().usesSegments()) {
            // This should never be an envelope channel as this is for 4.2-4.4 logs.
            // Returning empty here will make the original exception bubble up.
            return Optional.empty();
        }

        try (var fallbackReader =
                new ReadAheadLogChannel(new UnclosableChannel(fallbackChannel), context.getMemoryTracker())) {
            byte versionCode = fallbackReader.get();
            if (context.getBinarySupportedKernelVersions().latestSupportedIsLessThan(versionCode)) {
                return Optional.empty();
            }
            var kernelVersion = KernelVersion.EARLIEST.isGreaterThan(versionCode)
                    ? KernelVersion.EARLIEST
                    : KernelVersion.getForVersion(versionCode);
            var reverseBytes = kernelVersion.isLessThan(KernelVersion.VERSION_LITTLE_ENDIAN_TX_LOG_INTRODUCED);
            byte entryCode = fallbackReader.get();
            if (entryCode != TX_COMMIT) {
                return Optional.empty();
            }
            long transactionId = maybeReverse(fallbackReader.getLong(), reverseBytes);
            long timeWritten = maybeReverse(fallbackReader.getLong(), reverseBytes);
            int checksum = skipChecksum ? 0 : maybeReverse(fallbackReader.getInt(), reverseBytes);
            return Optional.of(
                    new TransactionId(transactionId, kernelVersion, checksum, timeWritten, UNKNOWN_CONSENSUS_INDEX));
        } catch (Exception e) {
            context.getLogProvider()
                    .getLog(CheckpointInfoFactory.class)
                    .debug("Fail to extract legacy transaction info.", e);
            return Optional.empty();
        }
    }

    private static int maybeReverse(int value, boolean reverseBytes) {
        return reverseBytes ? Integer.reverseBytes(value) : value;
    }

    private static long maybeReverse(long value, boolean reverseBytes) {
        return reverseBytes ? Long.reverseBytes(value) : value;
    }
}
