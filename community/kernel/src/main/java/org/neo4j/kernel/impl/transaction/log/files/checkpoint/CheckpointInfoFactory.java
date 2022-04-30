/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryDetachedCheckpointV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class CheckpointInfoFactory {
    // transaction id - long
    // time written - long
    // checksum - int
    // 2 bytes for version code and entry code
    private static final long COMMIT_ENTRY_OFFSET = 2 * Long.BYTES + Integer.BYTES + 2 * Byte.BYTES;

    public static CheckpointInfo ofLogEntry(
            LogEntry entry,
            LogPosition checkpointEntryPosition,
            LogPosition channelPositionAfterCheckpoint,
            LogPosition checkpointFilePostReadPosition,
            TransactionLogFilesContext context,
            LogFile logFile) {
        if (entry instanceof LogEntryDetachedCheckpointV4_2 checkpoint42) {
            var transactionInfo = readTransactionInfo(context, logFile, checkpoint42.getLogPosition());
            return new CheckpointInfo(
                    checkpoint42.getLogPosition(),
                    checkpoint42.getStoreId(),
                    checkpointEntryPosition,
                    channelPositionAfterCheckpoint,
                    // we need to use kernel version from transaction command since checkpoints were broken in old
                    // version and used incorrect kernel version
                    checkpointFilePostReadPosition,
                    transactionInfo.version(),
                    transactionInfo.transactionId(),
                    checkpoint42.getReason());
        } else if (entry instanceof LogEntryDetachedCheckpointV5_0 checkpoint50) {
            return new CheckpointInfo(
                    checkpoint50.getLogPosition(),
                    checkpoint50.getStoreId(),
                    checkpointEntryPosition,
                    channelPositionAfterCheckpoint,
                    checkpointFilePostReadPosition,
                    checkpoint50.getVersion(),
                    checkpoint50.getTransactionId(),
                    checkpoint50.getReason());
        } else {
            throw new UnsupportedOperationException(
                    "Expected to observe only checkpoint entries, but: `" + entry + "` was found.");
        }
    }

    private static TransactionInfo readTransactionInfo(
            TransactionLogFilesContext context, LogFile logFile, LogPosition transactionPosition) {
        try (var channel = logFile.openForVersion(transactionPosition.getLogVersion());
                var reader = new ReadAheadLogChannel(
                        new UnclosableChannel(channel), NO_MORE_CHANNELS, context.getMemoryTracker());
                var logEntryCursor =
                        new LogEntryCursor(new VersionAwareLogEntryReader(context.getCommandReaderFactory()), reader)) {
            LogPosition checkedPosition = null;
            while (logEntryCursor.next()) {
                LogEntry logEntry = logEntryCursor.get();
                checkedPosition = reader.getCurrentPosition();
                if (logEntry instanceof LogEntryCommit commit && checkedPosition.equals(transactionPosition)) {
                    return new TransactionInfo(
                            new TransactionId(commit.getTxId(), commit.getChecksum(), commit.getTimeWritten()),
                            commit.getVersion());
                }
            }

            // We have a checkpoint on this point but there is no transaction found that match it and log files are
            // corrupted.
            // Database should be restored from the last valid backup or dump in normal circumstances.
            if (!context.getConfig().get(fail_on_corrupted_log_files)) {
                return new TransactionInfo(TransactionIdStore.UNKNOWN_TRANSACTION_ID, KernelVersion.V4_4);
            }
            throw new IllegalStateException("Checkpoint record pointed to " + transactionPosition
                    + ", but log commit entry not found at that position. Last checked position: " + checkedPosition);
        } catch (UnsupportedLogVersionException e) {
            Throwable cause = e;
            // We were not able to read last transaction log file one of the reason can be inability to read full logs
            // because of transactions
            // in legacy formats that are present. Here we try to read pre-checkpoint last commit entry and extract our
            // tx info
            try (var fallbackChannel = logFile.openForVersion(transactionPosition.getLogVersion());
                    var fallbackReader = new ReadAheadLogChannel(
                            new UnclosableChannel(fallbackChannel), NO_MORE_CHANNELS, context.getMemoryTracker())) {
                fallbackChannel.position(transactionPosition.getByteOffset() - COMMIT_ENTRY_OFFSET);
                byte versionCode = fallbackReader.get();
                if (versionCode > KernelVersion.LATEST.version()) {
                    throw new IllegalStateException("Detected unsupported version code: " + versionCode
                            + ", latest supported is: " + KernelVersion.LATEST);
                }
                var kernelVersion = (versionCode < KernelVersion.EARLIEST.version())
                        ? KernelVersion.EARLIEST
                        : KernelVersion.getForVersion(versionCode);
                var reverseBytes = kernelVersion.isLessThan(KernelVersion.VERSION_LITTLE_ENDIAN_TX_LOG_INTRODUCED);
                byte entryCode = fallbackReader.get();
                if (entryCode == TX_COMMIT) {
                    long transactionId = maybeReverse(fallbackReader.getLong(), reverseBytes);
                    long timeWritten = maybeReverse(fallbackReader.getLong(), reverseBytes);
                    int checksum = maybeReverse(fallbackReader.getInt(), reverseBytes);
                    // we may not even have the earliest version, so we select the oldest available
                    return new TransactionInfo(new TransactionId(transactionId, checksum, timeWritten), kernelVersion);
                } else {
                    throw new IllegalStateException("Detected unsupported entry code: " + entryCode);
                }
            } catch (Exception fe) {
                // fallback was not able to get last tx record
                cause = Exceptions.chain(cause, fe);
            }

            throw new RuntimeException(
                    "Unable to find last transaction in log files. Position: " + transactionPosition, cause);
        } catch (IOException ioe) {
            throw new UncheckedIOException(
                    "Unable to find last transaction in log files. Position: " + transactionPosition, ioe);
        }
    }

    private static int maybeReverse(int value, boolean reverseBytes) {
        return reverseBytes ? Integer.reverseBytes(value) : value;
    }

    private static long maybeReverse(long value, boolean reverseBytes) {
        return reverseBytes ? Long.reverseBytes(value) : value;
    }

    private record TransactionInfo(TransactionId transactionId, KernelVersion version) {}
}
