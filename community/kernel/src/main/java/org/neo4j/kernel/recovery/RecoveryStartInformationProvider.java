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
package org.neo4j.kernel.recovery;

import static org.neo4j.kernel.recovery.RecoveryStartInformation.MISSING_LOGS;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.NO_RECOVERY_REQUIRED;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

import java.io.IOException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;

/**
 * Utility class to find the log position to start recovery from
 */
public class RecoveryStartInformationProvider implements ThrowingSupplier<RecoveryStartInformation, IOException> {
    public interface Monitor {
        /**
         * There's a check point log entry that points to the tail of .
         *
         * @param logPosition {@link LogPosition} of the last check point.
         */
        default void recoveryNotRequired(LogPosition logPosition) {}

        /**
         * There's a check point log entry, but there are log entries that need to be replayed
         *
         * @param logPosition {@link LogPosition} pointing to the first log entry after the last
         * check pointed transaction.
         * @param oldestNotVisibleTransactionLogPosition pointing to the oldest log entry before the last check pointed that needs to be recovered.
         * @param appendIndexAfterLastCheckPoint transaction id of the first transaction after the last check point.
         */
        default void recoveryRequiredAfterLastCheckPoint(
                LogPosition logPosition,
                LogPosition oldestNotVisibleTransactionLogPosition,
                long appendIndexAfterLastCheckPoint) {}

        /**
         * No check point log entry found in the transaction log.
         */
        default void noCheckPointFound() {}

        /**
         * Failure to read initial header of initial log file
         */
        default void failToExtractInitialFileHeader(Exception e) {}
    }

    public static final Monitor NO_MONITOR = new Monitor() {};

    private final LogFiles logFiles;
    private final Monitor monitor;

    public RecoveryStartInformationProvider(LogFiles logFiles, Monitor monitor) {
        this.logFiles = logFiles;
        this.monitor = monitor;
    }

    @Override
    public RecoveryStartInformation get() {
        var logTailInformation = (LogTailInformation) logFiles.getTailMetadata();
        CheckpointInfo lastCheckPoint = logTailInformation.lastCheckPoint;
        long appendIndexAfterLastCheckPoint = logTailInformation.firstAppendIndexAfterLastCheckPoint;

        if (!logTailInformation.isRecoveryRequired()) {
            LogPosition logPosition = lastCheckPoint != null ? lastCheckPoint.transactionLogPosition() : null;
            monitor.recoveryNotRequired(logPosition);
            return NO_RECOVERY_REQUIRED;
        }
        if (logTailInformation.logsMissing()) {
            return MISSING_LOGS;
        }
        if (logTailInformation.hasRecordsToRecover()) {
            if (lastCheckPoint == null) {
                return noCheckpointRecordRecoveryInfo(appendIndexAfterLastCheckPoint);
            }
            return checkpointBasedRecoveryInfo(lastCheckPoint, appendIndexAfterLastCheckPoint);
        } else if (logTailInformation.hasUnreadableBytesInCheckpointLogs()) {
            // The problem is just corruption in the checkpoint log file
            return new RecoveryStartInformation(
                    lastCheckPoint.transactionLogPosition(),
                    lastCheckPoint.oldestNotVisibleTransactionLogPosition(),
                    lastCheckPoint,
                    lastCheckPoint.appendIndex());
        } else {
            throw new UnderlyingStorageException(
                    "Fail to determine recovery information Log tail info: " + logTailInformation);
        }
    }

    private RecoveryStartInformation checkpointBasedRecoveryInfo(
            CheckpointInfo lastCheckPoint, long appendIndexAfterLastCheckPoint) {
        LogPosition transactionLogPosition = lastCheckPoint.transactionLogPosition();
        LogPosition oldestNotVisibleTransactionLogPosition = lastCheckPoint.oldestNotVisibleTransactionLogPosition();
        monitor.recoveryRequiredAfterLastCheckPoint(
                transactionLogPosition, oldestNotVisibleTransactionLogPosition, appendIndexAfterLastCheckPoint);
        return new RecoveryStartInformation(
                transactionLogPosition,
                oldestNotVisibleTransactionLogPosition,
                lastCheckPoint,
                appendIndexAfterLastCheckPoint);
    }

    private RecoveryStartInformation noCheckpointRecordRecoveryInfo(long appendIndexAfterLastCheckPoint) {
        long lowestLogVersion = logFiles.getLogFile().getLowestLogVersion();
        if (lowestLogVersion != INITIAL_LOG_VERSION) {
            throw new UnderlyingStorageException("No check point found in any log file and transaction log "
                    + "files do not exist from expected version " + INITIAL_LOG_VERSION
                    + ". Lowest found log file is "
                    + lowestLogVersion + ".");
        }
        monitor.noCheckPointFound();
        LogPosition position = tryExtractHeaderAndGetStartPosition();
        return new RecoveryStartInformation(position, position, null, appendIndexAfterLastCheckPoint);
    }

    private LogPosition tryExtractHeaderAndGetStartPosition() {
        try {
            return logFiles.getLogFile().extractHeader(INITIAL_LOG_VERSION).getStartPosition();
        } catch (IOException e) {
            monitor.failToExtractInitialFileHeader(e);
            throw new UnderlyingStorageException(
                    "Unable to read header from log file with version " + INITIAL_LOG_VERSION, e);
        }
    }
}
