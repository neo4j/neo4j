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

import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.helpers.Format.duration;

import java.nio.file.Path;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.recovery.RecoveryMode;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.kernel.recovery.RecoveryPredicate;
import org.neo4j.kernel.recovery.RecoveryStartInformation;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider;
import org.neo4j.logging.InternalLog;

public class LoggingLogFileMonitor
        implements RecoveryMonitor, RecoveryStartInformationProvider.Monitor, LogRotationMonitor {
    private long minObservedTransaction = Long.MAX_VALUE;
    private long maxObservedTransaction = Long.MIN_VALUE;
    private final InternalLog log;
    private int numberOfRecoveredTransactions;
    private int skippedFirstBatches;
    private int observedRollbacks;

    public LoggingLogFileMonitor(InternalLog log) {
        this.log = log;
    }

    @Override
    public void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {
        log.info("Recovery required from position " + recoveryStartInfo.transactionLogPosition());
    }

    @Override
    public void recoveryCompleted(long recoveryTimeInMilliseconds, RecoveryMode mode) {
        log.info(format(
                "Recovery in '%s' mode completed. Observed transactions range [first:%s, last:%s]: %d transactions applied, "
                        + "%d not completed transactions rolled back, "
                        + "skipped applying %d previously rolled back transactions. Time spent: %s.",
                mode.description(),
                valueOrDefault(minObservedTransaction, Long.MAX_VALUE),
                valueOrDefault(maxObservedTransaction, Long.MIN_VALUE),
                numberOfRecoveredTransactions,
                numberOfRecoveredTransactions(),
                observedRollbacks,
                duration(recoveryTimeInMilliseconds)));
    }

    @Override
    public void failToRecoverTransactionsAfterCommit(
            Throwable t, CommittedCommandBatch.BatchInformation commandBatch, LogPosition recoveryToPosition) {
        log.warn(
                format(
                        "Fail to recover database. Highest recovered transaction id:%d, committed "
                                + "at:%d. Any transactional logs after position %s can not be recovered and will be truncated.",
                        commandBatch.txId(), commandBatch.timeWritten(), recoveryToPosition),
                t);
    }

    @Override
    public void partialRecovery(
            RecoveryPredicate recoveryPredicate, CommittedCommandBatch.BatchInformation commandBatch) {
        log.info("Partial database recovery based on provided criteria: " + recoveryPredicate.describe()
                + ". Last replayed transaction: " + describeBatch(commandBatch) + ".");
    }

    @Override
    public void failToRecoverTransactionsAfterPosition(Throwable t, LogPosition recoveryFromPosition) {
        log.warn(
                format(
                        "Fail to recover database. Any transactional logs after position %s can not be recovered and will be truncated.",
                        recoveryFromPosition),
                t);
    }

    @Override
    public void failToExtractInitialFileHeader(Exception e) {
        log.warn("Fail to read initial transaction log file header.", e);
    }

    @Override
    public void batchRecovered(CommittedCommandBatch committedBatch) {
        trackTxId(committedBatch.txId());
        if (committedBatch.commandBatch().isLast()) {
            numberOfRecoveredTransactions++;
        }
    }

    @Override
    public void batchApplySkipped(CommittedCommandBatch committedBatch) {
        trackTxId(committedBatch.txId());
        if (committedBatch.commandBatch().isFirst()) {
            skippedFirstBatches++;
        }
        if (committedBatch.isRollback()) {
            observedRollbacks++;
        }
    }

    @Override
    public void recoveryNotRequired(LogPosition logPosition) {
        log.info(format(
                "No commits found after last check point (which is at %s)",
                logPosition != null ? logPosition.toString() : "<no log position given>"));
    }

    @Override
    public void recoveryRequiredAfterLastCheckPoint(
            LogPosition logPosition,
            LogPosition oldestNotVisibleTransactionLogPosition,
            long appendIndexAfterLastCheckPoint) {
        log.info(format(
                "Transaction logs recovery is required with the last check point (which points to %s, oldest log entry to recover %s). First observed post checkpoint append index: %d.",
                logPosition, oldestNotVisibleTransactionLogPosition, appendIndexAfterLastCheckPoint));
    }

    @Override
    public void noCheckPointFound() {
        log.info("No check point found in transaction log.");
    }

    @Override
    public void started(Path logFile, long logVersion) {
        log.info("Starting transaction log [%s] at version=%d", logFile, logVersion);
    }

    @Override
    public void startRotation(long currentLogVersion) {}

    @Override
    public void finishLogRotation(
            Path logFile, long logVersion, long lastAppendIndex, long rotationMillis, long millisSinceLastRotation) {
        StringBuilder sb = new StringBuilder("Rotated to transaction log [");
        sb.append(logFile).append("] version=").append(logVersion).append(", last append index in previous log=");
        sb.append(lastAppendIndex)
                .append(", rotation took ")
                .append(rotationMillis)
                .append(" millis");
        if (millisSinceLastRotation > 0) {
            sb.append(", started after ").append(millisSinceLastRotation).append(" millis");
        }
        log.info(sb.append('.').toString());
    }

    private void trackTxId(long txId) {
        minObservedTransaction = Math.min(minObservedTransaction, txId);
        maxObservedTransaction = Math.max(maxObservedTransaction, txId);
    }

    private int numberOfRecoveredTransactions() {
        return skippedFirstBatches - observedRollbacks;
    }

    private static String valueOrDefault(long value, long unknownValue) {
        return value == unknownValue ? "None" : String.valueOf(value);
    }

    private static String describeBatch(CommittedCommandBatch.BatchInformation commandBatch) {
        if (commandBatch == null) {
            return "Not found.";
        }
        return "transaction id: " + commandBatch.txId() + ", time " + date(ofEpochMilli(commandBatch.timeWritten()));
    }
}
