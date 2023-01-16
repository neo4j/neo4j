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
package org.neo4j.kernel.impl.transaction.log;

import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.helpers.Format.duration;

import java.nio.file.Path;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.kernel.recovery.RecoveryPredicate;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider;
import org.neo4j.logging.InternalLog;

public class LoggingLogFileMonitor
        implements RecoveryMonitor, RecoveryStartInformationProvider.Monitor, LogRotationMonitor {
    private static final int UNKNOWN_TRANSACTION = -1;
    private long minRecoveredTransaction = UNKNOWN_TRANSACTION;
    private long maxTransactionRecovered = -1;
    private final InternalLog log;
    private int numberOfRecoveredTransactions;
    private int numberOfRolledbackTransactions;

    public LoggingLogFileMonitor(InternalLog log) {
        this.log = log;
    }

    @Override
    public void recoveryRequired(LogPosition startPosition) {
        log.info("Recovery required from position " + startPosition);
    }

    @Override
    public void recoveryCompleted(long recoveryTimeInMilliseconds) {
        log.info(format(
                "Recovery completed. %d transactions applied [first:%s, last:%s], %d transactions rolled back. Time spent: %s.",
                numberOfRecoveredTransactions,
                valueOrDefault(minRecoveredTransaction),
                valueOrDefault(maxTransactionRecovered),
                numberOfRolledbackTransactions,
                duration(recoveryTimeInMilliseconds)));
    }

    @Override
    public void failToRecoverTransactionsAfterCommit(
            Throwable t, CommittedCommandBatch commandBatch, LogPosition recoveryToPosition) {
        log.warn(
                format(
                        "Fail to recover database. Highest recovered transaction id:%d, committed "
                                + "at:%d. Any transactional logs after position %s can not be recovered and will be truncated.",
                        commandBatch.txId(), commandBatch.timeWritten(), recoveryToPosition),
                t);
    }

    @Override
    public void partialRecovery(RecoveryPredicate recoveryPredicate, CommittedCommandBatch commandBatch) {
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
        if (minRecoveredTransaction == -1) {
            minRecoveredTransaction = committedBatch.txId();
        }
        maxTransactionRecovered = Math.max(maxTransactionRecovered, committedBatch.txId());
        if (committedBatch.commandBatch().isLast()) {
            numberOfRecoveredTransactions++;
        }
    }

    @Override
    public void batchRolledback(CommittedCommandBatch committedBatch) {
        if (committedBatch.commandBatch().isFirst()) {
            numberOfRolledbackTransactions++;
        }
    }

    @Override
    public void noCommitsAfterLastCheckPoint(LogPosition logPosition) {
        log.info(format(
                "No commits found after last check point (which is at %s)",
                logPosition != null ? logPosition.toString() : "<no log position given>"));
    }

    @Override
    public void logsAfterLastCheckPoint(LogPosition logPosition, long firstTxIdAfterLastCheckPoint) {
        log.info(format(
                "Transaction logs entries found after the last check point (which is at %s). First observed transaction id: %d.",
                logPosition, firstTxIdAfterLastCheckPoint));
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
            Path logFile, long logVersion, long lastTransactionId, long rotationMillis, long millisSinceLastRotation) {
        StringBuilder sb = new StringBuilder("Rotated to transaction log [");
        sb.append(logFile).append("] version=").append(logVersion).append(", last transaction in previous log=");
        sb.append(lastTransactionId)
                .append(", rotation took ")
                .append(rotationMillis)
                .append(" millis");
        if (millisSinceLastRotation > 0) {
            sb.append(", started after ").append(millisSinceLastRotation).append(" millis");
        }
        log.info(sb.append('.').toString());
    }

    private static String valueOrDefault(long value) {
        return value == UNKNOWN_TRANSACTION ? "None" : String.valueOf(value);
    }

    private static String describeBatch(CommittedCommandBatch commandBatch) {
        if (commandBatch == null) {
            return "Not found.";
        }
        return "transaction id: " + commandBatch.txId() + ", time " + date(ofEpochMilli(commandBatch.timeWritten()));
    }
}
