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
package org.neo4j.kernel.recovery;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.recovery.Recovery.throwUnableToCleanRecover;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

import java.nio.channels.ClosedByInterruptException;
import org.neo4j.common.ProgressReporter;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Stopwatch;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link Database}.
 */
public class TransactionLogsRecovery extends LifecycleAdapter {
    private static final String REVERSE_RECOVERY_TAG = "restoreDatabase";
    private static final String RECOVERY_TAG = "recoverDatabase";
    private static final String RECOVERY_COMPLETED_TAG = "databaseRecoveryCompleted";

    private final RecoveryService recoveryService;
    private final RecoveryMonitor monitor;
    private final CorruptedLogsTruncator logsTruncator;
    private final Lifecycle schemaLife;
    private final ProgressReporter progressReporter;
    private final boolean failOnCorruptedLogFiles;
    private final RecoveryStartupChecker recoveryStartupChecker;
    private final CursorContextFactory contextFactory;
    private final RecoveryPredicate recoveryPredicate;

    public TransactionLogsRecovery(
            RecoveryService recoveryService,
            CorruptedLogsTruncator logsTruncator,
            Lifecycle schemaLife,
            RecoveryMonitor monitor,
            ProgressReporter progressReporter,
            boolean failOnCorruptedLogFiles,
            RecoveryStartupChecker recoveryStartupChecker,
            RecoveryPredicate recoveryPredicate,
            CursorContextFactory contextFactory) {
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.logsTruncator = logsTruncator;
        this.schemaLife = schemaLife;
        this.progressReporter = progressReporter;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.recoveryStartupChecker = recoveryStartupChecker;
        this.contextFactory = contextFactory;
        this.recoveryPredicate = recoveryPredicate;
    }

    @Override
    public void init() throws Exception {
        RecoveryStartInformation recoveryStartInformation = recoveryService.getRecoveryStartInformation();
        if (!recoveryStartInformation.isRecoveryRequired()) {
            schemaLife.init();
            return;
        }

        Stopwatch recoveryStartTime = Stopwatch.start();

        LogPosition recoveryStartPosition = recoveryStartInformation.getTransactionLogPosition();

        monitor.recoveryRequired(recoveryStartPosition);

        LogPosition recoveryToPosition = recoveryStartPosition;
        LogPosition lastTransactionPosition = recoveryStartPosition;
        CommittedCommandBatch lastCommandBatch = null;
        CommittedCommandBatch lastReversedCommandBatch = null;
        if (!recoveryStartInformation.isMissingLogs()) {
            try {
                long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
                try (var transactionsToRecover =
                                recoveryService.getCommandBatchesInReverseOrder(recoveryStartPosition);
                        var recoveryVisitor = recoveryService.getRecoveryApplier(
                                REVERSE_RECOVERY, contextFactory, REVERSE_RECOVERY_TAG)) {
                    while (transactionsToRecover.next()) {
                        recoveryStartupChecker.checkIfCanceled();
                        CommittedCommandBatch commandBatch = transactionsToRecover.get();
                        if (lastReversedCommandBatch == null) {
                            lastReversedCommandBatch = commandBatch;
                            initProgressReporter(recoveryStartInformation, lastReversedCommandBatch);
                        }
                        recoveryVisitor.visit(commandBatch);
                        lowestRecoveredTxId = commandBatch.txId();
                        reportProgress();
                    }
                }

                monitor.reverseStoreRecoveryCompleted(lowestRecoveredTxId);

                // We cannot initialise the schema (tokens, schema cache, indexing service, etc.) until we have returned
                // the store to a consistent state.
                // We need to be able to read the store before we can even figure out what indexes, tokens, etc. we
                // have. Hence, we defer the initialisation
                // of the schema life until after we've done the reverse recovery.
                schemaLife.init();

                boolean fullRecovery = true;
                try (var transactionsToRecover = recoveryService.getCommandBatches(recoveryStartPosition);
                        var recoveryVisitor =
                                recoveryService.getRecoveryApplier(RECOVERY, contextFactory, RECOVERY_TAG)) {
                    while (fullRecovery && transactionsToRecover.next()) {
                        var nextCommandBatch = transactionsToRecover.get();
                        if (!recoveryPredicate.test(nextCommandBatch)) {
                            monitor.partialRecovery(recoveryPredicate, lastCommandBatch);
                            fullRecovery = false;
                            if (lastCommandBatch == null) {
                                // First transaction after checkpoint failed predicate test
                                // we can't always load transaction before checkpoint to check what values we had there
                                // since those logs may be pruned,
                                // but we will try to load first transaction before checkpoint to see if we just on the
                                // edge of provided criteria
                                // and will fail otherwise.
                                long beforeCheckpointTransaction =
                                        recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() - 1;
                                if (beforeCheckpointTransaction < TransactionIdStore.BASE_TX_ID) {
                                    throw new RecoveryPredicateException(format(
                                            "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching to provided "
                                                    + "criteria found and transaction before checkpoint is not valid. "
                                                    + "Transaction id before checkpoint: %d, criteria %s.",
                                            beforeCheckpointTransaction, recoveryPredicate.describe()));
                                }
                                try (var beforeCheckpointCursor =
                                        recoveryService.getCommandBatches(beforeCheckpointTransaction)) {
                                    if (beforeCheckpointCursor.next()) {
                                        CommittedCommandBatch candidate = beforeCheckpointCursor.get();
                                        if (!recoveryPredicate.test(candidate)) {
                                            throw new RecoveryPredicateException(format(
                                                    "Partial recovery criteria can't be satisfied. "
                                                            + "Transaction after and before checkpoint does not satisfy provided recovery criteria. "
                                                            + "Observed transaction id: %d, recovery criteria: %s.",
                                                    candidate.txId(), recoveryPredicate.describe()));
                                        }
                                        lastCommandBatch = candidate;
                                        lastTransactionPosition = beforeCheckpointCursor.position();
                                    } else {
                                        throw new RecoveryPredicateException(format(
                                                "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching "
                                                        + "to provided criteria found and transaction before checkpoint not found. Recovery criteria: %s.",
                                                recoveryPredicate.describe()));
                                    }
                                } catch (RecoveryPredicateException re) {
                                    throw re;
                                } catch (Exception e) {
                                    throw new RecoveryPredicateException(
                                            format(
                                                    "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching "
                                                            + "to provided criteria found and fail to read transaction before checkpoint. Recovery criteria: %s.",
                                                    recoveryPredicate.describe()),
                                            e);
                                }
                            }
                        } else {
                            recoveryStartupChecker.checkIfCanceled();
                            recoveryVisitor.visit(nextCommandBatch);

                            if (lastCommandBatch == null || lastCommandBatch.txId() < nextCommandBatch.txId()) {
                                lastCommandBatch = nextCommandBatch;
                            }
                            monitor.batchRecovered(nextCommandBatch);
                            lastTransactionPosition = transactionsToRecover.position();
                            recoveryToPosition = lastTransactionPosition;
                            reportProgress();
                        }
                    }
                    recoveryToPosition = fullRecovery ? transactionsToRecover.position() : lastTransactionPosition;
                }
            } catch (Error
                    | ClosedByInterruptException
                    | DatabaseStartAbortedException
                    | RecoveryPredicateException e) {
                // We do not want to truncate logs based on these exceptions. Since users can influence them with config
                // changes
                // the users are able to workaround this if truncations is really needed.
                throw e;
            } catch (Throwable t) {
                if (failOnCorruptedLogFiles) {
                    throwUnableToCleanRecover(t);
                }
                if (lastCommandBatch != null) {
                    monitor.failToRecoverTransactionsAfterCommit(t, lastCommandBatch, recoveryToPosition);
                } else {
                    monitor.failToRecoverTransactionsAfterPosition(t, recoveryStartPosition);
                }
            }
            progressReporter.completed();
            logsTruncator.truncate(recoveryToPosition);
        }

        try (var cursorContext = contextFactory.create(RECOVERY_COMPLETED_TAG)) {
            final boolean missingLogs = recoveryStartInformation.isMissingLogs();
            recoveryService.transactionsRecovered(
                    lastCommandBatch,
                    lastTransactionPosition,
                    recoveryToPosition,
                    recoveryStartInformation.getCheckpointPosition(),
                    missingLogs,
                    cursorContext);
        }
        monitor.recoveryCompleted(recoveryStartTime.elapsed(MILLISECONDS));
    }

    private void initProgressReporter(
            RecoveryStartInformation recoveryStartInformation, CommittedCommandBatch lastReversedBatch) {
        long numberOfTransactionToRecover =
                estimateNumberOfTransactionToRecover(recoveryStartInformation, lastReversedBatch);
        // since we will process each transaction twice (doing reverse and direct detour) we need to
        // multiply number of transactions that we want to recover by 2 to be able to report correct progress
        progressReporter.start(numberOfTransactionToRecover * 2);
    }

    private void reportProgress() {
        progressReporter.progress(1);
    }

    private static long estimateNumberOfTransactionToRecover(
            RecoveryStartInformation recoveryStartInformation, CommittedCommandBatch lastReversedCommandBatch) {
        return lastReversedCommandBatch.txId() - recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() + 1;
    }

    @Override
    public void start() throws Exception {
        schemaLife.start();
    }

    @Override
    public void stop() throws Exception {
        schemaLife.stop();
    }

    @Override
    public void shutdown() throws Exception {
        schemaLife.shutdown();
    }
}
