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
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
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
    private int numberOfRecoveredTransactions;

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
        CommittedTransactionRepresentation lastTransaction = null;
        CommittedTransactionRepresentation lastReversedTransaction = null;
        if (!recoveryStartInformation.isMissingLogs()) {
            try {
                long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
                try (var transactionsToRecover = recoveryService.getTransactionsInReverseOrder(recoveryStartPosition);
                        var recoveryVisitor = recoveryService.getRecoveryApplier(
                                REVERSE_RECOVERY, contextFactory, REVERSE_RECOVERY_TAG)) {
                    while (transactionsToRecover.next()) {
                        recoveryStartupChecker.checkIfCanceled();
                        CommittedTransactionRepresentation transaction = transactionsToRecover.get();
                        if (lastReversedTransaction == null) {
                            lastReversedTransaction = transaction;
                            initProgressReporter(recoveryStartInformation, lastReversedTransaction);
                        }
                        recoveryVisitor.visit(transaction);
                        lowestRecoveredTxId = transaction.getCommitEntry().getTxId();
                        reportProgress();
                    }
                }

                monitor.reverseStoreRecoveryCompleted(lowestRecoveredTxId);

                // We cannot initialise the schema (tokens, schema cache, indexing service, etc.) until we have returned
                // the store to a consistent state.
                // We need to be able to read the store before we can even figure out what indexes, tokens, etc. we
                // have. Hence we defer the initialisation
                // of the schema life until after we've done the reverse recovery.
                schemaLife.init();

                boolean fullRecovery = true;
                try (var transactionsToRecover = recoveryService.getTransactions(recoveryStartPosition);
                        var recoveryVisitor =
                                recoveryService.getRecoveryApplier(RECOVERY, contextFactory, RECOVERY_TAG)) {
                    while (fullRecovery && transactionsToRecover.next()) {
                        var nextTransaction = transactionsToRecover.get();
                        if (!recoveryPredicate.test(nextTransaction)) {
                            monitor.partialRecovery(recoveryPredicate, lastTransaction);
                            fullRecovery = false;
                            if (lastTransaction == null) {
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
                                        recoveryService.getTransactions(beforeCheckpointTransaction)) {
                                    if (beforeCheckpointCursor.next()) {
                                        CommittedTransactionRepresentation candidate = beforeCheckpointCursor.get();
                                        if (!recoveryPredicate.test(candidate)) {
                                            throw new RecoveryPredicateException(format(
                                                    "Partial recovery criteria can't be satisfied. "
                                                            + "Transaction after and before checkpoint does not satisfy provided recovery criteria. "
                                                            + "Observed transaction id: %d, recovery criteria: %s.",
                                                    candidate.getCommitEntry().getTxId(),
                                                    recoveryPredicate.describe()));
                                        }
                                        lastTransaction = candidate;
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
                            long txId = nextTransaction.getCommitEntry().getTxId();
                            recoveryVisitor.visit(nextTransaction);

                            lastTransaction = nextTransaction;
                            monitor.transactionRecovered(txId);
                            numberOfRecoveredTransactions++;
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
                if (lastTransaction != null) {
                    LogEntryCommit commitEntry = lastTransaction.getCommitEntry();
                    monitor.failToRecoverTransactionsAfterCommit(t, commitEntry, recoveryToPosition);
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
                    lastTransaction,
                    lastTransactionPosition,
                    recoveryToPosition,
                    recoveryStartInformation.getCheckpointPosition(),
                    missingLogs,
                    cursorContext);
        }
        monitor.recoveryCompleted(numberOfRecoveredTransactions, recoveryStartTime.elapsed(MILLISECONDS));
    }

    private void initProgressReporter(
            RecoveryStartInformation recoveryStartInformation,
            CommittedTransactionRepresentation lastReversedTransaction) {
        long numberOfTransactionToRecover =
                getNumberOfTransactionToRecover(recoveryStartInformation, lastReversedTransaction);
        // since we will process each transaction twice (doing reverse and direct detour) we need to
        // multiply number of transactions that we want to recover by 2 to be able to report correct progress
        progressReporter.start(numberOfTransactionToRecover * 2);
    }

    private void reportProgress() {
        progressReporter.progress(1);
    }

    private static long getNumberOfTransactionToRecover(
            RecoveryStartInformation recoveryStartInformation,
            CommittedTransactionRepresentation lastReversedTransaction) {
        return lastReversedTransaction.getCommitEntry().getTxId()
                - recoveryStartInformation.getFirstTxIdAfterLastCheckPoint()
                + 1;
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
