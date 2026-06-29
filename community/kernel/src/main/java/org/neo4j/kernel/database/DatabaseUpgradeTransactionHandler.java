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
package org.neo4j.kernel.database;

import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.api.TransactionIdSequence.TRANSACTION_SEQUENCE_INITIAL_VALUE;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.MaximumTransactionLimitExceededException;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.lock.Lock;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

class DatabaseUpgradeTransactionHandler {
    private final DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider;
    private final KernelVersionProvider kernelVersionProvider;
    private final LogFormatVersionProvider logFormatVersionProvider;
    private final DatabaseTransactionEventListeners transactionEventListeners;
    private final AtomicBoolean unregistered = new AtomicBoolean();

    // This lock is used to make the produced upgrade transaction a strong barrier such that there can be no transaction
    // of an older version committed after this barrier. Consider this scenario:
    // - DBMS runtime version is at 1
    // - Transaction A goes into commit, checks need to upgrade by this listener before generating commands
    // - Transaction A generates commands (with DBMS runtime version 1)
    // - DBMS runtime version is set to 2
    // - Transaction B commits, notices the upgraded DBMS runtime version and performs the upgrade transaction
    // - Transaction A appends to the log and applies
    // - Transaction B performs its own commit
    //
    // The above scenario would produce this transaction log stream:
    // - Upgrade transaction (1 -> 2)
    // - Transaction A (version 1)
    // - Transaction B (version 2)
    //
    // I.e. the upgrade transaction wouldn't be a strong barrier. This lock prevents this scenario.
    private final UpgradeLocker locker;
    private final InternalLog log;
    private final Config config;
    private final KernelImpl kernelApi;
    private final KernelTransactions kernelTransactions;
    private final boolean multiversioned;
    private final boolean raftTriggersUpgrade;

    DatabaseUpgradeTransactionHandler(
            DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider,
            KernelVersionProvider kernelVersionProvider,
            LogFormatVersionProvider logFormatVersionProvider,
            DatabaseTransactionEventListeners transactionEventListeners,
            UpgradeLocker locker,
            InternalLogProvider logProvider,
            Config config,
            KernelImpl kernelApi,
            KernelTransactions kernelTransactions,
            boolean multiversioned,
            boolean raftTriggersUpgrade) {
        this.dbmsRuntimeVersionProvider = dbmsRuntimeVersionProvider;
        this.kernelVersionProvider = kernelVersionProvider;
        this.logFormatVersionProvider = logFormatVersionProvider;
        this.transactionEventListeners = transactionEventListeners;
        this.locker = locker;
        this.log = logProvider.getLog(this.getClass());
        this.config = config;
        this.kernelApi = kernelApi;
        this.kernelTransactions = kernelTransactions;
        this.multiversioned = multiversioned;
        // If raft triggers upgrade this handler only takes read locks to prevent transactions committing concurrently
        // with upgrade
        this.raftTriggersUpgrade = raftTriggersUpgrade;
    }

    interface InternalUpgradeTransactionHandler {
        void upgrade(KernelVersion from, KernelVersion to, KernelTransaction tx, LogFormat currentLogFormat)
                throws TransactionFailureException;
    }

    /**
     * The general idea here is to register a transaction listener only if we are in a pending upgrade scenario.
     * The listener will before any other committed transaction create an "internal" transaction bringing the database up to date with the system database.
     * This transaction will act as a recoverable and safe "barrier" after which it is safe to use the new transaction log version.
     * The new version will then be used for the same transaction that triggered this upgrade in the first place.
     * On success it will unregister itself to get rid of any potential overhead during normal operation.
     *
     * In the rare event of the "internal" upgrade transaction failing, it will stay on the old version and fail all transactions for this db
     * until it succeeds.
     */
    void registerUpgradeListener(InternalUpgradeTransactionHandler internalUpgradeTransactionHandler) {
        if (!kernelVersionProvider.kernelVersion().isLatest(config)) {
            transactionEventListeners.registerTransactionEventListener(
                    new DatabaseUpgradeListener(internalUpgradeTransactionHandler));
        }
    }

    class DatabaseUpgradeListener extends InternalTransactionEventListener.Adapter<Lock> {
        private final InternalUpgradeTransactionHandler internalUpgradeTransactionHandler;
        private volatile long upgradeTransactionSequenceNumber = TRANSACTION_SEQUENCE_INITIAL_VALUE;
        private final MultiVersionUpgradeGate multiVersionUpgradeGate = new MultiVersionUpgradeGate(kernelTransactions);

        DatabaseUpgradeListener(InternalUpgradeTransactionHandler internalUpgradeTransactionHandler) {
            this.internalUpgradeTransactionHandler = internalUpgradeTransactionHandler;
        }

        @Override
        public Lock beforeCommit(TransactionData data, KernelTransaction tx, GraphDatabaseService databaseService)
                throws Exception {
            KernelVersion checkKernelVersion = kernelVersionProvider.kernelVersion();
            if (!raftTriggersUpgrade
                    && dbmsRuntimeVersionProvider.getVersion().kernelVersion().isGreaterThan(checkKernelVersion)) {
                try {
                    // multi version dbs should allow earlier transactions to complete before upgrade
                    if (multiversioned && tx.getTransactionSequenceNumber() < upgradeTransactionSequenceNumber) {
                        return null;
                    }
                    if (tx.getTransactionSequenceNumber() == upgradeTransactionSequenceNumber) {
                        // Don't block the transaction we created to do the upgrade
                        return null;
                    }
                    if (multiversioned) {
                        // this is not the last commit call so we do not do anything atm
                        if (!data.isLast()) {
                            return null;
                        }
                        if (!multiVersionUpgradeGate.upgradeGate(tx)) {
                            return null;
                        }
                        try {
                            tryUpgradeKernelVersion();
                        } finally {
                            multiVersionUpgradeGate.release();
                        }
                    } else {
                        try (Lock lock = locker.acquireWriteLock(tx)) {
                            tryUpgradeKernelVersion();
                        }
                    }
                } catch (LockAcquisitionTimeoutException | DeadlockDetectedException ignore) {
                    // This can happen if there is an ongoing committing transaction waiting for locks held by the
                    // "trigger tx". Let the "trigger tx" continue and try the upgrade again on the next write.
                    log.info(
                            "Upgrade transaction from %s to %s not possible right now due to conflicting transaction, will retry on next write",
                            checkKernelVersion,
                            dbmsRuntimeVersionProvider.getVersion().kernelVersion());
                } catch (MaximumTransactionLimitExceededException e) {
                    // This can happen even though we drain transactions because we do not drain reads.
                    // Let the "trigger tx" continue and try the upgrade again on the next write.
                    log.error(
                            "Upgrade transaction from %s to %s not possible right now because maximum concurrently "
                                    + "executed transactions was reached, will retry on next write."
                                    + " If this persists see setting %s.",
                            checkKernelVersion,
                            dbmsRuntimeVersionProvider.getVersion().kernelVersion(),
                            max_concurrent_transactions.name());
                } catch (Exception e) {
                    log.info(
                            "Upgrade transaction from %s to %s not possible right now due exception with message: '%s', will retry on next write",
                            checkKernelVersion,
                            dbmsRuntimeVersionProvider.getVersion().kernelVersion(),
                            e);
                    throw e;
                }
            }
            return locker.acquireReadLock(tx); // This read lock will be released in afterCommit or afterRollback
        }

        private void tryUpgradeKernelVersion() throws TransactionFailureException {
            KernelVersion kernelVersionToUpgradeTo =
                    dbmsRuntimeVersionProvider.getVersion().kernelVersion();
            KernelVersion currentKernelVersion = kernelVersionProvider.kernelVersion();
            if (kernelVersionToUpgradeTo.isGreaterThan(currentKernelVersion)) {
                log.info("Upgrade transaction from %s to %s started", currentKernelVersion, kernelVersionToUpgradeTo);
                try (KernelTransaction upgradeTx =
                        kernelApi.beginTransaction(KernelTransaction.Type.IMPLICIT, AUTH_DISABLED)) {
                    // Save a reference to this tx and let it through beforeCommit
                    upgradeTransactionSequenceNumber = upgradeTx.getTransactionSequenceNumber();
                    internalUpgradeTransactionHandler.upgrade(
                            currentKernelVersion,
                            kernelVersionToUpgradeTo,
                            upgradeTx,
                            logFormatVersionProvider.getCurrentLogFormat());
                    upgradeTx.commit();
                } finally {
                    upgradeTransactionSequenceNumber = TRANSACTION_SEQUENCE_INITIAL_VALUE;
                }
                log.info("Upgrade transaction from %s to %s completed", currentKernelVersion, kernelVersionToUpgradeTo);
            }
        }

        @Override
        public void afterCommit(TransactionData data, Lock readLock, GraphDatabaseService databaseService) {
            checkUnlockAndUnregister(readLock);
        }

        @Override
        public void afterRollback(TransactionData data, Lock readLock, GraphDatabaseService databaseService) {
            checkUnlockAndUnregister(readLock);
        }

        @Override
        public Set<TransactionData.DataSelection> transactionDataSelection() {
            return Collections.emptySet();
        }

        private void checkUnlockAndUnregister(Lock readLock) {
            // For some reason the transaction event listeners handling is such that even if beforeCommit fails for this
            // listener then an afterRollback will be called. Therefore, we distinguish between success and failure
            // using the state (which is the lock)
            if (readLock == null) {
                return;
            }

            readLock.close();
            if (kernelVersionProvider.kernelVersion().isLatest(config) && unregistered.compareAndSet(false, true)) {
                try {
                    transactionEventListeners.unregisterTransactionEventListener(this);
                } catch (Throwable e) {
                    unregistered.set(false);
                    throw e;
                }
            }
        }

        static class MultiVersionUpgradeGate {
            private static final int INITIAL_VALUE = 0;
            private final AtomicLong upgradeLock = new AtomicLong();
            private final KernelTransactions kernelTransactions;

            MultiVersionUpgradeGate(KernelTransactions kernelTransactions) {
                this.kernelTransactions = kernelTransactions;
            }

            void release() {
                upgradeLock.setRelease(INITIAL_VALUE);
            }

            boolean upgradeGate(KernelTransaction tx) {
                long transactionSequenceNumber = tx.getTransactionSequenceNumber();
                do {
                    long currentValue = upgradeLock.getAcquire();
                    if (currentValue != INITIAL_VALUE) {
                        if (currentValue > transactionSequenceNumber) {
                            return false;
                        }
                        // we are in a transaction that should wait
                        LockSupport.parkNanos(100);
                    }
                } while (!upgradeLock.weakCompareAndSetRelease(INITIAL_VALUE, transactionSequenceNumber));

                while (!oldTransactionCompleted(transactionSequenceNumber)) {
                    LockSupport.parkNanos(100);
                }

                // we are the transaction that won upgrade lock race but we are multi chunked so we will let someone
                // else to win
                // the race and do the upgrade instead
                if (((KernelTransactionImplementation) tx).txState().isMultiChunk()) {
                    release();
                    return false;
                }
                return true;
            }

            private boolean oldTransactionCompleted(long currentValue) {
                return kernelTransactions.earliestTransactionSequenceNumber() >= currentValue;
            }
        }
    }
}
