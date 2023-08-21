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

import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.lock.Lock;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

class DatabaseUpgradeTransactionHandler {
    private final DbmsRuntimeRepository dbmsRuntimeRepository;
    private final KernelVersionProvider kernelVersionProvider;
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

    DatabaseUpgradeTransactionHandler(
            DbmsRuntimeRepository dbmsRuntimeRepository,
            KernelVersionProvider kernelVersionProvider,
            DatabaseTransactionEventListeners transactionEventListeners,
            UpgradeLocker locker,
            InternalLogProvider logProvider,
            Config config) {
        this.dbmsRuntimeRepository = dbmsRuntimeRepository;
        this.kernelVersionProvider = kernelVersionProvider;
        this.transactionEventListeners = transactionEventListeners;
        this.locker = locker;
        this.log = logProvider.getLog(this.getClass());
        this.config = config;
    }

    interface InternalUpgradeTransactionHandler {
        void upgrade(KernelVersion from, KernelVersion to) throws TransactionFailureException;
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

    private class DatabaseUpgradeListener extends InternalTransactionEventListener.Adapter<Lock> {
        private final InternalUpgradeTransactionHandler internalUpgradeTransactionHandler;

        DatabaseUpgradeListener(InternalUpgradeTransactionHandler internalUpgradeTransactionHandler) {
            this.internalUpgradeTransactionHandler = internalUpgradeTransactionHandler;
        }

        @Override
        public Lock beforeCommit(TransactionData data, KernelTransaction tx, GraphDatabaseService databaseService)
                throws Exception {
            KernelVersion checkKernelVersion = kernelVersionProvider.kernelVersion();
            if (dbmsRuntimeRepository.getVersion().kernelVersion().isGreaterThan(checkKernelVersion)) {
                try {
                    try (Lock lock = locker.acquireWriteLock(tx)) {

                        KernelVersion kernelVersionToUpgradeTo =
                                dbmsRuntimeRepository.getVersion().kernelVersion();
                        KernelVersion currentKernelVersion = kernelVersionProvider.kernelVersion();
                        if (kernelVersionToUpgradeTo.isGreaterThan(currentKernelVersion)) {
                            log.info(
                                    "Upgrade transaction from %s to %s started",
                                    currentKernelVersion, kernelVersionToUpgradeTo);
                            internalUpgradeTransactionHandler.upgrade(currentKernelVersion, kernelVersionToUpgradeTo);
                            log.info(
                                    "Upgrade transaction from %s to %s completed",
                                    currentKernelVersion, kernelVersionToUpgradeTo);
                        }
                    }
                } catch (LockAcquisitionTimeoutException | DeadlockDetectedException ignore) {
                    // This can happen if there is an ongoing committing transaction waiting for locks held by the
                    // "trigger tx". Let the "trigger tx" continue and try the upgrade again on the next write.
                    log.info(
                            "Upgrade transaction from %s to %s not possible right now due to conflicting transaction, will retry on next write",
                            checkKernelVersion,
                            dbmsRuntimeRepository.getVersion().kernelVersion());
                }
            }
            return locker.acquireReadLock(tx); // This read lock will be released in afterCommit or afterRollback
        }

        @Override
        public void afterCommit(TransactionData data, Lock readLock, GraphDatabaseService databaseService) {
            checkUnlockAndUnregister(readLock);
        }

        @Override
        public void afterRollback(TransactionData data, Lock readLock, GraphDatabaseService databaseService) {
            checkUnlockAndUnregister(readLock);
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
    }
}
