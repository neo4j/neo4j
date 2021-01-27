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
package org.neo4j.kernel.database;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;

class DatabaseUpgradeTransactionHandler
{
    private static final Object LISTENER_STATE = new Object();

    private final StorageEngine storageEngine;
    private final DbmsRuntimeRepository dbmsRuntimeRepository;
    private final KernelVersionRepository kernelVersionRepository;
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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    DatabaseUpgradeTransactionHandler( StorageEngine storageEngine, DbmsRuntimeRepository dbmsRuntimeRepository,
            KernelVersionRepository kernelVersionRepository, DatabaseTransactionEventListeners transactionEventListeners )
    {
        this.storageEngine = storageEngine;
        this.dbmsRuntimeRepository = dbmsRuntimeRepository;
        this.kernelVersionRepository = kernelVersionRepository;
        this.transactionEventListeners = transactionEventListeners;
    }

    interface InternalTransactionCommitHandler
    {
        void commit( List<StorageCommand> commands ) throws TransactionFailureException;
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
    void registerUpgradeListener( InternalTransactionCommitHandler internalTransactionCommitHandler )
    {
        DbmsRuntimeVersion startupRuntimeVersion = dbmsRuntimeRepository.getVersion();
        KernelVersion startupKernelVersion = kernelVersionRepository.kernelVersion();
        if ( startupKernelVersion.isGreaterThan( startupRuntimeVersion.kernelVersion() ) )
        {
            // This database has a higher runtime version than the system db. Reasonable scenarios is that this database has been created or imported
            // on latest neo4j version jars before system has been "upgraded" to latest runtime version. This database is already past any
            // rolling upgrade scenario.
            return;
        }

        if ( !startupRuntimeVersion.isCurrent() || startupRuntimeVersion.kernelVersion().isGreaterThan( startupKernelVersion ) )
        {
            transactionEventListeners.registerTransactionEventListener( new DatabaseUpgradeListener( internalTransactionCommitHandler ) );
        }
    }

    private class DatabaseUpgradeListener extends InternalTransactionEventListener.Adapter<Object>
    {
        private final InternalTransactionCommitHandler internalTransactionCommitHandler;

        DatabaseUpgradeListener( InternalTransactionCommitHandler internalTransactionCommitHandler )
        {
            this.internalTransactionCommitHandler = internalTransactionCommitHandler;
        }

        @Override
        public Object beforeCommit( TransactionData data, Transaction transaction, GraphDatabaseService databaseService ) throws Exception
        {
            KernelVersion checkKernelVersion = kernelVersionRepository.kernelVersion();
            if ( dbmsRuntimeRepository.getVersion().kernelVersion().isGreaterThan( checkKernelVersion ) )
            {
                lock.writeLock().lock();
                try
                {
                    KernelVersion kernelVersionToUpgradeTo = dbmsRuntimeRepository.getVersion().kernelVersion();
                    KernelVersion currentKernelVersion = kernelVersionRepository.kernelVersion();
                    if ( kernelVersionToUpgradeTo.isGreaterThan( currentKernelVersion ) )
                    {
                        internalTransactionCommitHandler.commit( storageEngine.createUpgradeCommands( kernelVersionToUpgradeTo ) );
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }

            lock.readLock().lock();
            return LISTENER_STATE;
        }

        @Override
        public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            checkUnlockAndUnregister( state );
        }

        @Override
        public void afterRollback( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            checkUnlockAndUnregister( state );
        }

        private void checkUnlockAndUnregister( Object state )
        {
            // For some reason the transaction event listeners handling is such that even if beforeCommit fails for this listener
            // then an afterRollback will be called. Therefore we distinguish between success and failure using the state, which
            // on success must be our own special object.
            if ( state != LISTENER_STATE )
            {
                return;
            }

            lock.readLock().unlock();
            if ( kernelVersionRepository.kernelVersion().isLatest() && unregistered.compareAndSet( false, true ) )
            {
                lock.writeLock().lock();
                try
                {
                    transactionEventListeners.unregisterTransactionEventListener( this );
                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }
    }
}
