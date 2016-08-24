/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.MarshlandPool;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionStateImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.storageengine.api.StorageEngine;

import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toSet;

/**
 * Central source of transactions in the database.
 * <p>
 * This class maintains references to all transactions, a pool of passive kernel transactions, and provides
 * capabilities
 * for enumerating all running transactions. During normal operation, acquiring new transactions and enumerating live
 * ones requires no synchronization (although the live list is not guaranteed to be exact).
 */
public class KernelTransactions extends LifecycleAdapter
        implements Supplier<KernelTransactionsSnapshot>   // For providing KernelTransactionSnapshots
{
    // Transaction dependencies

    private final StatementLocksFactory statementLocksFactory;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final StatementOperationParts statementOperations;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final TransactionCommitProcess transactionCommitProcess;
    private final TransactionHooks hooks;
    private final TransactionMonitor transactionMonitor;
    private final LifeSupport dataSourceLife;
    private final Tracers tracers;
    private final StorageEngine storageEngine;
    private final Procedures procedures;
    private final TransactionIdStore transactionIdStore;
    private final Supplier<LegacyIndexTransactionState> legacyIndexTxStateSupplier;
    private final Clock clock;
    private final ReentrantReadWriteLock newTransactionsLock = new ReentrantReadWriteLock();

    // End Tx Dependencies

    /**
     * Used to enumerate all transactions in the system, active and idle ones.
     * <p>
     * This data structure is *only* updated when brand-new transactions are created, or when transactions are disposed
     * of. During normal operation (where all transactions come from and are returned to the pool), this will be left
     * in peace, working solely as a collection of references to all transaction objects (idle and active) in the
     * database.
     * <p>
     * As such, it provides a good mechanism for listing all transactions without requiring synchronization when
     * starting and committing transactions.
     */
    private final Set<KernelTransactionImplementation> allTransactions = newSetFromMap( new ConcurrentHashMap<>() );

    public KernelTransactions( StatementLocksFactory statementLocksFactory,
                               ConstraintIndexCreator constraintIndexCreator,
                               StatementOperationParts statementOperations,
                               SchemaWriteGuard schemaWriteGuard,
                               TransactionHeaderInformationFactory txHeaderFactory,
                               TransactionCommitProcess transactionCommitProcess,
                               IndexConfigStore indexConfigStore,
                               LegacyIndexProviderLookup legacyIndexProviderLookup,
                               TransactionHooks hooks,
                               TransactionMonitor transactionMonitor,
                               LifeSupport dataSourceLife,
                               Tracers tracers,
                               StorageEngine storageEngine,
                               Procedures procedures,
                               TransactionIdStore transactionIdStore,
                               Clock clock )
    {
        this.statementLocksFactory = statementLocksFactory;
        this.constraintIndexCreator = constraintIndexCreator;
        this.statementOperations = statementOperations;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionHeaderInformationFactory = txHeaderFactory;
        this.transactionCommitProcess = transactionCommitProcess;
        this.hooks = hooks;
        this.transactionMonitor = transactionMonitor;
        this.dataSourceLife = dataSourceLife;
        this.tracers = tracers;
        this.storageEngine = storageEngine;
        this.procedures = procedures;
        this.transactionIdStore = transactionIdStore;
        this.legacyIndexTxStateSupplier = () -> new CachingLegacyIndexTransactionState(
                new LegacyIndexTransactionStateImpl( indexConfigStore, legacyIndexProviderLookup ) );
        this.clock = clock;
    }

    /**
     * This is the factory that actually builds brand-new instances.
     */
    private final Factory<KernelTransactionImplementation> factory = new Factory<KernelTransactionImplementation>()
    {
        @Override
        public KernelTransactionImplementation newInstance()
        {
            KernelTransactionImplementation tx = new KernelTransactionImplementation(
                    statementOperations, schemaWriteGuard, hooks, constraintIndexCreator, procedures,
                    transactionHeaderInformationFactory, transactionCommitProcess, transactionMonitor,
                    legacyIndexTxStateSupplier, localTxPool, clock, tracers.transactionTracer,
                    storageEngine );

            allTransactions.add( tx );
            return tx;
        }
    };

    public KernelTransaction newInstance( KernelTransaction.Type type, AccessMode accessMode )
    {
        assertCurrentThreadIsNotBlockingNewTransactions();
        newTransactionsLock.readLock().lock();
        try
        {
            assertDatabaseIsRunning();
            TransactionId lastCommittedTransaction = transactionIdStore.getLastCommittedTransaction();
            KernelTransactionImplementation tx = localTxPool.acquire();
            StatementLocks statementLocks = statementLocksFactory.newInstance();
            tx.initialize( lastCommittedTransaction.transactionId(),
                    lastCommittedTransaction.commitTimestamp(), statementLocks, type, accessMode );
            return tx;
        }
        finally
        {
            newTransactionsLock.readLock().unlock();
        }
    }

    /**
     * Global pool of transactions, wrapped by the thread-local marshland pool and so is not used directly.
     */
    private final LinkedQueuePool<KernelTransactionImplementation> globalTxPool
            = new LinkedQueuePool<KernelTransactionImplementation>( 8, factory )
    {
        @Override
        protected void dispose( KernelTransactionImplementation tx )
        {
            allTransactions.remove( tx );
            tx.dispose();
            super.dispose( tx );
        }
    };

    /**
     * Give an approximate set of all transactions currently running.
     * This is not guaranteed to be exact, as transactions may stop and start while this set is gathered.
     *
     * @return the set of open transactions.
     */
    public Set<KernelTransactionHandle> activeTransactions()
    {
        return allTransactions.stream()
                .map( this::createHandle )
                .filter( KernelTransactionHandle::isOpen )
                .collect( toSet() );
    }

    /**
     * Create new handle for the given transaction.
     * <p>
     * <b>Note:</b> this method is package-private for testing <b>only</b>.
     *
     * @param tx transaction to wrap.
     * @return transaction handle.
     */
    KernelTransactionHandle createHandle( KernelTransactionImplementation tx )
    {
        return new KernelTransactionImplementationHandle( tx );
    }

    /**
     * Pool of unused transactions.
     */
    private final MarshlandPool<KernelTransactionImplementation> localTxPool = new MarshlandPool<>( globalTxPool );

    /**
     * Dispose of all pooled transactions. This is done on shutdown or on internal events (like an HA mode switch) that
     * require transactions to be re-created.
     */
    public void disposeAll()
    {
        for ( KernelTransactionImplementation tx : allTransactions )
        {
            // we mark all transactions for termination since we want to make sure these transactions
            // won't be reused, ever. Each transaction has, among other things, a Locks.Client and we
            // certainly want to keep that from being reused from this point.
            tx.markForTermination( Status.General.DatabaseUnavailable );
        }
        localTxPool.disposeAll();
        globalTxPool.disposeAll();
    }

    @Override
    public void shutdown() throws Throwable
    {
        disposeAll();
    }

    private void assertDatabaseIsRunning()
    {
        // TODO: Copied over from original source in NeoXADS - this should probably use DBAvailability,
        // rather than this.
        // Note, if you change this, you need a separate mechanism to stop transactions from being started during
        // Kernel#stop().
        if ( !dataSourceLife.isRunning() )
        {
            throw new DatabaseShutdownException();
        }
    }

    @Override
    public KernelTransactionsSnapshot get()
    {
        return new KernelTransactionsSnapshot( activeTransactions(), clock.millis() );
    }

    /**
     * Do not allow new transactions to start until {@link #unblockNewTransactions()} is called. Current thread have
     * responsibility of doing so.
     * <p>
     * Blocking call.
     */
    public void blockNewTransactions()
    {
        newTransactionsLock.writeLock().lock();
    }

    /**
     * Allow new transactions to be started again if current thread is the one who called
     * {@link #blockNewTransactions()}.
     *
     * @throws IllegalStateException if current thread is not the one that called {@link #blockNewTransactions()}.
     */
    public void unblockNewTransactions()
    {
        if ( !newTransactionsLock.writeLock().isHeldByCurrentThread() )
        {
            throw new IllegalStateException( "This thread did not block transactions previously" );
        }
        newTransactionsLock.writeLock().unlock();
    }

    private void assertCurrentThreadIsNotBlockingNewTransactions()
    {
        if ( newTransactionsLock.isWriteLockedByCurrentThread() )
        {
            throw new IllegalStateException(
                    "Thread that is blocking new transactions from starting can't start new transaction" );
        }
    }
}
