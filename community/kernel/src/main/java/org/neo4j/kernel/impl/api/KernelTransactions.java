/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.MarshlandPool;
import org.neo4j.function.Factory;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionStateImpl;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextFactory;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;

import static java.util.Collections.newSetFromMap;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Central source of transactions in the database.
 * <p>
 * This class maintains references to all transactions, a pool of passive kernel transactions, and provides
 * capabilities
 * for enumerating all running transactions. During normal operation, acquiring new transactions and enumerating live
 * ones requires no synchronization (although the live list is not guaranteed to be exact).
 */
public class KernelTransactions extends LifecycleAdapter
        implements Factory<KernelTransaction>, // For providing KernelTransaction instances
        Supplier<KernelTransactionsSnapshot>   // For providing KernelTransactionSnapshots
{
    public static final Setting<Boolean> tx_termination_aware_locks = setting(
            "unsupported.dbms.tx_termination_aware_locks", Settings.BOOLEAN, Settings.FALSE );

    // Transaction dependencies

    private final NeoStoreTransactionContextFactory neoStoreTransactionContextFactory;
    private final NeoStores neoStores;
    private final StatementLocksFactory statementLocksFactory;
    private final boolean txTerminationAwareLocks;
    private final IntegrityValidator integrityValidator;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;
    private final StatementOperationParts statementOperations;
    private final UpdateableSchemaState updateableSchemaState;
    private final SchemaWriteGuard schemaWriteGuard;
    private final SchemaIndexProviderMap providerMap;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final StoreReadLayer storeLayer;
    private final TransactionCommitProcess transactionCommitProcess;
    private final IndexConfigStore indexConfigStore;
    private final LegacyIndexProviderLookup legacyIndexProviderLookup;
    private final TransactionHooks hooks;
    private final ConstraintSemantics constraintSemantics;
    private final TransactionMonitor transactionMonitor;
    private final LifeSupport dataSourceLife;
    private final ProcedureCache procedureCache;
    private final Tracers tracers;
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
    private final Set<KernelTransactionImplementation> allTransactions = newSetFromMap(
            new ConcurrentHashMap<KernelTransactionImplementation,Boolean>() );

    public KernelTransactions( NeoStoreTransactionContextFactory neoStoreTransactionContextFactory,
                               NeoStores neoStores,
                               StatementLocksFactory statementLocksFactory,
                               IntegrityValidator integrityValidator,
                               ConstraintIndexCreator constraintIndexCreator,
                               IndexingService indexingService, LabelScanStore labelScanStore,
                               StatementOperationParts statementOperations,
                               UpdateableSchemaState updateableSchemaState, SchemaWriteGuard schemaWriteGuard,
                               SchemaIndexProviderMap providerMap, TransactionHeaderInformationFactory txHeaderFactory,
                               StoreReadLayer storeLayer,
                               TransactionCommitProcess transactionCommitProcess,
                               IndexConfigStore indexConfigStore,
                               LegacyIndexProviderLookup legacyIndexProviderLookup,
                               TransactionHooks hooks,
                               ConstraintSemantics constraintSemantics,
                               TransactionMonitor transactionMonitor,
                               LifeSupport dataSourceLife, ProcedureCache procedureCache,
                               Config config,
                               Tracers tracers,
                               Clock clock )
    {
        this.neoStoreTransactionContextFactory = neoStoreTransactionContextFactory;
        this.neoStores = neoStores;
        this.statementLocksFactory = statementLocksFactory;
        this.txTerminationAwareLocks = config.get( tx_termination_aware_locks );
        this.integrityValidator = integrityValidator;
        this.constraintIndexCreator = constraintIndexCreator;
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.statementOperations = statementOperations;
        this.updateableSchemaState = updateableSchemaState;
        this.schemaWriteGuard = schemaWriteGuard;
        this.providerMap = providerMap;
        this.transactionHeaderInformationFactory = txHeaderFactory;
        this.storeLayer = storeLayer;
        this.transactionCommitProcess = transactionCommitProcess;
        this.indexConfigStore = indexConfigStore;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.hooks = hooks;
        this.constraintSemantics = constraintSemantics;
        this.transactionMonitor = transactionMonitor;
        this.dataSourceLife = dataSourceLife;
        this.procedureCache = procedureCache;
        this.tracers = tracers;
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
            NeoStoreTransactionContext context = neoStoreTransactionContextFactory.newInstance();

            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator, context );
            LegacyIndexTransactionState legacyIndexTransactionState =
                    new LegacyIndexTransactionStateImpl( indexConfigStore, legacyIndexProviderLookup );
            KernelTransactionImplementation tx = new KernelTransactionImplementation(
                    statementOperations, schemaWriteGuard,
                    labelScanStore, indexingService, updateableSchemaState, recordState, providerMap,
                    neoStores, hooks, constraintIndexCreator, transactionHeaderInformationFactory,
                    transactionCommitProcess, transactionMonitor, storeLayer, legacyIndexTransactionState,
                    localTxPool, constraintSemantics, clock, tracers.transactionTracer, procedureCache,
                    statementLocksFactory, context, txTerminationAwareLocks );

            allTransactions.add( tx );

            return tx;
        }
    };

    @Override
    public KernelTransaction newInstance()
    {
        assertCurrentThreadIsNotBlockingNewTransactions();
        newTransactionsLock.readLock().lock();
        try
        {
            assertDatabaseIsRunning();
            TransactionId lastCommittedTransaction = neoStores.getMetaDataStore().getLastCommittedTransaction();
            return localTxPool.acquire().initialize( lastCommittedTransaction.transactionId(),
                    lastCommittedTransaction.commitTimestamp() );
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
     * Give an approximate list of all transactions currently running. This is not guaranteed to be exact, as
     * transactions may stop and start while this list is gathered.
     */
    public List<KernelTransaction> activeTransactions()
    {
        List<KernelTransaction> output = new ArrayList<>();
        for ( KernelTransactionImplementation tx : allTransactions )
        {
            if ( tx.isOpen() )
            {
                output.add( tx );
            }
        }

        return output;
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
            // We mark all transactions for termination since we want to be on the safe side here.
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
        return new KernelTransactionsSnapshot( allTransactions, clock.currentTimeMillis() );
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
