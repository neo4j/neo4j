/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.function.Factory;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.IntegrityValidator;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransactionContextSupplier;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.Collections.newSetFromMap;

/**
 * Central source of transactions in the database.
 *
 * This class maintains references to all transactions, a pool of passive kernel transactions, and provides capabilities
 * for enumerating all running transactions. During normal operation, acquiring new transactions and enumerating live
 * ones requires no synchronization (although the live list is not guaranteed to be exact).
 */
public class KernelTransactions extends LifecycleAdapter implements Factory<KernelTransaction>
{
    // Transaction dependencies

    private final NeoStoreTransactionContextSupplier neoStoreTransactionContextSupplier;
    private final NeoStore neoStore;
    private final Locks locks;
    private final IntegrityValidator integrityValidator;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;
    private final StatementOperationParts statementOperations;
    private final UpdateableSchemaState updateableSchemaState;
    private final SchemaWriteGuard schemaWriteGuard;
    private final SchemaIndexProviderMap providerMap;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final PersistenceCache persistenceCache;
    private final StoreReadLayer storeLayer;
    private final TransactionCommitProcess transactionCommitProcess;
    private final IndexConfigStore indexConfigStore;
    private final LegacyIndexApplier.ProviderLookup legacyIndexProviderLookup;
    private final TransactionHooks hooks;
    private final TransactionMonitor transactionMonitor;
    private final LifeSupport dataSourceLife;
    private final boolean readOnly;

    // End Tx Dependencies

    /**
     * Used to enumerate all transactions in the system, active and idle ones.
     *
     * This data structure is *only* updated when brand-new transactions are created, or when transactions are disposed
     * of. During normal operation (where all transactions come from and are returned to the pool), this will be left
     * in peace, working solely as a collection of references to all transaction objects (idle and active) in the
     * database.
     *
     * As such, it provides a good mechanism for listing all transactions without requiring synchronization when
     * starting and committing transactions.
     */
    private final Set<KernelTransactionImplementation> allTransactions = newSetFromMap(
            new ConcurrentHashMap<KernelTransactionImplementation, Boolean>() );

    /**
     * This is the factory that actually builds brand-new instances.
     */
    private final Factory<KernelTransactionImplementation> factory = new Factory<KernelTransactionImplementation>()
    {
        @Override
        public KernelTransactionImplementation newInstance()
        {
            NeoStoreTransactionContext context = neoStoreTransactionContextSupplier.acquire();
            Locks.Client locksClient = locks.newClient();
            context.bind( locksClient );
            TransactionRecordState neoStoreTransaction = new TransactionRecordState(
                    neoStore.getLastCommittedTransactionId(), neoStore, integrityValidator, context );
            LegacyIndexTransactionState legacyIndexTransactionState =
                    new LegacyIndexTransactionState( indexConfigStore, legacyIndexProviderLookup );
            KernelTransactionImplementation tx = new KernelTransactionImplementation(
                    statementOperations, readOnly, schemaWriteGuard,
                    labelScanStore, indexingService, updateableSchemaState, neoStoreTransaction, providerMap,
                    neoStore, locksClient, hooks, constraintIndexCreator, transactionHeaderInformationFactory.create(),
                    transactionCommitProcess, transactionMonitor, persistenceCache, storeLayer,
                    legacyIndexTransactionState, Clock.SYSTEM_CLOCK )
            {
                @Override
                protected void dispose()
                {
                    allTransactions.remove( this );
                }
            };

            allTransactions.add( tx );

            return tx;
        }
    };

    public KernelTransactions( NeoStoreTransactionContextSupplier neoStoreTransactionContextSupplier,
                               NeoStore neoStore, Locks locks, IntegrityValidator integrityValidator,
                               ConstraintIndexCreator constraintIndexCreator,
                               IndexingService indexingService, LabelScanStore labelScanStore,
                               StatementOperationParts statementOperations,
                               UpdateableSchemaState updateableSchemaState, SchemaWriteGuard schemaWriteGuard,
                               SchemaIndexProviderMap providerMap, TransactionHeaderInformationFactory txHeaderFactory,
                               PersistenceCache persistenceCache, StoreReadLayer storeLayer,
                               TransactionCommitProcess transactionCommitProcess,
                               IndexConfigStore indexConfigStore, LegacyIndexApplier.ProviderLookup legacyIndexProviderLookup,
                               TransactionHooks hooks, TransactionMonitor transactionMonitor, LifeSupport dataSourceLife, boolean readOnly )
    {
        this.neoStoreTransactionContextSupplier = neoStoreTransactionContextSupplier;
        this.neoStore = neoStore;
        this.locks = locks;
        this.integrityValidator = integrityValidator;
        this.constraintIndexCreator = constraintIndexCreator;
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.statementOperations = statementOperations;
        this.updateableSchemaState = updateableSchemaState;
        this.schemaWriteGuard = schemaWriteGuard;
        this.providerMap = providerMap;
        this.transactionHeaderInformationFactory = txHeaderFactory;
        this.persistenceCache = persistenceCache;
        this.storeLayer = storeLayer;
        this.transactionCommitProcess = transactionCommitProcess;
        this.indexConfigStore = indexConfigStore;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.hooks = hooks;
        this.transactionMonitor = transactionMonitor;
        this.dataSourceLife = dataSourceLife;
        this.readOnly = readOnly;
    }

    @Override
    public KernelTransaction newInstance()
    {
        assertDatabaseIsRunning();
        return factory.newInstance().initialize(
                transactionHeaderInformationFactory.create(),
                neoStore.getLastCommittedTransactionId() );
    }

    /**
     * Give an approximate list of all transactions currently running. This is not guaranteed to be exact, as
     * transactions may stop and start while this list is gathered.
     */
    public List<KernelTransaction> activeTransactions()
    {
        List<KernelTransaction> output = new ArrayList<>();
        for ( KernelTransactionImplementation tx : allTransactions )
        {
            if(tx.isOpen())
            {
                output.add( tx );
            }
        }

        return output;
    }

    /**
     * Dispose of all pooled transactions. This is done on shutdown or on internal events (like an HA mode switch) that
     * require transactions to be re-created.
     */
    public void disposeAll()
    {
        for ( KernelTransactionImplementation tx : allTransactions )
        {
            if(tx.isOpen())
            {
                tx.markForTermination();
            }
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        disposeAll();
    }

    private void assertDatabaseIsRunning()
    {
        // TODO: Copied over from original source in NeoXADS - this should probably use DBAvailability, rather than this.
        // Note, if you change this, you need a separate mechanism to stop transactions from being started during
        // Kernel#stop().
        if ( !dataSourceLife.isRunning() )
        {
            throw new DatabaseShutdownException();
        }
    }
}
