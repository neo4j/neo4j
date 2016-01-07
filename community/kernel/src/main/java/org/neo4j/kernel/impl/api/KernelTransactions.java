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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionStateImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.proc.Procedures;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.storageengine.api.StorageEngine;

import static java.util.Collections.newSetFromMap;

/**
 * Central source of transactions in the database.
 * <p>
 * This class maintains references to all running transactions and provides capabilities for enumerating them.
 * During normal operation, acquiring new transactions and enumerating live ones requires same amount of
 * synchronization as {@link ConcurrentHashMap} provides for insertions and iterations.
 * <p>
 * Live list is not guaranteed to be exact.
 */
public class KernelTransactions extends LifecycleAdapter implements Factory<KernelTransaction>
{
    private final Locks locks;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final StatementOperationParts statementOperations;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final TransactionCommitProcess transactionCommitProcess;
    private final IndexConfigStore indexConfigStore;
    private final LegacyIndexProviderLookup legacyIndexProviderLookup;
    private final TransactionHooks hooks;
    private final TransactionMonitor transactionMonitor;
    private final LifeSupport dataSourceLife;
    private final Tracers tracers;
    private final StorageEngine storageEngine;
    private final Procedures procedures;

    private final Set<KernelTransaction> allTransactions = newSetFromMap( new ConcurrentHashMap<>() );

    public KernelTransactions( Locks locks,
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
                               Procedures procedures )
    {
        this.locks = locks;
        this.constraintIndexCreator = constraintIndexCreator;
        this.statementOperations = statementOperations;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionHeaderInformationFactory = txHeaderFactory;
        this.transactionCommitProcess = transactionCommitProcess;
        this.indexConfigStore = indexConfigStore;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.hooks = hooks;
        this.transactionMonitor = transactionMonitor;
        this.dataSourceLife = dataSourceLife;
        this.tracers = tracers;
        this.storageEngine = storageEngine;
        this.procedures = procedures;
    }

    @Override
    public KernelTransaction newInstance()
    {
        assertDatabaseIsRunning();

        Locks.Client locksClient = locks.newClient();
        LegacyIndexTransactionState legacyIndexTransactionState =
                new LegacyIndexTransactionStateImpl( indexConfigStore, legacyIndexProviderLookup );

        long lastTransactionIdWhenStarted = ((MetaDataStore)storageEngine.metaDataStore()).getLastCommittedTransactionId();

        KernelTransactionImplementation tx = new KernelTransactionImplementation( statementOperations, schemaWriteGuard,
                locksClient, hooks, constraintIndexCreator, procedures, transactionHeaderInformationFactory,
                transactionCommitProcess, transactionMonitor, legacyIndexTransactionState,
                this, Clock.SYSTEM_CLOCK, tracers.transactionTracer, storageEngine, lastTransactionIdWhenStarted );

        allTransactions.add( tx );

        return tx;
    }

    /**
     * Signals that given transaction is closed and should be removed from the set of running transactions.
     *
     * @param tx the closed transaction.
     * @throws IllegalStateException if given transaction is not in the set of active transactions.
     */
    public void transactionClosed( KernelTransaction tx )
    {
        boolean removed = allTransactions.remove( tx );
        if ( !removed )
        {
            throw new IllegalStateException( "Transaction: " + tx + " is not present in the " +
                                             "set of known active transactions: " + allTransactions );
        }
    }

    /**
     * Give an approximate set of all transactions currently running.
     * This is not guaranteed to be exact, as transactions may stop and start while this set is gathered.
     *
     * @return the set of open transactions.
     */
    public Set<KernelTransaction> activeTransactions()
    {
        return Collections.unmodifiableSet( new HashSet<>( allTransactions ) );
    }

    /**
     * Dispose of all active transactions. This is done on shutdown or on internal events (like an HA mode switch) that
     * require transactions to be re-created.
     */
    public void disposeAll()
    {
        allTransactions.forEach( KernelTransaction::markForTermination );
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
}
