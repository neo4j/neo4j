/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for
 * consumption by both the Core API, Cypher, and any other components that want to interface with the
 * underlying database.
 *
 * This is currently in an intermediate phase, with many features still unavailable unless the Core API is also
 * present. We are in the process of moving Core API features into the kernel.
 *
 * <h1>Structure</h1>
 *
 * The Kernel itself has a simple API - it lets you start transactions. The transactions, in turn, allow you to
 * create statements, which, in turn, operate against the database. The reason for the separation between statements
 * and transactions is database isolation. Please refer to the {@link KernelTransaction} javadoc for details.
 *
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementOperations}, which will grab locks and delegate to {@link StateHandlingStatementOperations}
 * which will store the change in the transaction state, to be applied later if the transaction is committed.
 *
 * A read will, similarly, pass through {@link LockingStatementOperations}, which should (but does not currently) grab
 * read locks. It then reaches {@link StateHandlingStatementOperations}, which includes any changes that exist in the
 * current transaction, and then finally {@link org.neo4j.kernel.impl.api.store.DiskLayer} will read the current committed state from
 * the stores or caches.
 *
 * <h1>A story of JTA</h1>
 *
 * We have, for a long time, supported full X.Open two-phase commits, which is handled by our TxManager implementation
 * of the JTA interfaces. However, two phase commit is slow and complex. Ideally we don't want every day use of neo4j
 * to require JTA anymore, but rather have it be a bonus feature that can be enabled when the user wants two-phase
 * commit support. As such, we are working to keep the Kernel compatible with a JTA system built on top of it, but
 * at the same time it should remain independent and runnable without a transaction manager.
 *
 * The heart of this work is in the relationship between {@link KernelTransaction},
 * {@link org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction} and
 * {@link org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager}. The latter should become a wrapper around
 * KernelTransactions, exposing them as JTA-capable transactions. The Write transaction should be hidden from the outside,
 * an implementation detail living inside the kernel.
 *
 * <h1>Refactoring</h1>
 *
 * There are several sources of pain around the current state, which we hope to refactor away down the line.
 *
 * One pain is transaction state, where lots of legacy code still rules supreme. Please refer to {@link TxState}
 * for details about the work in this area.
 *
 * Cache invalidation is similarly problematic, where cache invalidation really should be done when changes are applied
 * to the store, through the logical log. However, this is mostly not the case, cache invalidation is done as we work
 * through the Core API. Only in HA mode is cache invalidation done through log application, and then only through
 * evicting whole entities from the cache whenever they change, leading to large performance hits on writes. This area
 * is still open for investigation, but an approach where the logical log simply tells a store write API to apply some
 * change, and the implementation of that API is responsible for keeping caches in sync.
 *
 * Please expand and update this as you learn things or find errors in the text above.
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final AbstractTransactionManager transactionManager;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final PersistenceManager persistenceManager;
    private final LockManager lockManager;
    private final UpdateableSchemaState schemaState;
    private final SchemaWriteGuard schemaWriteGuard;
    private final IndexingService indexService;
    private final NeoStore neoStore;
    private final Provider<NeoStore> neoStoreProvider;
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final SchemaIndexProviderMap providerMap;
    private final LabelScanStore labelScanStore;
    private final NodeManager nodeManager;
    private final LegacyKernelOperations legacyKernelOperations;
    private final StatementOperationParts statementOperations;

    private final boolean readOnly;
    private final LegacyPropertyTrackers legacyPropertyTrackers;

    private boolean isShutdown = false;

    public Kernel( AbstractTransactionManager transactionManager, PropertyKeyTokenHolder propertyKeyTokenHolder,
                   LabelTokenHolder labelTokenHolder, RelationshipTypeTokenHolder relationshipTypeTokenHolder,
                   PersistenceManager persistenceManager, LockManager lockManager, UpdateableSchemaState schemaState,
                   SchemaWriteGuard schemaWriteGuard,
                   IndexingService indexService, NodeManager nodeManager, Provider<NeoStore> neoStore, PersistenceCache persistenceCache,
                   SchemaCache schemaCache, SchemaIndexProviderMap providerMap, LabelScanStore labelScanStore, boolean readOnly )
    {
        this.transactionManager = transactionManager;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.persistenceManager = persistenceManager;
        this.lockManager = lockManager;
        this.schemaState = schemaState;
        this.providerMap = providerMap;
        this.readOnly = readOnly;
        this.schemaWriteGuard = schemaWriteGuard;
        this.indexService = indexService;
        this.neoStore = neoStore.instance();
        this.neoStoreProvider = neoStore;
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.labelScanStore = labelScanStore;
        this.nodeManager = nodeManager;

        this.legacyPropertyTrackers = new LegacyPropertyTrackers( propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(),
                nodeManager.getRelationshipPropertyTrackers(),
                nodeManager );
        this.legacyKernelOperations = new DefaultLegacyKernelOperations( nodeManager );
        this.statementOperations = buildStatementOperations();
    }

    @Override
    public void start()
    {
        loadSchemaCache();
    }

    public void loadSchemaCache()
    {
        schemaCache.clear();
        for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAllSchemaRules() ) )
        {
            schemaCache.addSchemaRule( schemaRule );
        }
    }

    @Override
    public void stop()
    {
        isShutdown = true;
    }

    @Override
    public KernelTransaction newTransaction()
    {
        checkIfShutdown();
        return new KernelTransactionImplementation( statementOperations, legacyKernelOperations, readOnly,
                schemaWriteGuard, labelScanStore, indexService, transactionManager, nodeManager,
                schemaState, new LockHolderImpl( lockManager, getJTATransaction(), nodeManager ),
                persistenceManager, providerMap, neoStore, getLegacyTxState() );
    }

    // We temporarily need this until all transaction state has moved into the kernel
    private TransactionState getLegacyTxState()
    {
        try
        {
            TransactionState legacyState = transactionManager.getTransactionState();
            return legacyState != null ? legacyState : TransactionState.NO_STATE;
        }
        catch ( RuntimeException e )
        {
            // If the transaction manager is in a bad state, we use an empty transaction state. It's not
            // a great thing, but without this we can't create kernel transactions during recovery.
            // Accepting that this is terrible for now, since the plan is to remove this dependency on the JTA
            // transaction entirely.
            // This should be safe to do, since we only use the JTA tx for locking, and we don't do any locking during
            // recovery.
            return TransactionState.NO_STATE;
        }
    }

    // We temporarily depend on this to satisfy locking. This should go away once all locks are handled in the kernel.
    private Transaction getJTATransaction()
    {
        try
        {
            return transactionManager.getTransaction();
        }
        catch ( SystemException e )
        {
            // If the transaction manager is in a bad state, we return a placebo transaction. It's not
            // a great thing, but without this we can't create kernel transactions during recovery.
            // Accepting that this is terrible for now, since the plan is to remove this dependency on the JTA
            // transaction entirely.
            // This should be safe to do, since we only use the JTA tx for locking, and we don't do any locking during
            // recovery.
            return new NoOpJTATransaction();
        }
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }

    private StatementOperationParts buildStatementOperations()
    {
        // Bottom layer: Read-access to committed data
        StoreReadLayer storeLayer = new CacheLayer( new DiskLayer( propertyKeyTokenHolder, labelTokenHolder,
                relationshipTypeTokenHolder, new SchemaStorage( neoStore.getSchemaStore() ), neoStoreProvider,
                indexService ), persistenceCache, indexService, schemaCache );

        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations(
                storeLayer, legacyPropertyTrackers,
                new ConstraintIndexCreator( new Transactor( transactionManager, persistenceManager ), indexService ) );

        StatementOperationParts parts = new StatementOperationParts( stateHandlingContext, stateHandlingContext,
                stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( schemaState ) );

        // + Constraints
        ConstraintEnforcingEntityOperations constraintEnforcingEntityOperations =
                new ConstraintEnforcingEntityOperations( parts.entityWriteOperations(), parts.entityReadOperations(),
                        parts.schemaReadOperations() );

        // + Data integrity
        DataIntegrityValidatingStatementOperations dataIntegrityContext = new
                DataIntegrityValidatingStatementOperations(
                parts.keyWriteOperations(),
                parts.schemaReadOperations(),
                parts.schemaWriteOperations() );

        parts = parts.override( null, dataIntegrityContext, constraintEnforcingEntityOperations,
                constraintEnforcingEntityOperations, null, dataIntegrityContext, null );

        // + Locking
        LockingStatementOperations lockingContext = new LockingStatementOperations(
                parts.entityWriteOperations(),
                parts.schemaReadOperations(),
                parts.schemaWriteOperations(),
                parts.schemaStateOperations() );
        parts = parts.override( null, null, null, lockingContext, lockingContext, lockingContext, lockingContext );

        return parts;
    }
}
