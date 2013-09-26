/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionImplementation;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.Transactor;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.transaction.XaDataSourceManager.neoStoreListener;

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for
 * consumption by both the Core API, Cypher, and any other components that want to interface with the
 * underlying database.
 * <p/>
 * This is currently in an intermediate phase, with many features still unavailable unless the Core API is also
 * present. We are in the process of moving Core API features into the kernel.
 * <p/>
 * <h1>Structure</h1>
 * <p/>
 * The Kernel itself has a simple API - it lets you start transactions. The transactions, in turn, allow you to
 * create statements, which, in turn, operate against the database. The reason for the separation between statements
 * and transactions is database isolation. Please refer to the {@link KernelTransaction} javadoc for details.
 * <p/>
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementOperations}, which will grab locks and delegate to {@link StateHandlingStatementOperations}
 * which
 * will store the change in the transaction state, to be applied later if the transaction is committed.
 * <p/>
 * A read will, similarly, pass through {@link LockingStatementOperations}, which should (but does not currently) grab
 * read locks. It then reaches {@link StateHandlingStatementOperations}, which includes any changes that exist in the
 * current transaction, and then finally {@link StoreStatementOperations} will read the current committed state from
 * the
 * stores or caches.
 * <p/>
 * <h1>Refactoring</h1>
 * <p/>
 * There are several sources of pain around the current state, which we hope to refactor away down the line. A major
 * source of pain is the interaction between this class and {@link NeoStoreXaDataSource}. We should discuss the role
 * of these two classes. Either one should create the other, or they should be combined into one class.
 * <p/>
 * Another pain is transaction state, where lots of legacy code still rules supreme. Please refer to {@link TxState}
 * for details about the work in this area.
 * <p/>
 * Cache invalidation is similarly problematic, where cache invalidation really should be done when changes are applied
 * to the store, through the logical log. However, this is mostly not the case, cache invalidation is done as we work
 * through the Core API. Only in HA mode is cache invalidation done through log application, and then only through
 * evicting whole entities from the cache whenever they change, leading to large performance hits on writes. This area
 * is still open for investigation, but an approach where the logical log simply tells a store write API to apply some
 * change, and the implementation of that API is responsible for keeping caches in sync.
 * <p/>
 * Please expand and update this as you learn things or find errors in the text above.
 * <p/>
 * The current interaction with the TransactionManager looks like this:
 * <p/>
 * <ol>
 * <li>
 * tx.close() --> TransactionImpl.commit() --> *KernelTransaction.commit()* --> TxManager.commit()
 * </li>
 * <li>
 * TxManager.commit() --> TransactionImpl.doCommit() --> dataSource.commit()
 * </li>
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final AbstractTransactionManager transactionManager;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final PersistenceManager persistenceManager;
    private final XaDataSourceManager dataSourceManager;
    private final LockManager lockManager;
    private final DependencyResolver dependencyResolver;
    private final UpdateableSchemaState schemaState;
    private final boolean readOnly;
    private final SchemaWriteGuard schemaWriteGuard;

    // These non-final components are all circular dependencies in various configurations.
    // As we work towards refactoring the old kernel, we should work to remove these.
    private IndexingService indexService;
    private NeoStore neoStore;
    private NodeManager nodeManager;
    private PersistenceCache persistenceCache;
    private StatementOperationParts statementOperations;
    private SchemaCache schemaCache;
    private SchemaIndexProviderMap providerMap = null;
    private LegacyKernelOperations legacyKernelOperations;
    private LabelScanStore labelScanStore;
    private boolean isShutdown = false;

    public Kernel( AbstractTransactionManager transactionManager, PropertyKeyTokenHolder propertyKeyTokenHolder,
                   LabelTokenHolder labelTokenHolder, RelationshipTypeTokenHolder relationshipTypeTokenHolder,
                   PersistenceManager persistenceManager, XaDataSourceManager dataSourceManager,
                   LockManager lockManager, UpdateableSchemaState schemaState, DependencyResolver dependencyResolver,
                   boolean readOnly, SchemaWriteGuard schemaWriteGuard )
    {
        this.transactionManager = transactionManager;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.persistenceManager = persistenceManager;
        this.dataSourceManager = dataSourceManager;
        this.lockManager = lockManager;
        this.dependencyResolver = dependencyResolver;
        this.schemaState = schemaState;
        this.readOnly = readOnly;
        this.schemaWriteGuard = schemaWriteGuard;
    }

    @Override
    public void start()
    {
        // TODO: This is a huge smell. See the refactoring section in the javadoc of this class for thoughts about
        // the interplay between Kernel and NeoStoreXaDataSource

        nodeManager = dependencyResolver.resolveDependency( NodeManager.class );

        dataSourceManager.addDataSourceRegistrationListener( neoStoreListener( new DataSourceRegistrationListener()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                NeoStoreXaDataSource neoDataSource = (NeoStoreXaDataSource) ds;
                neoStore = neoDataSource.getNeoStore();
                indexService = neoDataSource.getIndexService();
                labelScanStore = neoDataSource.getLabelScanStore();
                providerMap = neoDataSource.getProviderMap();
                persistenceCache = neoDataSource.getPersistenceCache();
                schemaCache = neoDataSource.getSchemaCache();

                for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAllSchemaRules() ) )
                {
                    schemaCache.addSchemaRule( schemaRule );
                }
            }

            @Override
            public void unregisteredDataSource( XaDataSource ds )
            {
                neoStore = null;
            }
        } ) );
    }

    @Override
    public void bootstrapAfterRecovery()
    {
        this.statementOperations = buildStatementOperations();
        this.legacyKernelOperations = new DefaultLegacyKernelOperations( nodeManager );
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
                schemaWriteGuard, labelScanStore, indexService, lockManager, transactionManager, nodeManager,
                persistenceCache, schemaState, persistenceManager, providerMap, neoStore );
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
        // Start off with the store layer.
        StoreStatementOperations context = new StoreStatementOperations(
                propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder,
                new SchemaStorage( neoStore.getSchemaStore() ), neoStore,
                persistenceManager, indexService );
        StatementOperationParts parts = new StatementOperationParts(
                context, context, context, context, context, null, null )
                .additionalPart( AuxiliaryStoreOperations.class, context );

        // + Caching
        CachingStatementOperations cachingContext = new CachingStatementOperations(
                parts.entityReadOperations(),
                parts.schemaReadOperations(),
                persistenceCache, schemaCache );
        parts = parts.override( null, null, cachingContext, null, cachingContext, null, null );

        // + Transaction-local state awareness
        AuxiliaryStoreOperations auxStoreOperations = parts.resolve( AuxiliaryStoreOperations.class );
        auxStoreOperations = new LegacyAutoIndexAuxStoreOps( auxStoreOperations, propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(),
                nodeManager.getRelationshipPropertyTrackers(),
                nodeManager );

        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations(
                parts.entityReadOperations(),
                parts.schemaReadOperations(),
                auxStoreOperations,
                new ConstraintIndexCreator( new Transactor( transactionManager ), indexService ) );

        parts = parts.override(
                null, null, stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
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
