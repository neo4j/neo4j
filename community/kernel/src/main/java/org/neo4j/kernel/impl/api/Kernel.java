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

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.OldTxStateBridgeImpl;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
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

import static java.util.Collections.synchronizedList;

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
 * and transactions is database isolation. Please refer to the {@link TransactionContext} javadoc for details.
 *
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementContext}, which will grab locks and delegate to {@link StateHandlingStatementContext} which
 * will store the change in the transaction state, to be applied later if the transaction is committed.
 *
 * A read will, similarly, pass through {@link LockingStatementContext}, which should (but does not currently) grab
 * read locks. It then reaches {@link StateHandlingStatementContext}, which includes any changes that exist in the
 * current transaction, and then finally {@link StoreStatementContext} will read the current committed state from the
 * stores or caches.
 *
 * <h1>Refactoring</h1>
 *
 * There are several sources of pain around the current state, which we hope to refactor away down the line. A major
 * source of pain is the interaction between this class and {@link NeoStoreXaDataSource}. We should discuss the role
 * of these two classes. Either one should create the other, or they should be combined into one class.
 *
 * Another pain is transaction state, where lots of legacy code still rules supreme. Please refer to {@link TxState}
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
 * 
 * The current interaction with the TransactionManager looks like this:
 * 
 * <ol>
 * <li>
 * tx.finish() --> TransactionImpl.commit() --> TransactionContext.commit() --> TxManager.commit()
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
    private final PersistenceManager persistenceManager;
    private final XaDataSourceManager dataSourceManager;
    private final LockManager lockManager;
    private final DependencyResolver dependencyResolver;
    private SchemaCache schemaCache;
    private final UpdateableSchemaState schemaState;
    private final boolean highlyAvailableInstance;
    private final StatementContextOwners statementContextOwners = new StatementContextOwners();
    private SchemaIndexProviderMap providerMap = null;

    // These non-final components are all circular dependencies in various configurations.
    // As we work towards refactoring the old kernel, we should work to remove these.
    private IndexingService indexService;
    private NeoStore neoStore;
    private NodeManager nodeManager;
    private PersistenceCache persistenceCache;
    private boolean isShutdown = false;

    public Kernel( AbstractTransactionManager transactionManager,
                   PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                   PersistenceManager persistenceManager, XaDataSourceManager dataSourceManager,
                   LockManager lockManager, UpdateableSchemaState schemaState,
                   DependencyResolver dependencyResolver, boolean highlyAvailable )
    {
        this.transactionManager = transactionManager;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.persistenceManager = persistenceManager;
        this.dataSourceManager = dataSourceManager;
        this.lockManager = lockManager;
        this.dependencyResolver = dependencyResolver;
        this.schemaState = schemaState;
        highlyAvailableInstance = highlyAvailable;
    }

    @Override
    public void start() throws Throwable
    {
        // TODO: This is a huge smell. See the refactoring section in the javadoc of this class for thoughts about
        // the interplay between Kernel and NeoStoreXaDataSource

        nodeManager = dependencyResolver.resolveDependency( NodeManager.class );

        dataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( isNeoDataSource( ds ) )
                {
                    NeoStoreXaDataSource neoDataSource = (NeoStoreXaDataSource) ds;
                    neoStore = neoDataSource.getNeoStore();
                    indexService = neoDataSource.getIndexService();
                    providerMap = neoDataSource.getProviderMap();
                    persistenceCache = neoDataSource.getPersistenceCache();
                    schemaCache = neoDataSource.getSchemaCache();

                    for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAll() ) )
                    {
                        schemaCache.addSchemaRule( schemaRule );
                    }
                }
            }

            @Override
            public void unregisteredDataSource( XaDataSource ds )
            {
                if ( isNeoDataSource( ds ) )
                {
                    neoStore = null;
                }
            }

            private boolean isNeoDataSource( XaDataSource ds )
            {
                return ds.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
            }
        } );
    }

    @Override
    public void stop() throws Throwable
    {
        statementContextOwners.close();
        isShutdown = true;
    }

    @Override
    public TransactionContext newTransactionContext()
    {
        checkIfShutdown();


        // I/O
        // TODO The store layer should depend on a clean abstraction of the data, not on all the XXXManagers from the
        // old code base
        TransactionContext result = new StoreTransactionContext(
                transactionManager, propertyKeyTokenHolder, labelTokenHolder, nodeManager, neoStore, indexService );

        // + Transaction state and Caching
        result = new StateHandlingTransactionContext( result, new SchemaStorage( neoStore.getSchemaStore() ),
                                                      newTxState(), providerMap, persistenceCache, schemaCache,
                                                      persistenceManager, schemaState,
                                                      new ConstraintIndexCreator(
                                                      new Transactor( transactionManager ), indexService ) );

        // + Constraint evaluation
        result = new ConstraintValidatingTransactionContext( result );

        // + Locking
        result = new LockingTransactionContext( result, lockManager, transactionManager );

        if ( highlyAvailableInstance )
        {
            // + Stop HA from creating constraints
            result = new UniquenessConstraintStoppingTransactionContext( result );
        }

        // + Single statement at a time
        result = new ReferenceCountingTransactionContext( result );

        // done
        return result;
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }

    @Override
    public StatementContext newReadOnlyStatementContext()
    {
        checkIfShutdown();
        return statementContextOwners.get().getStatementContext();
    }

    private StatementContext createReadOnlyStatementContext()
    {
        checkIfShutdown();

        // I/O
        SchemaStorage schemaStorage = new SchemaStorage( neoStore.getSchemaStore() );
        StatementContext result = new StoreStatementContext( propertyKeyTokenHolder, labelTokenHolder, nodeManager,
                schemaStorage, neoStore, indexService, new IndexReaderFactory.Caching( indexService ) );

        // + Cache
        result = new CachingStatementContext( result, persistenceCache, schemaCache );

        // + Read only access
        result = new ReadOnlyStatementContext( result );

        // + Schema state handling
        result = createSchemaStateStatementContext( result );

        return result;
    }

    private StatementContext createSchemaStateStatementContext( StatementContext inner )
    {
        return new CompositeStatementContext( inner, new SchemaStateConcern( schemaState ) );
    }

    private TxState newTxState()
    {
        return new TxState(
                new OldTxStateBridgeImpl( nodeManager, transactionManager.getTransactionState() ),
                persistenceManager,
                new TxState.IdGeneration()
                {
                    @Override
                    public long newNodeId()
                    {
                        throw new UnsupportedOperationException( "not implemented" );
                    }

                    @Override
                    public long newRelationshipId()
                    {
                        throw new UnsupportedOperationException( "not implemented" );
                    }
                }
        );
    }

    private class StatementContextOwners extends ThreadLocal<StatementContextOwner>
    {
        private final Collection<StatementContextOwner> all =
                synchronizedList( new ArrayList<StatementContextOwner>() );

        @Override
        protected StatementContextOwner initialValue()
        {
            StatementContextOwner owner = new StatementContextOwner()
            {
                @Override
                protected StatementContext createStatementContext()
                {
                    return Kernel.this.createReadOnlyStatementContext();
                }
            };
            all.add( owner );
            return owner;
        }

        void close()
        {
            for ( StatementContextOwner owner : all )
            {
                owner.closeAllStatements();
            }
        }
    }
}
