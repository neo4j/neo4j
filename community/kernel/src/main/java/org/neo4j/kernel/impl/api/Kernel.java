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

import static java.util.Collections.synchronizedList;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.OldTxStateBridgeImpl;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
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

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for
 * consumption by
 * both the Beans API, Cypher, and any other components that want to interface with the underlying database.
 * <p/>
 * This is currently in an intermediate phase, where for many features you still have to use the beans API. To use this
 * implementation together with the beans API (eg. perform operations within the same transactions), then you should
 * use the beans API to start transactions, and use the Beans2KernelTransition class to get an {@link StatementContext}
 * that is hooked into that transaction.
 * <p/>
 * The cake:
 * <p/>
 * <ol>
 * <li>Locking</li>
 * <li>Constraint evaluation</li>
 * <li>Transaction state</li>
 * <li>Caching</li>
 * <li>Store</li>
 * </ol>
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final AbstractTransactionManager transactionManager;
    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;
    private final XaDataSourceManager dataSourceManager;
    private final LockManager lockManager;
    private final DependencyResolver dependencyResolver;
    private final SchemaCache schemaCache;
    private final UpdateableSchemaState schemaState;
    private final StatementContextOwners statementContextOwners = new StatementContextOwners();
    private SchemaIndexProviderMap providerMap = null;

    // These non-final components are all circular dependencies in various configurations.
    // As we work towards refactoring the old kernel, we should work to remove these.
    private IndexingService indexService;
    private NeoStore neoStore;
    private NodeManager nodeManager;
    private PersistenceCache persistenceCache;

    public Kernel( AbstractTransactionManager transactionManager, PropertyIndexManager propertyIndexManager,
                   PersistenceManager persistenceManager, XaDataSourceManager dataSourceManager,
                   LockManager lockManager,
                   SchemaCache schemaCache, UpdateableSchemaState schemaState,
                   DependencyResolver dependencyResolver )
    {
        this.transactionManager = transactionManager;
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
        this.dataSourceManager = dataSourceManager;
        this.lockManager = lockManager;
        this.dependencyResolver = dependencyResolver;
        this.schemaCache = schemaCache;
        this.schemaState = schemaState;
    }

    @Override
    public void start() throws Throwable
    {
        nodeManager = dependencyResolver.resolveDependency( NodeManager.class );

        dataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( isNeoDataSource( ds ) )
                {
                    neoStore = ((NeoStoreXaDataSource) ds).getNeoStore();
                    indexService = ((NeoStoreXaDataSource) ds).getIndexService();
                    providerMap = ((NeoStoreXaDataSource) ds).getProviderMap();
                    for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAll() ) )
                    {
                        schemaCache.addSchemaRule( schemaRule );
                    }

                    persistenceCache = new PersistenceCache( new NodeCacheLoader( neoStore.getNodeStore() ) );
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
    }

    @Override
    public TransactionContext newTransactionContext()
    {
        // I/O
        // TODO The store layer should depend on a clean abstraction of the data, not on all the XXXManagers from the
        // old code base
        TransactionContext result = new StoreTransactionContext( propertyIndexManager, nodeManager, neoStore,
                indexService );
        // + Transaction life cycle
        // XXX: This is disabled during transition phase, we are still using the legacy transaction management stuff
        //result = new TransactionLifecycleTransactionContext( result, transactionManager, propertyIndexManager,
        // persistenceManager, cache );

        // + Transaction state and Caching
        result = new StateHandlingTransactionContext( result, newTxState(), persistenceCache,
                transactionManager.getTransactionState(), schemaCache, schemaState, nodeManager );
        // + Constraints evaluation
        result = new ConstraintEvaluatingTransactionContext( result );
        // + Locking
        result = new LockingTransactionContext( result, lockManager, transactionManager );
        // + Single statement at a time
        result = new ReferenceCountingTransactionContext( result );

        // done
        return result;
    }

    @Override
    public StatementContext newReadOnlyStatementContext()
    {
        return statementContextOwners.get().getStatementContext();
    }

    private StatementContext createReadOnlyStatementContext()
    {
        // I/O
        StatementContext result = new StoreStatementContext( propertyIndexManager, nodeManager,
                neoStore, indexService, new IndexReaderFactory.Caching( indexService ) );

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
        SchemaOperations schemaOps = new SchemaStateOperations( inner, schemaState );
        return new CompositeStatementContext( inner, schemaOps );
    }

    private TxState newTxState()
    {
        return new TxState(
                new OldTxStateBridgeImpl( nodeManager, transactionManager.getTransactionState() ),
                persistenceManager,
                new TxState.IdGeneration()
                {
                    @Override
                    public long newSchemaRuleId()
                    {
                        return neoStore.getSchemaStore().nextId();
                    }
                },
                providerMap
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
