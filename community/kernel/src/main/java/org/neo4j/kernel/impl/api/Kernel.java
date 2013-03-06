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

import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
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
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for consumption by
 * both the Beans API, Cypher, and any other components that want to interface with the underlying database.
 *
 * This is currently in an intermediate phase, where for many features you still have to use the beans API. To use this
 * implementation together with the beans API (eg. perform operations within the same transactions), then you should
 * use the beans API to start transactions, and use the Beans2KernelTransition class to get an {@link StatementContext}
 * that is hooked into that transaction.
 * 
 * The cake:
 * 
 * <ol>
 *   <li>Locking</li>
 *   <li>Constraint evaluation</li>
 *   <li>Transaction state</li>
 *   <li>Caching</li>
 *   <li>Store</li>
 * </ol>
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final AbstractTransactionManager transactionManager;
    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;
    private final XaDataSourceManager dataSourceManager;
    private final LockManager lockManager;
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private IndexingService indexService;
    private NeoStore neoStore;

    public Kernel( AbstractTransactionManager transactionManager, PropertyIndexManager propertyIndexManager,
            PersistenceManager persistenceManager, XaDataSourceManager dataSourceManager, LockManager lockManager,
            SchemaCache schemaCache )
    {
        this.transactionManager = transactionManager;
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
        this.dataSourceManager = dataSourceManager;
        this.lockManager = lockManager;
        this.persistenceCache = new PersistenceCache( new NodeCacheLoader( persistenceManager ) );
        this.schemaCache = schemaCache;
    }
    
    @Override
    public void start() throws Throwable
    {
        dataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( isNeoDataSource( ds ) )
                {
                    neoStore = ((NeoStoreXaDataSource) ds).getNeoStore();
                    indexService = ((NeoStoreXaDataSource) ds).getIndexService();
                    for ( SchemaRule schemaRule : neoStore.getSchemaStore().loadAll() )
                        schemaCache.addSchemaRule( schemaRule );
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
    public TransactionContext newTransactionContext()
    {
        // I/O
        // TODO figure out another way to get access to the PropertyStore, or to not having to pass it in
        TransactionContext result = new TemporaryLabelAsPropertyTransactionContext( propertyIndexManager,
                persistenceManager, neoStore, indexService );
        // + Transaction life cycle
        // XXX: This is disabled during transition phase, we are still using the legacy transaction management stuff
        //result = new TransactionLifecycleTransactionContext( result, transactionManager, propertyIndexManager, persistenceManager, cache );

        // + Transaction state and Caching
        result = new StateHandlingTransactionContext( result, persistenceCache,
                transactionManager.getTransactionState(), schemaCache );
        // + Constraints evaluation
        result = new ConstraintEvaluatingTransactionContext( result );
        // + Locking
        result = new LockingTransactionContext( result, lockManager, transactionManager );
        // + Single statement at a time
        result = new SingleStatementTransactionContext( result );
        
        // done
        return result;
    }

    @Override
    public StatementContext newReadOnlyStatementContext()
    {
        // I/O
        StatementContext result = new StoreStatementContext( propertyIndexManager,
                persistenceManager, neoStore, indexService );
        // + Cache
        result = new CachingStatementContext( result, persistenceCache, schemaCache );
        // + Read only access
        result = new ReadOnlyStatementContext( result );

        return result;
    }
}
