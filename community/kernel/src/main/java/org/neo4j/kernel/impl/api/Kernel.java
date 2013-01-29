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
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for consumption by
 * both the Beans API, Cypher, and any other components that want to interface with the underlying database.
 *
 * This is currently in an intermediate phase, where for many features you still have to use the beans API. To use this
 * implementation together with the beans API (eg. perform operations within the same transactions), then you should
 * use the beans API to start transactions, and use the Beans2KernelTransition class to get an {@link StatementContext}
 * that is hooked into that transaction.
 */
public class Kernel implements KernelAPI
{
    private final AbstractTransactionManager transactionManager;
    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;
    private final XaDataSourceManager dataSourceManager;
    private final LockManager lockManager;
    private final PersistenceCache cache;

    public Kernel( AbstractTransactionManager transactionManager, PropertyIndexManager propertyIndexManager,
            PersistenceManager persistenceManager, XaDataSourceManager dataSourceManager, LockManager lockManager )
    {
        this.transactionManager = transactionManager;
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
        this.dataSourceManager = dataSourceManager;
        this.lockManager = lockManager;
        this.cache = new PersistenceCache( new TemporaryLabelAsPropertyLoader( persistenceManager ) );
    }
    
    @Override
    public TransactionContext newTransactionContext()
    {
        // I/O
        // TODO figure out another way to get access to the PropertyStore, or to not having to pass it in
        TransactionContext result = new TemporaryLabelAsPropertyTransactionContext( propertyIndexManager,
                persistenceManager, propertyStore() );
        // + Transaction life cycle
        // XXX: This is disabled during transition phase, we are still using the legacy transaction management stuff
        //result = new TransactionLifecycleTransactionContext( result, transactionManager, propertyIndexManager, persistenceManager, cache );

        // + Transaction state and Caching
        result = new StateHandlingTransactionContext( result, cache );
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
        StatementContext result = new TemporaryLabelAsPropertyStatementContext( propertyIndexManager,
                persistenceManager, propertyStore() );
        // + Cache
        result = new CachingStatementContext( result, cache );
        // + Read only access
        result = new ReadOnlyStatementContext( result );

        return result;
    }
    
    private PropertyStore propertyStore()
    {
        return dataSourceManager.getNeoStoreDataSource().getNeoStore().getPropertyStore();
    }
}
