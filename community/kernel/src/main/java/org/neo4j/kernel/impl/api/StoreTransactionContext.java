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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class StoreTransactionContext implements TransactionContext
{
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final AbstractTransactionManager transactionManager;
    private final NeoStore neoStore;
    private final IndexingService indexingService;
    private final LabelTokenHolder labelTokenHolder;
    private final NodeManager nodeManager;

    public StoreTransactionContext( AbstractTransactionManager transactionManager,
                                    PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                                    NodeManager nodeManager, NeoStore neoStore, IndexingService indexingService )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.transactionManager = transactionManager;
        this.nodeManager = nodeManager;
        this.neoStore = neoStore;
        this.indexingService = indexingService;
    }

    @Override
    public StatementContext newStatementContext()
    {
        return new StoreStatementContext( propertyKeyTokenHolder, labelTokenHolder, nodeManager,
                                          new SchemaStorage( neoStore.getSchemaStore() ), neoStore, indexingService,
                                          new IndexReaderFactory.Caching( indexingService ) );
    }

    @Override
    public void prepare()
    {
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        try
        {
            transactionManager.commit();
        }
        catch ( HeuristicMixedException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( HeuristicRollbackException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( RollbackException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( IllegalStateException e )
        {
            throw new TransactionFailureException( e );
        }
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        try
        {
            if ( transactionManager.getTransaction() != null )
            {
                transactionManager.rollback();
            }
        }
        catch ( IllegalStateException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( SecurityException e )
        {
            throw new TransactionFailureException( e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( e );
        }
    }
}
