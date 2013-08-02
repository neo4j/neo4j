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

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.WritableStatementState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class StoreKernelTransaction implements KernelTransaction
{
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final AbstractTransactionManager transactionManager;
    private final NeoStore neoStore;
    private final IndexingService indexingService;
    private final LabelTokenHolder labelTokenHolder;
    private final PersistenceManager persistenceManager;

    public StoreKernelTransaction( AbstractTransactionManager transactionManager,
                                    PersistenceManager persistenceManager,
                                    PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                                    NeoStore neoStore, IndexingService indexingService )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.neoStore = neoStore;
        this.indexingService = indexingService;
    }

    @Override
    public StatementOperationParts newStatementOperations()
    {
        StoreStatementOperations context = new StoreStatementOperations( propertyKeyTokenHolder, labelTokenHolder,
                new SchemaStorage( neoStore.getSchemaStore() ), neoStore, persistenceManager,
                indexingService );
        return new StatementOperationParts( context, context, context, context, context, null, null )
            .additionalPart( AuxiliaryStoreOperations.class, context );
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

    @Override
    public StatementState newStatementState()
    {
        WritableStatementState result = new WritableStatementState();
        result.provide( new IndexReaderFactory.Caching( indexingService ) );
        return result;
    }
}
