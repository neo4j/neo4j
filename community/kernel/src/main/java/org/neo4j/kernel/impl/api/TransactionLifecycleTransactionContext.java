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

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class TransactionLifecycleTransactionContext implements TransactionContext
{
    private final TransactionContext actual;
    private final AbstractTransactionManager transactionManager;
    private final Transaction transaction;

    public TransactionLifecycleTransactionContext( TransactionContext actual,
            AbstractTransactionManager transactionManager)
    {
        this.actual = actual;
        this.transactionManager = transactionManager;
        this.transaction = getOrBeginTransaction();
    }

    private Transaction getOrBeginTransaction()
    {
        try
        {
            javax.transaction.Transaction tx = transactionManager.getTransaction();
            if ( tx != null )
            {
                return new PlaceboTransaction( transactionManager, null );
            }
            
            transactionManager.begin();
            return new TopLevelTransaction( transactionManager, null );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( "Couldn't get transaction", e );
        }
        catch ( NotSupportedException e )
        {
            throw new TransactionFailureException( "Couldn't begin transaction", e );
        }
    }

    @Override
    public StatementContext newStatementContext()
    {
        return actual.newStatementContext();
    }

    @Override
    public void success()
    {
        transaction.success();
    }

    @Override
    public void finish()
    {
        // - flush changes from tx state to the store
        // - tx.finish()
        transaction.finish();
    }

    @Override
    public void failure()
    {
        transaction.failure();
    }
}
