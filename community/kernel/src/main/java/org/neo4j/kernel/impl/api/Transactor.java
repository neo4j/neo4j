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

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.BeginTransactionFailureException;
import org.neo4j.kernel.api.KernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.TransactionFailureException;
import org.neo4j.kernel.api.TransactionalException;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class Transactor
{
    public static interface Statement<RESULT, FAILURE extends KernelException>
    {
        RESULT perform( StatementContext statementContext ) throws FAILURE;
    }

    private final AbstractTransactionManager txManager;

    public Transactor( AbstractTransactionManager txManager )
    {
        this.txManager = txManager;
    }

    public <RESULT, FAILURE extends KernelException> RESULT execute( Statement<RESULT, FAILURE> statement )
            throws FAILURE, TransactionalException
    {
        Transaction previousTransaction = suspendTransaction();
        try
        {
            beginTransaction();
            @SuppressWarnings("deprecation")
            TransactionContext tx = txManager.getTransactionContext();
            boolean success = false;
            try
            {
                RESULT result;
                StatementContext context = tx.newStatementContext();
                try
                {
                    result = statement.perform( context );
                }
                finally
                {
                    context.close();
                }
                success = true;
                return result;
            }
            finally
            {
                if ( success )
                {
                    tx.commit();
                }
                else
                {
                    tx.rollback();
                }
            }
        }
        catch ( TransactionalException failure )
        {
            previousTransaction = null; // the transaction manager threw an exception, don't resume previous.
            throw failure;
        }
        finally
        {
            if ( previousTransaction != null )
            {
                resumeTransaction( previousTransaction );
            }
        }
    }

    private void beginTransaction() throws BeginTransactionFailureException
    {
        try
        {
            txManager.begin();
        }
        catch ( NotSupportedException e )
        {
            throw new BeginTransactionFailureException( e );
        }
        catch ( SystemException e )
        {
            throw new BeginTransactionFailureException( e );
        }
    }

    private Transaction suspendTransaction() throws TransactionFailureException
    {
        Transaction existingTransaction;
        try
        {
            existingTransaction = txManager.suspend();
        }
        catch ( SystemException failure )
        {
            throw new TransactionFailureException( failure );
        }
        return existingTransaction;
    }

    private void resumeTransaction( Transaction existingTransaction ) throws TransactionFailureException
    {
        try
        {
            txManager.resume( existingTransaction );
        }
        catch ( InvalidTransactionException failure )
        { // thrown from resume()
            throw new ThisShouldNotHappenError( "Tobias Lindaaker",
                                                "Transaction resumed in the same transaction manager as it was " +
                                                "suspended from should not be invalid. The Neo4j code base does not " +
                                                "throw InvalidTransactionException", failure );
        }
        catch ( SystemException failure )
        {
            throw new TransactionFailureException( failure );
        }
    }
}
