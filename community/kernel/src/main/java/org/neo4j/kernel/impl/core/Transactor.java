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
package org.neo4j.kernel.impl.core;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.BeginTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class Transactor
{
    public interface Work<RESULT, FAILURE extends KernelException>
    {
        RESULT perform( Statement statement ) throws FAILURE;
    }

    private final TransactionManager txManager;
    private final PersistenceManager persistenceManager;

    public Transactor( TransactionManager txManager, PersistenceManager persistenceManager )
    {
        this.txManager = txManager;
        this.persistenceManager = persistenceManager;
    }

    public <RESULT, FAILURE extends KernelException> RESULT execute( Work<RESULT, FAILURE> work )
            throws FAILURE, TransactionalException
    {
        Transaction previousTransaction = suspendTransaction();
        try
        {
            beginTransaction();
            KernelTransaction tx = persistenceManager.currentKernelTransactionForWriting();
            boolean success = false;
            try
            {
                RESULT result;
                try ( Statement statement = tx.acquireStatement() )
                {
                    result = work.perform( statement );
                }
                success = true;
                return result;
            }
            finally
            {
                if ( success )
                {
                    txManager.commit();
                }
                else
                {
                    txManager.rollback();
                }
            }
        }
        catch ( HeuristicMixedException | RollbackException | HeuristicRollbackException | SystemException |
               TransactionalException failure )
        {
            previousTransaction = null; // the transaction manager threw an exception, don't resume previous.
            throw new TransactionFailureException(failure);
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
        catch ( NotSupportedException | SystemException e )
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
