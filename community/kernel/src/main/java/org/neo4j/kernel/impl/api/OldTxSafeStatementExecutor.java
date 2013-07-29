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

import org.neo4j.helpers.Function2;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;

import static java.lang.String.format;

public class OldTxSafeStatementExecutor
{
    private final AbstractTransactionManager transactionManager;
    private final String executorDescription;

    public OldTxSafeStatementExecutor( String executorDescription, AbstractTransactionManager transactionManager )
    {
        this.executorDescription = executorDescription;
        this.transactionManager = transactionManager;
    }

    public <T> T executeSingleStatement( Function2<StatementState, StatementOperationParts, T> work )
            throws TransactionFailureException
    {
        try
        {
            Transaction runningTransaction = transactionManager.suspend();
            transactionManager.begin( ForceMode.unforced );
            try
            {
                StatementState statementState = transactionManager.newStatement();
                StatementOperationParts statementLogic = statementOperations();
                boolean success = false;
                try
                {
                    T result = work.apply( statementState, statementLogic );
                    success = true;
                    return result;
                }
                finally
                {
                    statementLogic.close( statementState );
                    if ( success )
                    {
                        try
                        {
                            transactionManager.commit();
                        }
                        catch ( Throwable t )
                        {
                            try
                            {
                                transactionManager.rollback();
                            }
                            catch ( Throwable rollbackError )
                            {
                                throw new org.neo4j.graphdb.TransactionFailureException(
                                    exceptionMessage( "Unable to execute read only statement" ), rollbackError );
                            }
                            throw new org.neo4j.graphdb.TransactionFailureException(
                                    exceptionMessage( "Unable to execute read only statement" ) , t );
                        }
                    }
                    else
                    {
                        try
                        {
                            transactionManager.rollback();
                        }
                        catch ( Throwable t )
                        {
                            throw new org.neo4j.graphdb.TransactionFailureException(
                                    exceptionMessage( "Unable to execute read only statement" ) , t );

                        }
                    }
                }
            }
            finally
            {
                if ( runningTransaction != null )
                {
                    transactionManager.resume( runningTransaction );
                }
            }
        }
        catch ( SystemException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException(
                exceptionMessage( "Unable to resume or suspend running transaction" ), e );
        }
        catch ( InvalidTransactionException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException(
                exceptionMessage( "Unable to resume or suspend running transaction" ), e );
        }
        catch ( NotSupportedException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException(
                exceptionMessage( "Unable to resume or suspend running transaction" ), e );
        }
    }

    protected StatementOperationParts statementOperations()
    {
        KernelTransaction kernelTransaction = transactionManager.getKernelTransaction();
        return kernelTransaction.newStatementOperations();
    }

    private String exceptionMessage( String messageStart )
    {
        return format( "%s (statement executor: %s)", messageStart, toString() );
    }

    public String toString()
    {
        return executorDescription;
    }
}
