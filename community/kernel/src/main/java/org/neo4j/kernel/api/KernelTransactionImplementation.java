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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.LegacyKernelOperations;

/**
 * This class should replace the {@link KernelTransaction} interface, and take its name, as soon as
 * {@code TransitionalTxManagementKernelTransaction} is gone from {@code server}.
 */
public abstract class KernelTransactionImplementation implements KernelTransaction
{
    private TransactionType transactionType = TransactionType.ANY;
    final StatementOperationParts operations;
    private boolean closing, closed;
    final LegacyKernelOperations legacyKernelOperations;

    protected KernelTransactionImplementation( StatementOperationParts operations,
                                               LegacyKernelOperations legacyKernelOperations )
    {
        this.operations = operations;
        this.legacyKernelOperations = legacyKernelOperations;
    }

    public BaseStatement acquireBaseStatement()
    {
        assertOpen();
        return new BaseStatement( this, acquireStatement() );
    }

    public DataStatement acquireDataStatement() throws InvalidTransactionTypeException
    {
        assertOpen();
        transactionType = transactionType.upgradeToDataTransaction();
        return new DataStatement( this, acquireStatement() );
    }

    public SchemaStatement acquireSchemaStatement() throws InvalidTransactionTypeException
    {
        assertOpen();
        transactionType = transactionType.upgradeToSchemaTransaction();
        return new SchemaStatement( this, acquireStatement() );
    }

    public void commit() throws TransactionFailureException
    {
        beginClose();
        doCommit();
        close();
    }

    public void rollback() throws TransactionFailureException
    {
        beginClose();
        doRollback();
        close();
    }

    @Override
    public <RESULT, FAILURE extends KernelException> RESULT execute( MicroTransaction<RESULT, FAILURE> transaction )
            throws FAILURE
    {
        Statement statement = acquireStatement();
        try
        {
            return transaction.work.perform( operations, statement );
        }
        finally
        {
            releaseStatement( statement );
        }
    }

    protected abstract void doCommit() throws TransactionFailureException;

    protected abstract void doRollback() throws TransactionFailureException;

    protected abstract Statement newStatement();

    /** Implements reusing the same underlying {@link Statement} for overlapping statements. */
    private Statement currentStatement;

    private Statement acquireStatement()
    {
        if ( currentStatement == null )
        {
            currentStatement = newStatement();
        }
        currentStatement.acquire();
        return currentStatement;
    }

    void releaseStatement( Statement statement )
    {
        assert currentStatement == statement;
        if ( statement.release() )
        {
            currentStatement = null;
            statement.close();
        }
    }

    private void close()
    {
        assertOpen();
        closed = true;
        if ( currentStatement != null )
        {
            currentStatement.forceClose();
            currentStatement = null;
        }
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This transaction has already been completed." );
        }
    }

    private void beginClose()
    {
        assertOpen();
        if ( closing )
        {
            throw new IllegalStateException( "This transaction is already being closed." );
        }
        if ( currentStatement != null )
        {
            currentStatement.forceClose();
            currentStatement = null;
        }
        closing = true;
    }

    private enum TransactionType
    {
        ANY,
        DATA
                {
                    @Override
                    TransactionType upgradeToSchemaTransaction() throws InvalidTransactionTypeException
                    {
                        throw new InvalidTransactionTypeException(
                                "Cannot perform schema updates in a transaction that has performed data updates." );
                    }
                },
        SCHEMA
                {
                    @Override
                    TransactionType upgradeToDataTransaction() throws InvalidTransactionTypeException
                    {
                        throw new InvalidTransactionTypeException(
                                "Cannot perform data updates in a transaction that has performed schema updates." );
                    }
                };

        TransactionType upgradeToDataTransaction() throws InvalidTransactionTypeException
        {
            return DATA;
        }

        TransactionType upgradeToSchemaTransaction() throws InvalidTransactionTypeException
        {
            return SCHEMA;
        }
    }
}
