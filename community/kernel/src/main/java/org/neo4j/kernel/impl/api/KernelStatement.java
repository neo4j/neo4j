/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.StorageStatement;

public class KernelStatement implements TxStateHolder, Statement
{
    protected final Locks.Client locks;
    protected final TxStateHolder txStateHolder;
    private final StorageStatement storeStatement;
    private final KernelTransactionImplementation transaction;
    private final OperationsFacade facade;
    private int referenceCount;
    private boolean closed;

    public KernelStatement( KernelTransactionImplementation transaction,
            TxStateHolder txStateHolder, Locks.Client locks,
            StatementOperationParts operations, StorageStatement storeStatement, Procedures procedures )
    {
        this.transaction = transaction;
        this.locks = locks;
        this.txStateHolder = txStateHolder;
        this.storeStatement = storeStatement;
        this.facade = new OperationsFacade( transaction, this, operations, procedures );
    }

    @Override
    public ReadOperations readOperations()
    {
        transaction.verifyReadTransaction();
        return facade;
    }

    @Override
    public TokenWriteOperations tokenWriteOperations()
    {
        return facade;
    }

    @Override
    public DataWriteOperations dataWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        transaction.verifyDataWriteTransaction();
        return facade;
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        transaction.verifySchemaWriteTransaction();
        return facade;
    }

    @Override
    public TransactionState txState()
    {
        return txStateHolder.txState();
    }

    @Override
    public LegacyIndexTransactionState legacyIndexTxState()
    {
        return txStateHolder.legacyIndexTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txStateHolder.hasTxStateWithChanges();
    }

    @Override
    public void close()
    {
        if ( !closed && release() )
        {
            closed = true;
            cleanupResources();
        }
    }

    void assertOpen()
    {
        if ( closed )
        {
            throw new NotInTransactionException( "The statement has been closed." );
        }
        if ( transaction.shouldBeTerminated() )
        {
            throw new TransactionTerminatedException();
        }
    }

    public Locks.Client locks()
    {
        return locks;
    }

    final void acquire()
    {
        referenceCount++;
    }

    private boolean release()
    {
        referenceCount--;

        return (referenceCount == 0);
    }

    final void forceClose()
    {
        if ( !closed )
        {
            closed = true;
            referenceCount = 0;

            cleanupResources();
        }
    }

    private void cleanupResources()
    {
        storeStatement.close();
        transaction.releaseStatement( this );
    }

    public StorageStatement getStoreStatement()
    {
        return storeStatement;
    }
}
