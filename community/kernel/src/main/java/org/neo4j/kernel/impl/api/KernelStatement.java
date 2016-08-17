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
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.StorageStatement;

/**
 * A resource efficient implementation of {@link Statement}. Designed to be reused within a
 * {@link KernelTransactionImplementation} instance, even across transactions since this instances itself
 * doesn't hold essential state. Usage:
 *
 * <ol>
 * <li>Construct {@link KernelStatement} when {@link KernelTransactionImplementation} is constructed</li>
 * <li>For every transaction...</li>
 * <li>Call {@link #initialize(StatementLocks)} which makes this instance
 * full available and ready to use. Call when the {@link KernelTransactionImplementation} is initialized.</li>
 * <li>Alternate {@link #acquire()} / {@link #close()} when acquiring / closing a statement for the transaction...
 * Temporarily asymmetric number of calls to {@link #acquire()} / {@link #close()} is supported, although in
 * the end an equal number of calls must have been issued.</li>
 * <li>To be safe call {@link #forceClose()} at the end of a transaction to force a close of the statement,
 * even if there are more than one current call to {@link #acquire()}. This instance is now again ready
 * to be {@link #initialize(StatementLocks)  initialized} and used for the transaction
 * instance again, when it's initialized.</li>
 * </ol>
 */
public class KernelStatement implements TxStateHolder, Statement
{
    private final TxStateHolder txStateHolder;
    private final StorageStatement storeStatement;
    private final KernelTransactionImplementation transaction;
    private final OperationsFacade facade;
    private StatementLocks statementLocks;
    private int referenceCount;

    public KernelStatement( KernelTransactionImplementation transaction,
            TxStateHolder txStateHolder,
            StatementOperationParts operations, StorageStatement storeStatement, Procedures procedures )
    {
        this.transaction = transaction;
        this.txStateHolder = txStateHolder;
        this.storeStatement = storeStatement;
        this.facade = new OperationsFacade( transaction, this, operations, procedures );
    }

    @Override
    public ReadOperations readOperations()
    {
        if( !transaction.mode().allowsReads() )
        {
            throw transaction.mode().onViolation(
                    String.format( "Read operations are not allowed for '%s'.", transaction.mode().name() ) );
        }
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
        if( !transaction.mode().allowsWrites() )
        {
            throw transaction.mode().onViolation(
                    String.format( "Write operations are not allowed for '%s'.", transaction.mode().name() ) );
        }
        transaction.upgradeToDataWrites();
        return facade;
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        if( !transaction.mode().allowsSchemaWrites() )
        {
            throw transaction.mode().onViolation(
                    String.format( "Schema operations are not allowed for '%s'.", transaction.mode().name() ) );
        }
        transaction.upgradeToSchemaWrites();
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
        // Check referenceCount > 0 since we allow multiple close calls,
        // i.e. ignore closing already closed statements
        if ( referenceCount > 0 && (--referenceCount == 0) )
        {
            cleanupResources();
        }
    }

    void assertOpen()
    {
        if ( referenceCount == 0 )
        {
            throw new NotInTransactionException( "The statement has been closed." );
        }

        Status terminationReason = transaction.getReasonIfTerminated();
        if ( terminationReason != null )
        {
            throw new TransactionTerminatedException( terminationReason );
        }
    }

    void initialize( StatementLocks statementLocks )
    {
        this.statementLocks = statementLocks;
    }

    public StatementLocks locks()
    {
        return statementLocks;
    }

    final void acquire()
    {
        if ( referenceCount++ == 0 )
        {
            storeStatement.acquire();
        }
    }

    final void forceClose()
    {
        if ( referenceCount > 0 )
        {
            referenceCount = 0;
            cleanupResources();
        }
    }

    private void cleanupResources()
    {
        storeStatement.release();
        // closing is done by KTI
    }

    public StorageStatement getStoreStatement()
    {
        return storeStatement;
    }
}
