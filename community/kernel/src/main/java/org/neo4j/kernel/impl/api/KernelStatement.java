/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Optional;
import java.util.function.Function;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ExecutionStatisticsOperations;
import org.neo4j.kernel.api.ProcedureCallOperations;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.locking.LockTracer;
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
 * <li>Call {@link #initialize(StatementLocks, StatementOperationParts, PageCursorTracer)} which makes this instance
 * full available and ready to use. Call when the {@link KernelTransactionImplementation} is initialized.</li>
 * <li>Alternate {@link #acquire()} / {@link #close()} when acquiring / closing a statement for the transaction...
 * Temporarily asymmetric number of calls to {@link #acquire()} / {@link #close()} is supported, although in
 * the end an equal number of calls must have been issued.</li>
 * <li>To be safe call {@link #forceClose()} at the end of a transaction to force a close of the statement,
 * even if there are more than one current call to {@link #acquire()}. This instance is now again ready
 * to be {@link #initialize(StatementLocks, StatementOperationParts, PageCursorTracer)}  initialized} and used for the transaction
 * instance again, when it's initialized.</li>
 * </ol>
 */
public class KernelStatement implements TxStateHolder, Statement
{
    private final TxStateHolder txStateHolder;
    private final StorageStatement storeStatement;
    private final AccessCapability accessCapability;
    private final KernelTransactionImplementation transaction;
    private final OperationsFacade facade;
    private StatementLocks statementLocks;
    private PageCursorTracer pageCursorTracer = PageCursorTracer.NULL;
    private int referenceCount;
    private volatile ExecutingQueryList executingQueryList;
    private final LockTracer systemLockTracer;

    public KernelStatement( KernelTransactionImplementation transaction,
                            TxStateHolder txStateHolder,
                            StorageStatement storeStatement,
                            Procedures procedures,
                            AccessCapability accessCapability,
                            LockTracer systemLockTracer )
    {
        this.transaction = transaction;
        this.txStateHolder = txStateHolder;
        this.storeStatement = storeStatement;
        this.accessCapability = accessCapability;
        this.facade = new OperationsFacade( transaction, this, procedures );
        this.executingQueryList = ExecutingQueryList.EMPTY;
        this.systemLockTracer = systemLockTracer;
    }

    @Override
    public ReadOperations readOperations()
    {
        assertAllows( AccessMode::allowsReads, "Read" );
        return facade;
    }

    @Override
    public ProcedureCallOperations procedureCallOperations()
    {
        return facade;
    }

    @Override
    public ExecutionStatisticsOperations executionStatisticsOperations()
    {
        return facade;
    }

    @Override
    public TokenWriteOperations tokenWriteOperations()
    {
        accessCapability.assertCanWrite();

        return facade;
    }

    @Override
    public DataWriteOperations dataWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();

        assertAllows( AccessMode::allowsWrites, "Write" );
        transaction.upgradeToDataWrites();
        return facade;
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        accessCapability.assertCanWrite();

        assertAllows( AccessMode::allowsSchemaWrites, "Schema" );
        transaction.upgradeToSchemaWrites();
        return facade;
    }

    @Override
    public QueryRegistryOperations queryRegistration()
    {
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

        Optional<Status> terminationReason = transaction.getReasonIfTerminated();
        if ( terminationReason.isPresent() )
        {
            throw new TransactionTerminatedException( terminationReason.get() );
        }
    }

    public void initialize( StatementLocks statementLocks, StatementOperationParts operationParts,
            PageCursorTracer pageCursorCounters )
    {
        this.statementLocks = statementLocks;
        this.pageCursorTracer = pageCursorCounters;
        facade.initialize( operationParts );
    }

    public StatementLocks locks()
    {
        return statementLocks;
    }

    public LockTracer lockTracer()
    {
        LockTracer tracer = executingQueryList.top( ExecutingQuery::lockTracer );
        return tracer == null ? systemLockTracer : systemLockTracer.combine( tracer );
    }

    public PageCursorTracer getPageCursorTracer()
    {
        return pageCursorTracer;
    }

    public final void acquire()
    {
        if ( referenceCount++ == 0 )
        {
            storeStatement.acquire();
        }
    }

    final boolean isAcquired()
    {
        return referenceCount > 0;
    }

    final void forceClose()
    {
        if ( referenceCount > 0 )
        {
            referenceCount = 0;
            cleanupResources();
        }
        pageCursorTracer.reportEvents();
    }

    final String username()
    {
        return transaction.securityContext().subject().username();
    }

    final ExecutingQueryList executingQueryList()
    {
        return executingQueryList;
    }

    final void startQueryExecution( ExecutingQuery query )
    {
        this.executingQueryList = executingQueryList.push( query );
    }

    final void stopQueryExecution( ExecutingQuery executingQuery )
    {
        this.executingQueryList = executingQueryList.remove( executingQuery );
    }

    public StorageStatement getStoreStatement()
    {
        return storeStatement;
    }

    private void cleanupResources()
    {
        // closing is done by KTI
        storeStatement.release();
        executingQueryList = ExecutingQueryList.EMPTY;
    }

    public KernelTransactionImplementation getTransaction()
    {
        return transaction;
    }

    void assertAllows( Function<AccessMode,Boolean> allows, String mode )
    {
        AccessMode accessMode = transaction.securityContext().mode();
        if ( !allows.apply( accessMode ) )
        {
            throw accessMode.onViolation(
                    String.format( "%s operations are not allowed for %s.", mode,
                            transaction.securityContext().description() ) );
        }
    }
}
