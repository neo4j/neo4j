/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.storageengine.api.StorageStatement;

import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.toggle;

/**
 * A resource efficient implementation of {@link Statement}. Designed to be reused within a
 * {@link KernelTransactionImplementation} instance, even across transactions since this instances itself
 * doesn't hold essential state. Usage:
 *
 * <ol>
 * <li>Construct {@link KernelStatement} when {@link KernelTransactionImplementation} is constructed</li>
 * <li>For every transaction...</li>
 * <li>Call {@link #initialize(StatementLocks, PageCursorTracer)} which makes this instance
 * full available and ready to use. Call when the {@link KernelTransactionImplementation} is initialized.</li>
 * <li>Alternate {@link #acquire()} / {@link #close()} when acquiring / closing a statement for the transaction...
 * Temporarily asymmetric number of calls to {@link #acquire()} / {@link #close()} is supported, although in
 * the end an equal number of calls must have been issued.</li>
 * <li>To be safe call {@link #forceClose()} at the end of a transaction to force a close of the statement,
 * even if there are more than one current call to {@link #acquire()}. This instance is now again ready
 * to be {@link #initialize(StatementLocks, PageCursorTracer)}  initialized} and used for the transaction
 * instance again, when it's initialized.</li>
 * </ol>
 */
public class KernelStatement extends CloseableResourceManager implements TxStateHolder, Statement, AssertOpen
{
    private static final boolean TRACK_STATEMENTS = flag( KernelStatement.class, "trackStatements", false );
    private static final boolean RECORD_STATEMENTS_TRACES = flag( KernelStatement.class, "recordStatementsTraces", false );
    private static final int STATEMENT_TRACK_HISTORY_MAX_SIZE = 100;
    private static final Deque<StackTraceElement[]> EMPTY_STATEMENT_HISTORY = new ArrayDeque<>( 0 );

    private final TxStateHolder txStateHolder;
    private final StorageStatement storeStatement;
    private final KernelTransactionImplementation transaction;
    private final OperationsFacade facade;
    private StatementLocks statementLocks;
    private PageCursorTracer pageCursorTracer = PageCursorTracer.NULL;
    private int referenceCount;
    private volatile ExecutingQueryList executingQueryList;
    private final LockTracer systemLockTracer;
    private final Deque<StackTraceElement[]> statementOpenCloseCalls;
    private final ClockContext clockContext;
    private final VersionContextSupplier versionContextSupplier;

    public KernelStatement( KernelTransactionImplementation transaction,
            TxStateHolder txStateHolder,
            StorageStatement storeStatement,
            LockTracer systemLockTracer,
            StatementOperationParts statementOperations,
            ClockContext clockContext,
            VersionContextSupplier versionContextSupplier )
    {
        this.transaction = transaction;
        this.txStateHolder = txStateHolder;
        this.storeStatement = storeStatement;
        this.facade = new OperationsFacade( this, statementOperations );
        this.executingQueryList = ExecutingQueryList.EMPTY;
        this.systemLockTracer = systemLockTracer;
        this.statementOpenCloseCalls = RECORD_STATEMENTS_TRACES ? new ArrayDeque<>() : EMPTY_STATEMENT_HISTORY;
        this.clockContext = clockContext;
        this.versionContextSupplier = versionContextSupplier;
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
    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return txStateHolder.explicitIndexTxState();
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
        recordOpenCloseMethods();
    }

    @Override
    public void assertOpen()
    {
        if ( referenceCount == 0 )
        {
            throw new NotInTransactionException( "The statement has been closed." );
        }

        Optional<Status> terminationReason = transaction.getReasonIfTerminated();
        terminationReason.ifPresent( status ->
        {
            throw new TransactionTerminatedException( status );
        } );
    }

    public void initialize( StatementLocks statementLocks, PageCursorTracer pageCursorCounters )
    {
        this.statementLocks = statementLocks;
        this.pageCursorTracer = pageCursorCounters;
        this.clockContext.initializeTransaction();
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
            clockContext.initializeStatement();
        }
        recordOpenCloseMethods();
    }

    final boolean isAcquired()
    {
        return referenceCount > 0;
    }

    final void forceClose()
    {
        if ( referenceCount > 0 )
        {
            int leakedStatements = referenceCount;
            referenceCount = 0;
            cleanupResources();
            if ( TRACK_STATEMENTS && transaction.isSuccess() )
            {
                String message = getStatementNotClosedMessage( leakedStatements );
                throw new StatementNotClosedException( message, statementOpenCloseCalls );
            }
        }
        pageCursorTracer.reportEvents();
    }

    private String getStatementNotClosedMessage( int leakedStatements )
    {
        String additionalInstruction = RECORD_STATEMENTS_TRACES ? StringUtils.EMPTY :
                                       format(" To see statement open/close stack traces please pass '%s' to your JVM" +
                                                       " or enable corresponding feature toggle.",
                                       toggle( KernelStatement.class, "recordStatementsTraces", Boolean.TRUE ) );
        return format( "Statements were not correctly closed. Number of leaked statements: %d.%s", leakedStatements,
                additionalInstruction );
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
        transaction.getStatistics().addWaitingTime( executingQuery.reportedWaitingTimeNanos() );
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
        closeAllCloseableResources();
    }

    public KernelTransactionImplementation getTransaction()
    {
        return transaction;
    }

    public VersionContext getVersionContext()
    {
        return versionContextSupplier.getVersionContext();
    }

    void assertAllows( Function<AccessMode,Boolean> allows, String mode )
    {
      transaction.assertAllows( allows, mode );
    }

    private void recordOpenCloseMethods()
    {
        if ( RECORD_STATEMENTS_TRACES )
        {
            if ( statementOpenCloseCalls.size() > STATEMENT_TRACK_HISTORY_MAX_SIZE )
            {
                statementOpenCloseCalls.pop();
            }
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            statementOpenCloseCalls.add( Arrays.copyOfRange(stackTrace, 2, stackTrace.length) );
        }
    }

    public ClockContext clocks()
    {
        return clockContext;
    }

    static class StatementNotClosedException extends IllegalStateException
    {

        StatementNotClosedException( String s, Deque<StackTraceElement[]> openCloseTraces )
        {
            super( s );
            this.addSuppressed( new StatementTraceException( buildMessage( openCloseTraces ) ) );
        }

        private static String buildMessage( Deque<StackTraceElement[]> openCloseTraces )
        {
            if ( openCloseTraces.isEmpty() )
            {
                return StringUtils.EMPTY;
            }
            int separatorLength = 80;
            String paddingString = "=";

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream( out );
            printStream.println();
            printStream.println( "Last " + STATEMENT_TRACK_HISTORY_MAX_SIZE + " statements open/close stack traces are:" );
            int element = 0;
            for ( StackTraceElement[] traceElements : openCloseTraces )
            {
                printStream.println( StringUtils.center( "*StackTrace " + element + "*", separatorLength, paddingString ) );
                for ( StackTraceElement traceElement : traceElements )
                {
                    printStream.println( "\tat " + traceElement );
                }
                printStream.println( StringUtils.center( "", separatorLength, paddingString ) );
                printStream.println();
                element++;
            }
            printStream.println( "All statement open/close stack traces printed." );
            return out.toString();
        }

        private static class StatementTraceException extends RuntimeException
        {
            StatementTraceException( String message )
            {
                super( message );
            }

            @Override
            public synchronized Throwable fillInStackTrace()
            {
                return this;
            }
        }
    }
}
