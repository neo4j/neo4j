/*
 * Copyright (c) "Neo4j"
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;

import static java.lang.Math.subtractExact;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.trace_tx_statements;

/**
 * A resource efficient implementation of {@link Statement}. Designed to be reused within a
 * {@link KernelTransactionImplementation} instance, even across transactions since this instances itself
 * doesn't hold essential state. Usage:
 *
 * <ol>
 * <li>Construct {@link KernelStatement} when {@link KernelTransactionImplementation} is constructed</li>
 * <li>For every transaction...</li>
 * <li>Call {@link #initialize(Locks.Client, CursorContext, long)} which makes this instance
 * full available and ready to use. Call when the {@link KernelTransactionImplementation} is initialized.</li>
 * <li>Alternate {@link #acquire()} / {@link #close()} when acquiring / closing a statement for the transaction...
 * Temporarily asymmetric number of calls to {@link #acquire()} / {@link #close()} is supported, although in
 * the end an equal number of calls must have been issued.</li>
 * <li>To be safe call {@link #forceClose()} at the end of a transaction to force a close of the statement,
 * even if there are more than one current call to {@link #acquire()}. This instance is now again ready
 * to be {@link #initialize(Locks.Client, CursorContext, long)}  initialized} and used for the transaction
 * instance again, when it's initialized.</li>
 * </ol>
 */
public class KernelStatement extends CloseableResourceManager implements Statement, AssertOpen
{
    private static final int EMPTY_COUNTER = 0;
    private static final int STATEMENT_TRACK_HISTORY_MAX_SIZE = 100;
    private static final Deque<StackTraceElement[]> EMPTY_STATEMENT_HISTORY = new ArrayDeque<>( 0 );

    private final QueryRegistry queryRegistry;
    private final KernelTransactionImplementation transaction;
    private final NamedDatabaseId namedDatabaseId;
    private final boolean traceStatements;
    private final boolean trackStatementClose;
    private Locks.Client lockClient;
    private CursorContext cursorContext = CursorContext.NULL;
    private int referenceCount;
    private volatile ExecutingQuery executingQuery;
    private final LockTracer systemLockTracer;
    private final Deque<StackTraceElement[]> statementOpenCloseCalls;
    private final ClockContext clockContext;
    private long initialStatementHits;
    private long initialStatementFaults;

    public KernelStatement( KernelTransactionImplementation transaction, LockTracer systemLockTracer, ClockContext clockContext,
            AtomicReference<CpuClock> cpuClockRef, NamedDatabaseId namedDatabaseId, Config config )
    {
        this.transaction = transaction;
        this.queryRegistry = new StatementQueryRegistry( this, clockContext.systemClock(), cpuClockRef, config );
        this.systemLockTracer = systemLockTracer;
        this.traceStatements = config.get( trace_tx_statements );
        this.trackStatementClose = config.get( GraphDatabaseInternalSettings.track_tx_statement_close );
        this.statementOpenCloseCalls = traceStatements ? new ArrayDeque<>() : EMPTY_STATEMENT_HISTORY;
        this.clockContext = clockContext;
        this.namedDatabaseId = namedDatabaseId;
    }

    public QueryRegistry queryRegistration()
    {
        return queryRegistry;
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
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

    public void initialize( Locks.Client lockClient, CursorContext cursorContext, long startTimeMillis )
    {
        this.lockClient = lockClient;
        this.cursorContext = cursorContext;
        this.clockContext.initializeTransaction( startTimeMillis );
    }

    public Locks.Client locks()
    {
        return lockClient;
    }

    public LockTracer lockTracer()
    {
        LockTracer tracer = executingQuery != null ? executingQuery.lockTracer() : null;
        return tracer == null ? systemLockTracer : systemLockTracer.combine( tracer );
    }

    public long getHits()
    {
        return isAcquired() ? subtractExact( cursorContext.getCursorTracer().hits(), initialStatementHits ) : EMPTY_COUNTER;
    }

    public long getFaults()
    {
        return isAcquired() ? subtractExact( cursorContext.getCursorTracer().faults(), initialStatementFaults ) : EMPTY_COUNTER;
    }

    public final void acquire()
    {
        if ( referenceCount++ == 0 )
        {
            clockContext.initializeStatement();
            var cursorTracer = cursorContext.getCursorTracer();
            this.initialStatementHits = cursorTracer.hits();
            this.initialStatementFaults = cursorTracer.faults();
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
            if ( trackStatementClose && transaction.isSuccess() )
            {
                String message = getStatementNotClosedMessage( leakedStatements );
                throw new StatementNotClosedException( message, statementOpenCloseCalls );
            }
        }
    }

    private String getStatementNotClosedMessage( int leakedStatements )
    {
        String additionalInstruction = traceStatements ? StringUtils.EMPTY :
                                       format( " To see statement open/close stack traces please set '%s' setting to true", trace_tx_statements.name() );
        return format( "Statements were not correctly closed. Number of leaked statements: %d.%s", leakedStatements, additionalInstruction );
    }

    final String username()
    {
        return transaction.securityContext().subject().username();
    }

    final Optional<ExecutingQuery> executingQuery()
    {
        return Optional.ofNullable( executingQuery );
    }

    final void startQueryExecution( ExecutingQuery query )
    {
        query.setPreviousQuery( executingQuery );
        this.executingQuery = query;
    }

    final void stopQueryExecution( ExecutingQuery executingQuery )
    {
        this.executingQuery = executingQuery.getPreviousQuery();
        transaction.getStatistics().addWaitingTime( executingQuery.reportedWaitingTimeNanos() );
    }

    private void cleanupResources()
    {
        // closing is done by KTI
        transaction.releaseStatementResources();
        executingQuery = null;
        initialStatementHits = EMPTY_COUNTER;
        initialStatementFaults = EMPTY_COUNTER;
        closeAllCloseableResources();
    }

    public KernelTransactionImplementation getTransaction()
    {
        return transaction;
    }

    private void recordOpenCloseMethods()
    {
        if ( traceStatements )
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
            PrintStream printStream = new PrintStream( out, false, StandardCharsets.UTF_8 );
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
            return out.toString( StandardCharsets.UTF_8 );
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
