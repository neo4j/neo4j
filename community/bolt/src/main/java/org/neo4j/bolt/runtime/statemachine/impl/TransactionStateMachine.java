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
package org.neo4j.bolt.runtime.statemachine.impl;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.virtual.MapValue;

import static org.apache.commons.collections.MapUtils.isEmpty;
import static org.neo4j.bolt.runtime.Bookmark.EMPTY_BOOKMARK;
import static org.neo4j.util.Preconditions.checkState;

public class TransactionStateMachine implements StatementProcessor
{
    private final TransactionStateMachineSPI spi;
    final MutableTransactionState ctx;
    State state = State.AUTO_COMMIT;
    private final String databaseName;

    public TransactionStateMachine( String databaseName, TransactionStateMachineSPI spi, AuthenticationResult authenticationResult, Clock clock )
    {
        this.spi = spi;
        ctx = new MutableTransactionState( authenticationResult, clock );
        this.databaseName = databaseName;
    }

    @Override
    public void beginTransaction( List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata ) throws KernelException
    {
        state = state.beginTransaction( ctx, spi, bookmarks, txTimeout, accessMode, txMetadata );
    }

    @Override
    public StatementMetadata run( String statement, MapValue params ) throws KernelException
    {
        return run( statement, params, List.of(), null, AccessMode.WRITE, Map.of() );
    }

    @Override
    public StatementMetadata run( String statement, MapValue params, List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode,
            Map<String,Object> txMetaData ) throws KernelException
    {
        state = state.run( ctx, spi, statement, params, bookmarks, txTimeout, accessMode, txMetaData );

        StatementMetadata metadata = ctx.lastStatementMetadata;
        ctx.lastStatementMetadata = null; // metadata should not be needed more than once
        return metadata;
    }

    @Override
    public Bookmark streamResult( int statementId, ResultConsumer resultConsumer ) throws Throwable
    {
        return state.streamResult( ctx, spi, statementId, resultConsumer );
    }

    @Override
    public Bookmark commitTransaction() throws KernelException
    {
        try
        {
            BoltTransaction tx = ctx.currentTransaction;
            state = state.commitTransaction( ctx, spi );
            return newestBookmark( spi, tx );
        }
        catch ( TransactionFailureException ex )
        {
            state = State.AUTO_COMMIT;
            throw ex;
        }
    }

    @Override
    public void rollbackTransaction() throws KernelException
    {
        state = state.rollbackTransaction( ctx, spi );
    }

    @Override
    public boolean hasOpenStatement()
    {
        return !ctx.statementOutcomes.isEmpty();
    }

    /**
     * Rollback and close transaction. Move back to {@link State#AUTO_COMMIT}.
     * <p>
     * <b>Warning:</b>This method should only be called by the bolt worker thread during it's regular message
     * processing. It is wrong to call it from a different thread because kernel transactions are not thread-safe.
     *
     * @throws TransactionFailureException when transaction fails to close.
     */
    @Override
    public void reset() throws TransactionFailureException
    {
        state.terminateQueryAndRollbackTransaction( spi, ctx );
        state = State.AUTO_COMMIT;
    }

    @Override
    public void markCurrentTransactionForTermination()
    {
        BoltTransaction tx = ctx.currentTransaction;
        if ( tx != null )
        {
            tx.markForTermination( Status.Transaction.Terminated );
        }
    }

    @Override
    public Status validateTransaction() throws KernelException
    {
        BoltTransaction tx = ctx.currentTransaction;

        if ( tx != null )
        {
            Optional<Status> statusOpt = tx.getReasonIfTerminated();

            if ( statusOpt.isPresent() )
            {
                if ( statusOpt.get().code().classification().rollbackTransaction() )
                {
                    Status pendingTerminationNotice = statusOpt.get();
                    reset();
                    return pendingTerminationNotice;
                }
            }
        }
        return null;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public boolean hasTransaction()
    {
        return state == State.EXPLICIT_TRANSACTION;
    }

    enum State
    {
        AUTO_COMMIT
                {
                    @Override
                    State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, List<Bookmark> bookmarks, Duration txTimeout,
                            AccessMode accessMode, Map<String,Object> txMetadata ) throws KernelException
                    {
                        beginTransaction( ctx, spi, bookmarks, txTimeout, accessMode, txMetadata, false );
                        return EXPLICIT_TRANSACTION;
                    }

                    @Override
                    State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, List<Bookmark> bookmarks,
                            Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        execute( ctx, spi, statement, params, spi.isPeriodicCommit( statement ), bookmarks, txTimeout, accessMode, txMetadata );
                        return AUTO_COMMIT;
                    }

                    void execute( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, boolean isPeriodicCommit,
                            List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        beginTransaction( ctx, spi, bookmarks, txTimeout, accessMode, txMetadata, isPeriodicCommit );

                        boolean failed = true;
                        try
                        {
                            int statementId = StatementMetadata.ABSENT_QUERY_ID;

                            BoltQueryExecutor boltQueryExecutor = ctx.currentTransaction;

                            BoltResultHandle resultHandle = spi.executeQuery( boltQueryExecutor, statement, params);
                            BoltResult result = startExecution( resultHandle );
                            ctx.statementOutcomes.put( statementId, new StatementOutcome( resultHandle, result ) );

                            String[] fieldNames = result.fieldNames();
                            ctx.lastStatementMetadata = new AutoCommitStatementMetadata( fieldNames );

                            failed = false;
                        }
                        finally
                        {
                            if ( failed )
                            {
                                closeTransaction( ctx, spi, false );
                            }
                        }
                    }

                    private void beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, List<Bookmark> bookmarks, Duration txTimeout,
                            AccessMode accessMode, Map<String,Object> txMetadata, boolean isPeriodicCommit )
                    {
                        try
                        {
                            ctx.currentTransaction = isPeriodicCommit ?
                                                     spi.beginPeriodicCommitTransaction( ctx.loginContext, bookmarks, txTimeout, accessMode, txMetadata ) :
                                                     spi.beginTransaction( ctx.loginContext, bookmarks, txTimeout, accessMode, txMetadata );
                        }
                        catch ( Throwable e )
                        {
                            // If we failed to begin a transaction for some reason such as the database is stopped, we need to release ourselves
                            spi.transactionClosed();
                            throw e;
                        }
                    }

                    @Override
                    Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, int statementId, ResultConsumer resultConsumer )
                            throws Throwable
                    {
                        StatementOutcome outcome = ctx.statementOutcomes.get( statementId );
                        if ( outcome == null )
                        {
                            throw new IllegalArgumentException( "Unknown statement ID: " + statementId + ". Existing IDs: " + ctx.statementOutcomes.keySet() );
                        }

                        boolean success = false;
                        try
                        {
                            consumeResult( ctx, statementId, outcome, resultConsumer );
                            if ( !resultConsumer.hasMore() )
                            {
                                var tx = ctx.currentTransaction;
                                closeTransaction( ctx, spi, true );
                                success = true;
                                return newestBookmark( spi, tx );
                            }
                            success = true;
                        }
                        finally
                        {
                            // throw error
                            if ( !success )
                            {
                                closeTransaction( ctx, spi, false );
                            }
                        }
                        return EMPTY_BOOKMARK;
                    }

                    @Override
                    State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        throw new QueryExecutionKernelException( new InvalidSemanticsException( "No current transaction to commit.", null ) );
                    }

                    @Override
                    State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi )
                    {
                        // add dummy outcome useful for < Bolt V3, i.e. `RUN "ROLLBACK" & PULL_ALL`
                        int statementId = StatementMetadata.ABSENT_QUERY_ID;
                        ctx.statementOutcomes.put( statementId, new StatementOutcome( BoltResult.EMPTY ) );

                        return AUTO_COMMIT;
                    }
                },
        EXPLICIT_TRANSACTION
                {
                    @Override
                    State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, List<Bookmark> bookmarks, Duration txTimeout,
                            AccessMode accessMode, Map<String,Object> txMetadata ) throws KernelException
                    {
                        throw new QueryExecutionKernelException( new InvalidSemanticsException( "Nested transactions are not supported.", null ) );
                    }

                    @Override
                    State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, List<Bookmark> bookmarks,
                            Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        checkState( txTimeout == null, "Explicit Transaction should not run with tx_timeout" );
                        checkState( isEmpty( txMetadata ), "Explicit Transaction should not run with tx_metadata" );

                        if ( spi.isPeriodicCommit( statement ) )
                        {
                            throw new QueryExecutionKernelException( new InvalidSemanticsException(
                                    "Executing queries that use periodic commit in an " +
                                    "open transaction is not possible.", null ) );
                        }
                        else
                        {
                            // generate real statement ID only when nested statements in transaction are supported
                            int statementId = spi.supportsNestedStatementsInTransaction() ? ctx.nextStatementId() : StatementMetadata.ABSENT_QUERY_ID;

                            BoltResultHandle resultHandle = spi.executeQuery( ctx.currentTransaction, statement, params);
                            BoltResult result = startExecution( resultHandle );
                            ctx.statementOutcomes.put( statementId, new StatementOutcome( resultHandle, result ) );

                            String[] fieldNames = result.fieldNames();
                            ctx.lastStatementId = statementId;
                            ctx.lastStatementMetadata = new ExplicitTxStatementMetadata( fieldNames, statementId );

                            return EXPLICIT_TRANSACTION;
                        }
                    }

                    @Override
                    Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, int statementId, ResultConsumer resultConsumer )
                            throws Throwable
                    {
                        if ( statementId == StatementMetadata.ABSENT_QUERY_ID )
                        {
                            statementId = ctx.lastStatementId;
                        }
                        StatementOutcome outcome = ctx.statementOutcomes.get( statementId );
                        if ( outcome == null )
                        {
                            throw new IllegalArgumentException( "Unknown statement ID: " + statementId + ". Existing IDs: " + ctx.statementOutcomes.keySet() );
                        }

                        consumeResult( ctx, statementId, outcome, resultConsumer );
                        return EMPTY_BOOKMARK; // Explicit tx shall not get a bookmark in PULL_ALL or DISCARD_ALL
                    }

                    @Override
                    State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        closeTransaction( ctx, spi, true );
                        return AUTO_COMMIT;
                    }

                    @Override
                    State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        closeTransaction( ctx, spi, false );
                        return AUTO_COMMIT;
                    }
                };

        abstract State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, List<Bookmark> bookmarks, Duration txTimeout,
                AccessMode accessMode, Map<String,Object> txMetadata ) throws KernelException;

        abstract State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, List<Bookmark> bookmarks,
                Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
                throws KernelException;

        abstract Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, int statementId, ResultConsumer resultConsumer )
                throws Throwable;

        abstract State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException;

        abstract State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException;

        void terminateQueryAndRollbackTransaction( TransactionStateMachineSPI spi, MutableTransactionState ctx ) throws TransactionFailureException
        {
            terminateActiveStatements( ctx );
            closeTransaction( ctx, spi, false );
        }

        /*
         * This is overly careful about always closing and nulling the transaction since
         * reset can cause ctx.currentTransaction to be null we store in local variable.
         */
        void closeTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, boolean success ) throws TransactionFailureException
        {
            closeActiveStatements( ctx, success );

            BoltTransaction tx = ctx.currentTransaction;
            ctx.currentTransaction = null;
            if ( tx != null )
            {
                try
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
                finally
                {
                    ctx.currentTransaction = null;
                    ctx.statementCounter = 0;
                    spi.transactionClosed(); // we need to release ourselves, after this point we are in the hand of GC
                }
            }
        }

        private void terminateActiveStatements( MutableTransactionState ctx )
        {
            RuntimeException error = null;

            for ( StatementOutcome outcome : ctx.statementOutcomes.values() )
            {
                try
                {
                    BoltResultHandle resultHandle = outcome.resultHandle;
                    if ( resultHandle != null )
                    {
                        resultHandle.terminate();
                    }

                    BoltResult result = outcome.result;
                    if ( result != null )
                    {
                        result.close();
                    }
                }
                catch ( Throwable e )
                {
                    if ( error == null )
                    {
                        error = new RuntimeException( "Failed to terminate active statements.", e );
                    }
                    else
                    {
                        error.addSuppressed( e );
                    }
                }
            }
            ctx.statementOutcomes.clear();

            if ( error != null )
            {
                throw error;
            }
        }

        private void closeActiveStatements( MutableTransactionState ctx, boolean success )
        {
            RuntimeException error = null;

            for ( StatementOutcome outcome : ctx.statementOutcomes.values() )
            {
                try
                {
                    BoltResultHandle resultHandle = outcome.resultHandle;
                    if ( resultHandle != null )
                    {
                        resultHandle.close( success );
                    }

                    BoltResult result = outcome.result;
                    if ( result != null )
                    {
                        result.close();
                    }
                }
                catch ( Throwable e )
                {
                    if ( error == null )
                    {
                        error = new RuntimeException( "Failed to close active statements.", e );
                    }
                    else
                    {
                        error.addSuppressed( e );
                    }
                }
            }
            ctx.statementOutcomes.clear();

            if ( error != null )
            {
                throw error;
            }
        }

        void consumeResult( MutableTransactionState ctx, int statementId, StatementOutcome outcome, ResultConsumer resultConsumer ) throws Throwable
        {
            boolean success = false;
            try
            {
                resultConsumer.consume( outcome.result );
                success = true;
            }
            finally
            {
                // if throws errors or if we finished consuming result
                if ( !success || !resultConsumer.hasMore() )
                {
                    outcome.result.close();
                    BoltResultHandle resultHandle = outcome.resultHandle;
                    if ( resultHandle != null )
                    {
                        resultHandle.close( success );
                    }
                    ctx.statementOutcomes.remove( statementId );
                }
            }
        }

        BoltResult startExecution( BoltResultHandle resultHandle ) throws KernelException
        {
            try
            {
                return resultHandle.start();
            }
            catch ( Throwable t )
            {
                resultHandle.close( false );
                throw t;
            }
        }

    }

    private static Bookmark newestBookmark( TransactionStateMachineSPI spi, BoltTransaction tx )
    {
        return spi.newestBookmark( tx );
    }

    static class MutableTransactionState
    {
        int statementCounter;

        /** The current session security context to be used for starting transactions */
        final LoginContext loginContext;

        /** The current transaction, if present */
        BoltTransaction currentTransaction;

        final Map<Integer,StatementOutcome> statementOutcomes = new HashMap<>();

        final Clock clock;

        /**
         * Used to handle RUN + PULL combo that arrives at the same time. PULL will not contain qid in this case
         */
        int lastStatementId = StatementMetadata.ABSENT_QUERY_ID;

        StatementMetadata lastStatementMetadata;

        MutableTransactionState( AuthenticationResult authenticationResult, Clock clock )
        {
            this.clock = clock;
            this.loginContext = authenticationResult.getLoginContext();
        }

        int nextStatementId()
        {
            return statementCounter++;
        }
    }

    static class StatementOutcome
    {
        BoltResultHandle resultHandle;
        BoltResult result;

        StatementOutcome( BoltResult result )
        {
            this.result = result;
        }

        StatementOutcome( BoltResultHandle resultHandle, BoltResult result )
        {
            this.resultHandle = resultHandle;
            this.result = result;
        }
    }

    @Override
    public String toString()
    {
        return "TransactionStateMachine{" + "state=" + state + ", databaseName='" + databaseName + '\'' + '}';
    }
}
