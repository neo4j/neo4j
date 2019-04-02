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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.runtime.AutoCommitStatementMetadata;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.ExplicitTxStatementMetadata;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v4.messaging.ResultConsumer;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v1.runtime.bookmarking.Bookmark.EMPTY_BOOKMARK;
import static org.neo4j.util.Preconditions.checkState;

public class TransactionStateMachine implements StatementProcessor
{
    private final TransactionStateMachineSPI spi;
    final MutableTransactionState ctx;
    State state = State.AUTO_COMMIT;
    private final DatabaseId databaseId;

    public TransactionStateMachine( DatabaseId databaseId, TransactionStateMachineSPI spi, AuthenticationResult authenticationResult, Clock clock )
    {
        this.spi = spi;
        ctx = new MutableTransactionState( authenticationResult, clock );
        this.databaseId = databaseId;
    }

    public State state()
    {
        return state;
    }

    private void before()
    {
        if ( ctx.currentTransaction != null )
        {
            spi.bindTransactionToCurrentThread( ctx.currentTransaction );
        }
    }

    @Override
    public void beginTransaction( Bookmark bookmark ) throws KernelException
    {
        beginTransaction( bookmark, null, null );
    }

    @Override
    public void beginTransaction( Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetadata ) throws KernelException
    {
        before();
        try
        {
            state = state.beginTransaction( ctx, spi, bookmark, txTimeout, txMetadata );
        }
        finally
        {
            after();
        }
    }

    @Override
    public StatementMetadata run( String statement, MapValue params ) throws KernelException
    {
        return run( statement, params, null, null, null );
    }

    @Override
    public StatementMetadata run( String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetaData )
            throws KernelException
    {
        before();
        try
        {
            state = state.run( ctx, spi, statement, params, bookmark, txTimeout, txMetaData );

            StatementMetadata metadata = ctx.lastStatementMetadata;
            ctx.lastStatementMetadata = null; // metadata should not be needed more than once
            return metadata;
        }
        finally
        {
            after();
        }
    }

    @Override
    public Bookmark streamResult( int statementId, ResultConsumer resultConsumer ) throws Throwable
    {
        before();
        try
        {
            return state.streamResult( ctx, spi, statementId, resultConsumer );
        }
        finally
        {
            after();
        }
    }

    @Override
    public Bookmark commitTransaction() throws KernelException
    {
        before();
        try
        {
            state = state.commitTransaction( ctx, spi );
            return newestBookmark( spi );
        }
        catch ( TransactionFailureException ex )
        {
            state = State.AUTO_COMMIT;
            throw ex;
        }
        finally
        {
            after();
        }
    }

    @Override
    public void rollbackTransaction() throws KernelException
    {
        before();
        try
        {
            state = state.rollbackTransaction( ctx, spi );
        }
        finally
        {
            after();
        }
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

    private void after()
    {
        spi.unbindTransactionFromCurrentThread();
    }

    @Override
    public void markCurrentTransactionForTermination()
    {
        KernelTransaction tx = ctx.currentTransaction;
        if ( tx != null )
        {
            tx.markForTermination( Status.Transaction.Terminated );
        }
    }

    @Override
    public Status validateTransaction() throws KernelException
    {
        KernelTransaction tx = ctx.currentTransaction;

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
    public DatabaseId databaseId()
    {
        return databaseId;
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
                    State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                            Map<String,Object> txMetadata ) throws KernelException
                    {
                        waitForBookmark( spi, bookmark );

                        beginTransaction( ctx, spi, txTimeout, txMetadata );
                        return EXPLICIT_TRANSACTION;
                    }

                    @Override
                    State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                            Duration txTimeout, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        waitForBookmark( spi, bookmark );
                        execute( ctx, spi, statement, params, spi.isPeriodicCommit( statement ), txTimeout, txMetadata );
                        return AUTO_COMMIT;
                    }

                    void execute( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, boolean isPeriodicCommit,
                            Duration txTimeout, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        // only acquire a new transaction when the statement does not contain periodic commit
                        if ( !isPeriodicCommit )
                        {
                            beginTransaction( ctx, spi, txTimeout, txMetadata );
                        }

                        boolean failed = true;
                        try
                        {
                            int statementId = StatementMetadata.ABSENT_QUERY_ID;

                            BoltResultHandle resultHandle = spi.executeQuery( ctx.loginContext, statement, params, txTimeout, txMetadata );
                            BoltResult result = startExecution( resultHandle );
                            ctx.statementOutcomes.put( statementId, new StatementOutcome( resultHandle, result ) );

                            String[] fieldNames = result.fieldNames();
                            ctx.lastStatementMetadata = new AutoCommitStatementMetadata( fieldNames );

                            failed = false;
                        }
                        finally
                        {
                            // if we acquired a transaction and a failure occurred, then simply close the transaction
                            if ( !isPeriodicCommit )
                            {
                                if ( failed )
                                {
                                    closeTransaction( ctx, spi, false );
                                }
                            }
                            else
                            {
                                beginTransaction( ctx, spi, txTimeout, txMetadata );
                            }
                        }
                    }

                    private void beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, Duration txTimeout,
                            Map<String,Object> txMetadata ) throws TransactionFailureException
                    {
                        try
                        {
                            ctx.currentTransaction = spi.beginTransaction( ctx.loginContext, txTimeout, txMetadata );
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
                                closeTransaction( ctx, spi, true );
                                success = true;
                                return newestBookmark( spi );
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
                        throw new QueryExecutionKernelException( new InvalidSemanticsException( "No current transaction to commit." ) );
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
                    State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                            Map<String,Object> txMetadata ) throws KernelException
                    {
                        throw new QueryExecutionKernelException( new InvalidSemanticsException( "Nested transactions are not supported." ) );
                    }

                    @Override
                    State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                            Duration ignored1, Map<String,Object> ignored2 )
                            throws KernelException
                    {
                        checkState( ignored1 == null, "Explicit Transaction should not run with tx_timeout" );
                        checkState( ignored2 == null, "Explicit Transaction should not run with tx_metadata" );

                        if ( spi.isPeriodicCommit( statement ) )
                        {
                            throw new QueryExecutionKernelException( new InvalidSemanticsException(
                                    "Executing queries that use periodic commit in an " +
                                    "open transaction is not possible." ) );
                        }
                        else
                        {
                            // generate real statement ID only when nested statements in transaction are supported
                            int statementId = spi.supportsNestedStatementsInTransaction() ? ctx.nextStatementId() : StatementMetadata.ABSENT_QUERY_ID;

                            BoltResultHandle resultHandle = spi.executeQuery( ctx.loginContext, statement, params, null, null /*ignored in explict tx run*/ );
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

        abstract State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                Map<String,Object> txMetadata ) throws KernelException;

        abstract State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                Duration txTimeout, Map<String,Object> txMetadata )
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

            KernelTransaction tx = ctx.currentTransaction;
            ctx.currentTransaction = null;
            if ( tx != null )
            {
                try
                {
                    if ( success )
                    {
                        tx.success();
                    }
                    else
                    {
                        tx.failure();
                    }
                    if ( tx.isOpen() )
                    {
                        tx.close();
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

    private static void waitForBookmark( TransactionStateMachineSPI spi, Bookmark bookmark )
            throws TransactionFailureException
    {
        if ( bookmark != null )
        {
            spi.awaitUpToDate( bookmark.txId() );
        }
    }

    private static Bookmark newestBookmark( TransactionStateMachineSPI spi )
    {
        long txId = spi.newestEncounteredTxId();
        return new Bookmark( txId );
    }

    static class MutableTransactionState
    {
        int statementCounter;

        /** The current session security context to be used for starting transactions */
        final LoginContext loginContext;

        /** The current transaction, if present */
        KernelTransaction currentTransaction;

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
        return "TransactionStateMachine{" + "state=" + state + ", databaseId='" + databaseId + '\'' + '}';
    }
}
