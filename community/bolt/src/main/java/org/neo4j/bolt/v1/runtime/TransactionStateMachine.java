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
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v1.runtime.spi.BookmarkResult;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.util.Preconditions.checkState;

public class TransactionStateMachine implements StatementProcessor
{
    final TransactionStateMachineSPI spi;
    final MutableTransactionState ctx;
    State state = State.AUTO_COMMIT;

    TransactionStateMachine( TransactionStateMachineSPI spi, AuthenticationResult authenticationResult, Clock clock )
    {
        this.spi = spi;
        ctx = new MutableTransactionState( authenticationResult, clock );
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
            ensureNoPendingTerminationNotice();

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
            ensureNoPendingTerminationNotice();

            state = state.run( ctx, spi, statement, params, bookmark, txTimeout, txMetaData );

            return ctx.currentStatementMetadata;
        }
        finally
        {
            after();
        }
    }

    @Override
    public Bookmark streamResult( ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception
    {
        before();
        try
        {
            ensureNoPendingTerminationNotice();

            return state.streamResult( ctx, spi, resultConsumer );
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
            ensureNoPendingTerminationNotice();

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
            ensureNoPendingTerminationNotice();
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
        return ctx.currentResultHandle != null;
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
        state.terminateQueryAndRollbackTransaction( ctx );
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
    public void validateTransaction() throws KernelException
    {
        KernelTransaction tx = ctx.currentTransaction;

        if ( tx != null )
        {
            Optional<Status> statusOpt = tx.getReasonIfTerminated();

            if ( statusOpt.isPresent() )
            {
                if ( statusOpt.get().code().classification().rollbackTransaction() )
                {
                    ctx.pendingTerminationNotice = statusOpt.get();

                    reset();
                }
            }
        }
    }

    private void ensureNoPendingTerminationNotice()
    {
        if ( ctx.pendingTerminationNotice != null )
        {
            Status status = ctx.pendingTerminationNotice;

            ctx.pendingTerminationNotice = null;

            throw new TransactionTerminatedException( status );
        }
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
                        ctx.currentResult = BoltResult.EMPTY;
                        ctx.currentTransaction = spi.beginTransaction( ctx.loginContext, txTimeout, txMetadata );
                        return EXPLICIT_TRANSACTION;
                    }

                    @Override
                    State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                            Duration txTimeout, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        statement = parseStatement( ctx, statement );
                        waitForBookmark( spi, bookmark );
                        execute( ctx, spi, statement, params, spi.isPeriodicCommit( statement ), txTimeout, txMetadata );
                        return AUTO_COMMIT;
                    }

                    private String parseStatement( MutableTransactionState ctx, String statement )
                    {
                        if ( statement.isEmpty() )
                        {
                            statement = ctx.lastStatement;
                        }
                        else
                        {
                            ctx.lastStatement = statement;
                        }
                        return statement;
                    }

                    void execute( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, boolean isPeriodicCommit,
                            Duration txTimeout, Map<String,Object> txMetadata )
                            throws KernelException
                    {
                        // only acquire a new transaction when the statement does not contain periodic commit
                        if ( !isPeriodicCommit )
                        {
                            ctx.currentTransaction = spi.beginTransaction( ctx.loginContext, txTimeout, txMetadata );
                        }

                        boolean failed = true;
                        try
                        {
                            BoltResultHandle resultHandle = spi.executeQuery( ctx.loginContext, statement, params, txTimeout, txMetadata );
                            startExecution( ctx, resultHandle );
                            failed = false;
                        }
                        finally
                        {
                            // if we acquired a transaction and a failure occurred, then simply close the transaction
                            if ( !isPeriodicCommit )
                            {
                                if ( failed )
                                {
                                    closeTransaction( ctx, false );
                                }
                            }
                            else
                            {
                                ctx.currentTransaction = spi.beginTransaction( ctx.loginContext, txTimeout, txMetadata );
                            }
                        }
                    }

                    @Override
                    Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult,Exception> resultConsumer )
                            throws Exception
                    {
                        assert ctx.currentResult != null;

                        try
                        {
                            consumeResult( ctx, resultConsumer );
                            closeTransaction( ctx, true );
                            return newestBookmark( spi );
                        }
                        finally
                        {
                            closeTransaction( ctx, false );
                        }
                    }

                    @Override
                    State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        throw new QueryExecutionKernelException( new InvalidSemanticsException( "No current transaction to commit." ) );
                    }

                    @Override
                    State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi )
                    {
                        ctx.currentResult = BoltResult.EMPTY;
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

                        if ( statement.isEmpty() )
                        {
                            statement = ctx.lastStatement;
                        }
                        else
                        {
                            ctx.lastStatement = statement;
                        }
                        if ( spi.isPeriodicCommit( statement ) )
                        {
                            throw new QueryExecutionKernelException( new InvalidSemanticsException(
                                    "Executing queries that use periodic commit in an " +
                                    "open transaction is not possible." ) );
                        }
                        else
                        {
                            BoltResultHandle resultHandle = spi.executeQuery( ctx.loginContext, statement, params, null, null /*ignored in explict tx run*/ );
                            startExecution( ctx, resultHandle );
                            return EXPLICIT_TRANSACTION;
                        }
                    }

                    @Override
                    Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult,Exception> resultConsumer )
                            throws Exception
                    {
                        assert ctx.currentResult != null;
                        consumeResult( ctx, resultConsumer );
                        return null; // Explict tx shall not get a bookmark in PULL_ALL or DISCARD_ALL
                    }

                    @Override
                    State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        closeTransaction( ctx, true );
                        Bookmark bookmark = newestBookmark( spi );
                        ctx.currentResult = new BookmarkResult( bookmark );
                        return AUTO_COMMIT;
                    }

                    @Override
                    State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException
                    {
                        closeTransaction( ctx, false );
                        ctx.currentResult = BoltResult.EMPTY;
                        return AUTO_COMMIT;
                    }
                };

        abstract State beginTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi, Bookmark bookmark, Duration txTimeout,
                Map<String,Object> txMetadata ) throws KernelException;

        abstract State run( MutableTransactionState ctx, TransactionStateMachineSPI spi, String statement, MapValue params, Bookmark bookmark,
                Duration txTimeout, Map<String,Object> txMetadata )
                throws KernelException;

        abstract Bookmark streamResult( MutableTransactionState ctx, TransactionStateMachineSPI spi, ThrowingConsumer<BoltResult,Exception> resultConsumer )
                throws Exception;

        abstract State commitTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException;

        abstract State rollbackTransaction( MutableTransactionState ctx, TransactionStateMachineSPI spi ) throws KernelException;

        void terminateQueryAndRollbackTransaction( MutableTransactionState ctx ) throws TransactionFailureException
        {
            if ( ctx.currentResultHandle != null )
            {
                ctx.currentResultHandle.terminate();
                ctx.currentResultHandle = null;
            }
            if ( ctx.currentResult != null )
            {
                ctx.currentResult.close();
                ctx.currentResult = null;
            }

           closeTransaction( ctx, false);
        }

        /*
         * This is overly careful about always closing and nulling the transaction since
         * reset can cause ctx.currentTransaction to be null we store in local variable.
         */
        void closeTransaction( MutableTransactionState ctx, boolean success ) throws TransactionFailureException
        {
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
                }
            }
        }

        boolean consumeResult( MutableTransactionState ctx, ThrowingConsumer<BoltResult,Exception> resultConsumer ) throws Exception
        {
            boolean success = false;
            try
            {
                resultConsumer.accept( ctx.currentResult );
                success = true;
            }
            finally
            {
                ctx.currentResult.close();
                ctx.currentResult = null;

                if ( ctx.currentResultHandle != null )
                {
                    ctx.currentResultHandle.close( success );
                    ctx.currentResultHandle = null;
                }
            }
            return success;
        }

        void startExecution( MutableTransactionState ctx, BoltResultHandle resultHandle ) throws KernelException
        {
            ctx.currentResultHandle = resultHandle;
            try
            {
                ctx.currentResult = resultHandle.start();
            }
            catch ( Throwable t )
            {
                ctx.currentResultHandle.close( false );
                ctx.currentResultHandle = null;
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
        /** The current session security context to be used for starting transactions */
        final LoginContext loginContext;

        /** The current transaction, if present */
        KernelTransaction currentTransaction;

        Status pendingTerminationNotice;

        /** Last Cypher statement executed */
        String lastStatement = "";

        /** The current pending result, if present */
        BoltResult currentResult;

        BoltResultHandle currentResultHandle;

        final Clock clock;

        /** A re-usable statement metadata instance that always represents the currently running statement */
        private final StatementMetadata currentStatementMetadata = new StatementMetadata()
        {
            @Override
            public String[] fieldNames()
            {
                return currentResult.fieldNames();
            }
        };

        private MutableTransactionState( AuthenticationResult authenticationResult, Clock clock )
        {
            this.clock = clock;
            this.loginContext = authenticationResult.getLoginContext();
        }
    }
}
