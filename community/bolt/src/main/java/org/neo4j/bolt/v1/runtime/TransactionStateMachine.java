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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.util.regex.Pattern;

import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.bolt.v1.runtime.spi.BookmarkResult;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.function.ThrowingAction.noop;

public class TransactionStateMachine implements StatementProcessor
{
    private static final Pattern BEGIN = Pattern.compile("(?i)^\\s*BEGIN\\s*;?\\s*$");
    private static final Pattern COMMIT = Pattern.compile("(?i)^\\s*COMMIT\\s*;?\\s*$");
    private static final Pattern ROLLBACK = Pattern.compile("(?i)^\\s*ROLLBACK\\s*;?\\s*$");

    final SPI spi;
    final MutableTransactionState ctx;
    State state = State.AUTO_COMMIT;

    TransactionStateMachine( SPI spi, AuthenticationResult authenticationResult, Clock clock )
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
    public StatementMetadata run( String statement, MapValue params ) throws KernelException
    {
        before();
        try
        {
            state = state.run( ctx, spi, statement, params );

            return ctx.currentStatementMetadata;
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
    public void streamResult( ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception
    {
        before();
        try
        {
            state.streamResult( ctx, resultConsumer );
        }
        finally
        {
            after();
        }
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
        if ( ctx.currentTransaction != null )
        {
            spi.unbindTransactionFromCurrentThread();
        }
    }

    public void markCurrentTransactionForTermination()
    {
        KernelTransaction tx = ctx.currentTransaction;
        if ( tx != null )
        {
            tx.markForTermination( Status.Transaction.Terminated );
        }
    }

    @Override
    public boolean hasTransaction()
    {
        return state == State.EXPLICIT_TRANSACTION;
    }

    @Override
    public void setQuerySource( BoltQuerySource querySource )
    {
        this.ctx.querySource = querySource;
    }

    enum State
    {
        AUTO_COMMIT
                {
                    @Override
                    State run( MutableTransactionState ctx, SPI spi, String statement,
                               MapValue params ) throws KernelException

                    {
                        if ( BEGIN.matcher( statement ).matches() )
                        {
                            ctx.currentTransaction = spi.beginTransaction( ctx.securityContext );

                            Bookmark bookmark = Bookmark.fromParamsOrNull( params );
                            if ( bookmark != null )
                            {
                                spi.awaitUpToDate( bookmark.txId() );
                                ctx.currentResult = new BookmarkResult( bookmark );
                            }
                            else
                            {
                                ctx.currentResult = BoltResult.EMPTY;
                            }

                            return EXPLICIT_TRANSACTION;
                        }
                        else if ( COMMIT.matcher( statement ).matches() )
                        {
                            throw new QueryExecutionKernelException(
                                    new InvalidSemanticsException( "No current transaction to commit." ) );
                        }
                        else if ( ROLLBACK.matcher( statement ).matches() )
                        {
                            throw new QueryExecutionKernelException(
                                    new InvalidSemanticsException( "No current transaction to rollback." ) );
                        }
                        else
                        {
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
                                BoltResultHandle resultHandle = executeQuery( ctx, spi, statement, params, noop() );
                                ctx.currentResultHandle = resultHandle;
                                ctx.currentResult = resultHandle.start();
                                ctx.currentTransaction = null; // Periodic commit will change the current transaction, so
                                // we can't trust this to point to the actual current transaction;
                                return AUTO_COMMIT;
                            }
                            else
                            {
                                ctx.currentTransaction = spi.beginTransaction( ctx.securityContext );
                                BoltResultHandle resultHandle = execute( ctx, spi, statement, params );
                                ctx.currentResultHandle = resultHandle;
                                ctx.currentResult = resultHandle.start();
                                return AUTO_COMMIT;
                            }
                        }
                    }

                    /*
                     * In AUTO_COMMIT we must make sure to fail, close and set the current
                     * transaction to null.
                     */
                    private BoltResultHandle execute( MutableTransactionState ctx, SPI spi,
                            String statement, MapValue params )
                            throws TransactionFailureException, QueryExecutionKernelException
                    {
                        return executeQuery( ctx, spi, statement, params, () ->
                        {
                           closeTransaction( ctx, false );
                        } );
                    }

                    @Override
                    void streamResult( MutableTransactionState ctx,
                                       ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception
                    {
                        assert ctx.currentResult != null;
                        resultConsumer.accept( ctx.currentResult );
                        ctx.currentResult.close();
                        closeTransaction( ctx, true );
                    }
                },
        EXPLICIT_TRANSACTION
                {
                    @Override
                    State run( MutableTransactionState ctx, SPI spi, String statement, MapValue params )
                            throws KernelException
                    {
                        if ( BEGIN.matcher( statement ).matches() )
                        {
                            throw new QueryExecutionKernelException(
                                    new InvalidSemanticsException( "Nested transactions are not supported." ) );
                        }
                        else if ( COMMIT.matcher( statement ).matches() )
                        {
                            closeTransaction( ctx, true );
                            long txId = spi.newestEncounteredTxId();
                            Bookmark bookmark = new Bookmark( txId );
                            ctx.currentResult = new BookmarkResult( bookmark );

                            return AUTO_COMMIT;
                        }
                        else if ( ROLLBACK.matcher( statement ).matches() )
                        {
                            closeTransaction( ctx, false );
                            ctx.currentResult = BoltResult.EMPTY;
                            return AUTO_COMMIT;
                        }
                        else
                        {
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
                                ctx.currentResultHandle = execute( ctx, spi, statement, params );
                                ctx.currentResult = ctx.currentResultHandle.start();
                                return EXPLICIT_TRANSACTION;
                            }
                        }
                    }

                    private BoltResultHandle execute( MutableTransactionState ctx, SPI spi,
                            String statement, MapValue params )
                            throws QueryExecutionKernelException
                    {
                        return executeQuery( ctx, spi, statement, params,
                                () ->
                                {
                                    if ( ctx.currentTransaction != null )
                                    {
                                        ctx.currentTransaction.failure();
                                    }
                                } );
                    }

                    @Override
                    void streamResult( MutableTransactionState ctx,
                            ThrowingConsumer<BoltResult,Exception> resultConsumer ) throws Exception
                    {
                        assert ctx.currentResult != null;
                        resultConsumer.accept( ctx.currentResult );
                        ctx.currentResult.close();
                    }
                };

        abstract State run( MutableTransactionState ctx,
                            SPI spi,
                            String statement,
                            MapValue params ) throws KernelException;

        abstract void streamResult( MutableTransactionState ctx,
                                    ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception;

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
    }

    private static BoltResultHandle executeQuery( MutableTransactionState ctx, SPI spi, String statement,
                                                  MapValue params, ThrowingAction<KernelException> onFail )
            throws QueryExecutionKernelException
    {
        return spi.executeQuery( ctx.querySource, ctx.securityContext, statement, params, onFail );
    }

    /**
     * This interface makes it possible to abort queries even before they have returned a Result object.
     * In some cases, creating the Result object will take as long as running the query takes. This way, we can
     * terminate the underlying transaction while the Result object is created.
     */
    interface BoltResultHandle
    {
        BoltResult start() throws KernelException;
        void terminate();
    }

    static class MutableTransactionState
    {
        /** The current session security context to be used for starting transactions */
        final SecurityContext securityContext;

        /** The current transaction, if present */
        KernelTransaction currentTransaction;

        /** Last Cypher statement executed */
        String lastStatement = "";

        /** The current pending result, if present */
        BoltResult currentResult;

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

        BoltQuerySource querySource;
        BoltResultHandle currentResultHandle;

        private MutableTransactionState( AuthenticationResult authenticationResult, Clock clock )
        {
            this.clock = clock;
            this.securityContext = authenticationResult.getSecurityContext();
        }
    }

    interface SPI
    {
        void awaitUpToDate( long oldestAcceptableTxId ) throws TransactionFailureException;

        long newestEncounteredTxId();

        KernelTransaction beginTransaction( SecurityContext securityContext );

        void bindTransactionToCurrentThread( KernelTransaction tx );

        void unbindTransactionFromCurrentThread();

        boolean isPeriodicCommit( String query );

        BoltResultHandle executeQuery( BoltQuerySource querySource,
                SecurityContext securityContext,
                String statement,
                MapValue params,
                ThrowingAction<KernelException> onFail ) throws QueryExecutionKernelException;
    }
}
