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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.util.Map;

import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v1.runtime.cypher.CypherAdapterStream;
import org.neo4j.bolt.v1.runtime.cypher.StatementMetadata;
import org.neo4j.bolt.v1.runtime.cypher.StatementProcessor;
import org.neo4j.bolt.v1.runtime.spi.BookmarkResult;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;

import java.time.Duration;

public class TransactionStateMachine implements StatementProcessor
{
    private static final String BEGIN = "BEGIN";
    private static final String COMMIT = "COMMIT";
    private static final String ROLLBACK = "ROLLBACK";

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
    public StatementMetadata run( String statement, Map<String, Object> params ) throws KernelException
    {
        before();
        try
        {
            state = state.run( ctx, spi, statement, params );

            return ctx.currentStatementMetadata;
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

    @Override
    public void reset() throws TransactionFailureException
    {
        state.rollbackTransaction( ctx );
        state.closeResult( ctx );
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
    public void setQuerySource( String querySource )
    {
        this.ctx.querySource = querySource;
    }

    enum State
    {
        AUTO_COMMIT
                {
                    @Override
                    State run( MutableTransactionState ctx, SPI spi, String statement,
                               Map<String, Object> params ) throws KernelException

                    {
                        if ( statement.equalsIgnoreCase( BEGIN ) )
                        {
                            ctx.currentTransaction = spi.beginTransaction( ctx.authSubject );

                            if ( params.containsKey( "bookmark" ) )
                            {
                                final Bookmark bookmark = Bookmark.fromString( params.get( "bookmark" ).toString() );
                                spi.awaitUpToDate( bookmark.txId(), Duration.ofSeconds( 30 ) );
                                ctx.currentResult = new BookmarkResult( bookmark );
                            }
                            else
                            {
                                ctx.currentResult = BoltResult.EMPTY;
                            }

                            return EXPLICIT_TRANSACTION;
                        }
                        else if ( statement.equalsIgnoreCase( COMMIT ) )
                        {
                            throw new QueryExecutionKernelException(
                                    new InvalidSemanticsException( "No current transaction to commit." ) );
                        }
                        else if ( statement.equalsIgnoreCase( ROLLBACK ) )
                        {
                            throw new QueryExecutionKernelException(
                                    new InvalidSemanticsException( "No current transaction to rollback." ) );
                        }
                        else if( spi.isPeriodicCommit( statement ) )
                        {
                            Result result = executeQuery( ctx, spi, statement, params );

                            ctx.currentTransaction = spi.beginTransaction( ctx.authSubject );

                            ctx.currentResult = new CypherAdapterStream( result, ctx.clock );
                            return AUTO_COMMIT;
                        }
                        else
                        {
                            ctx.currentTransaction = spi.beginTransaction( ctx.authSubject );

                            Result result = executeQuery( ctx, spi, statement, params );

                            ctx.currentResult = new CypherAdapterStream( result, ctx.clock );
                            return AUTO_COMMIT;
                        }
                    }

                    @Override
                    void streamResult( MutableTransactionState ctx,
                                       ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception
                    {
                        assert ctx.currentResult != null;
                        resultConsumer.accept( ctx.currentResult );
                        ctx.currentResult.close();
                        if ( ctx.currentTransaction != null )
                        {
                            ctx.currentTransaction.success();
                            ctx.currentTransaction.close();
                            ctx.currentTransaction = null;
                        }
                    }
                },
        EXPLICIT_TRANSACTION
                {
                    @Override
                    State run( MutableTransactionState ctx, SPI spi, String statement, Map<String, Object> params )
                            throws KernelException
                    {
                        if ( statement.equalsIgnoreCase( BEGIN ) )
                        {
                            return EXPLICIT_TRANSACTION;
                        }
                        else if ( statement.equalsIgnoreCase( COMMIT ) )
                        {
                            ctx.currentTransaction.success();
                            ctx.currentTransaction.close();
                            ctx.currentTransaction = null;

                            long txId = spi.newestEncounteredTxId();
                            Bookmark bookmark = new Bookmark( txId );
                            ctx.currentResult = new BookmarkResult( bookmark );

                            return AUTO_COMMIT;
                        }
                        else if ( statement.equalsIgnoreCase( ROLLBACK ) )
                        {
                            ctx.currentTransaction.failure();
                            ctx.currentTransaction.close();
                            ctx.currentTransaction = null;
                            ctx.currentResult = BoltResult.EMPTY;
                            return AUTO_COMMIT;
                        }
                        else if( spi.isPeriodicCommit( statement ) )
                        {
                            throw new QueryExecutionKernelException( new InvalidSemanticsException(
                                    "Executing queries that use periodic commit in an " +
                                            "open transaction is not possible." ) );
                        }
                        else
                        {
                            Result result = executeQuery( ctx, spi, statement, params );

                            ctx.currentResult = new CypherAdapterStream( result, ctx.clock );
                            return EXPLICIT_TRANSACTION;
                        }
                    }

                    @Override
                    void streamResult( MutableTransactionState ctx, ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception
                    {
                        assert ctx.currentResult != null;
                        resultConsumer.accept( ctx.currentResult );
                        ctx.currentResult.close();
                    }
                };

        abstract State run( MutableTransactionState ctx,
                            SPI spi,
                            String statement,
                            Map<String, Object> params ) throws KernelException;

        abstract void streamResult( MutableTransactionState ctx,
                                    ThrowingConsumer<BoltResult, Exception> resultConsumer ) throws Exception;

        void rollbackTransaction( MutableTransactionState ctx ) throws TransactionFailureException
        {
            if ( ctx.currentTransaction != null )
            {
                if ( ctx.currentTransaction.isOpen() )
                {
                    ctx.currentTransaction.failure();
                    ctx.currentTransaction.close();
                    ctx.currentTransaction = null;
                }
            }
        }

        void closeResult( MutableTransactionState ctx )
        {
            if ( ctx.currentResult != null )
            {
                ctx.currentResult.close();
                ctx.currentResult = null;
            }
        }

    }

    private static Result executeQuery( MutableTransactionState ctx, SPI spi, String statement,
                                        Map<String, Object> params ) throws QueryExecutionKernelException
    {
        return spi.executeQuery( ctx.querySource, ctx.authSubject, statement, params );
    }

    static class MutableTransactionState
    {
        /** The current session auth state to be used for starting transactions */
        final AuthSubject authSubject;

        /** The current transaction, if present */
        KernelTransaction currentTransaction;

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

        String querySource;

        private MutableTransactionState( AuthenticationResult authenticationResult, Clock clock )
        {
            this.clock = clock;
            this.authSubject = authenticationResult.getAuthSubject();
        }
    }

    interface SPI
    {
        void awaitUpToDate( long oldestAcceptableTxId, Duration timeout ) throws TransactionFailureException;

        long newestEncounteredTxId();

        KernelTransaction beginTransaction( AuthSubject authSubject );

        void bindTransactionToCurrentThread( KernelTransaction tx );

        void unbindTransactionFromCurrentThread();

        boolean isPeriodicCommit( String query );

        Result executeQuery( String querySource, AuthSubject authSubject, String statement, Map<String, Object> params )
                throws QueryExecutionKernelException;
    }
}
