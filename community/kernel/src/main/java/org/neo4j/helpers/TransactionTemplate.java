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
package org.neo4j.helpers;

import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientFailureException;

/**
 * Neo4j transaction template that automates the retry-on-exception logic. It uses the builder
 * pattern for configuration, with copy-semantics, so you can iteratively build up instances for
 * different scenarios.
 * <p>
 * First instantiate and configure the template using the fluent API methods, and then
 * invoke execute which will begin/commit transactions in a loop for the specified number of times.
 * <p>
 * By default all exceptions (except Errors and TransactionTerminatedException) cause a retry,
 * and the monitor does nothing, but these can be overridden with custom behavior.
 * A bit more narrow and typical exception to retry on is {@link TransientFailureException},
 * which aims to represent exceptions that are most likely to succeed after a retry.
 */
public class TransactionTemplate
{
    public interface Monitor
    {
        /**
         * Called when an exception occur from the transactions.
         *
         * @param ex the exception thrown.
         */
        void failure( Throwable ex );

        /**
         * Called when the whole retry logic fails. Can be that all retries have failed or that the executing thread was
         * interrupted.
         *
         * @param ex the last exception thrown when it failed.
         */
        void failed( Throwable ex );

        /**
         * Called when a retry is done.
         */
        void retrying();

        class Adapter implements Monitor
        {
            @Override
            public void failure( Throwable ex )
            {
            }

            @Override
            public void failed( Throwable ex )
            {
            }

            @Override
            public void retrying()
            {
            }
        }
    }

    private final GraphDatabaseService gds;
    private final Monitor monitor;
    private final int retries;
    private final long backoff;
    private final Predicate<Throwable> retryPredicate;

    /**
     * Creates a template for performing transactions with retry logic.
     * <p>
     * Default exceptions to retry on is everything except {@link Error} and {@link TransactionTerminatedException}.
     */
    public TransactionTemplate()
    {
        this( (GraphDatabaseService) Proxy.newProxyInstance( GraphDatabaseService.class.getClassLoader(),
                new Class<?>[]{GraphDatabaseService.class}, ( proxy, method, args ) ->
                {
                    throw new IllegalArgumentException( "You need to call 'with(GraphDatabaseService)' on the template in order to use it" );
                } ), new Monitor.Adapter(), 0, 0,
                ex -> !Error.class.isInstance( ex ) && !TransactionTerminatedException.class.isInstance( ex ) );
    }

    /**
     * Create a template for performing transaction with retry logic.
     *
     * @param gds graph database to execute on.
     * @param monitor a monitor that can react to events.
     * @param retries number of retries to try before failing.
     * @param backoff milliseconds to wait between each retry.
     * @param retryPredicate what {@link Throwable}'s to retry on.
     */
    public TransactionTemplate( GraphDatabaseService gds, Monitor monitor, int retries,
                                long backoff, Predicate<Throwable> retryPredicate )
    {
        Objects.requireNonNull( gds );
        Objects.requireNonNull( monitor );
        if ( retries < 0 )
        {
            throw new IllegalArgumentException( "Number of retries must be greater than or equal to 0" );
        }
        if ( backoff < 0 )
        {
            throw new IllegalArgumentException( "Backoff time must be a positive number" );
        }
        Objects.requireNonNull( retryPredicate );

        this.gds = gds;
        this.monitor = monitor;
        this.retries = retries;
        this.backoff = backoff;
        this.retryPredicate = retryPredicate;
    }

    public TransactionTemplate with( GraphDatabaseService gds )
    {
        return new TransactionTemplate( gds, monitor, retries, backoff, retryPredicate );
    }

    public TransactionTemplate retries( int retries )
    {
        return new TransactionTemplate( gds, monitor, retries, backoff, retryPredicate );
    }

    public TransactionTemplate backoff( long backoff, TimeUnit unit )
    {
        return new TransactionTemplate( gds, monitor, retries, unit.toMillis( backoff ), retryPredicate );
    }

    public TransactionTemplate monitor( Monitor monitor )
    {
        return new TransactionTemplate( gds, monitor, retries, backoff, retryPredicate );
    }

    public TransactionTemplate retryOn( Predicate<Throwable> retryPredicate )
    {
        return new TransactionTemplate( gds, monitor, retries, backoff, retryPredicate );
    }

    /**
     * Executes a transaction with retry logic.
     *
     * @param txConsumer a consumer that takes transactions.
     * @throws TransactionFailureException if an error occurs other than those specified in {@link #retryOn(Predicate)}
     * or if the retry count was exceeded.
     */
    public void execute( final Consumer<Transaction> txConsumer )
    {
        execute( transaction ->
        {
            txConsumer.accept( transaction );
            return null;
        } );
    }

    /**
     * Executes a transaction with retry logic returning a result.
     *
     * @param txFunction function taking a transaction and producing a result.
     * @param <T> type of the result.
     * @return the result with type {@code T}.
     * @throws TransactionFailureException if an error occurs other than those specified in {@link #retryOn(Predicate)}
     * or if the retry count was exceeded.
     */
    public <T> T execute( Function<Transaction, T> txFunction ) throws TransactionFailureException
    {
        Throwable txEx;
        int retriesLeft = retries;
        while ( true )
        {
            try ( Transaction tx = gds.beginTx() )
            {
                T result = txFunction.apply( tx );
                tx.success();
                return result;
            }
            catch ( Throwable ex )
            {
                monitor.failure( ex );
                txEx = ex;

                if ( !retryPredicate.test( ex ) )
                {
                    break;
                }
            }

            try
            {
                Thread.sleep( backoff );
            }
            catch ( InterruptedException e )
            {
                TransactionFailureException interrupted = new TransactionFailureException( "Interrupted", e );
                monitor.failed( interrupted );
                throw interrupted;
            }

            if ( retriesLeft == 0 )
            {
                break;
            }
            retriesLeft--;
            monitor.retrying();
        }

        monitor.failed( txEx );
        throw new TransactionFailureException( "Failed", txEx );
    }
}
