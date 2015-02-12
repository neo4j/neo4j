/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.concurrent.TimeUnit;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;

import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.Predicates.or;

/**
 * Neo4j transaction template that automates the retry-on-exception logic. It uses the builder
 * pattern for configuration, with copy-semantics, so you can iteratively build up instances for
 * different scenarios.
 * <p/>
 * First instantiate and configure the template using the fluent API methods, and then
 * invoke execute which will begin/commit transactions in a loop for the specified number of times.
 * <p/>
 * By default all exceptions (except Errors and TransactionTerminatedException) cause a retry, and the monitor does nothing, but these can be overridden
 * with
 * custom behaviour.
 */
public class TransactionTemplate
{
    public interface Monitor
    {
        public void failure( Throwable ex );

        public void failed( Throwable ex );

        public void retrying();

        public class Adapter
                implements Monitor
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

    public TransactionTemplate()
    {
        this( null, new Monitor.Adapter(), 0, 0, not( or(
                Predicates.<Throwable>instanceOf( Error.class ),
                Predicates.<Throwable>instanceOf( TransactionTerminatedException.class ) ) ));
    }

    public TransactionTemplate( GraphDatabaseService gds, Monitor monitor, int retries,
                                long backoff, Predicate<Throwable> retryPredicate )
    {
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

    public <T> T execute( Function<Transaction, T> txFunction )
            throws TransactionFailureException
    {
        Throwable txEx = null;
        for ( int i = 0; i < retries; i++ )
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

                if ( !retryPredicate.accept( ex ) )
                {
                    break;
                }
            }

            if ( i < retries - 1 )
            {
                try
                {
                    Thread.sleep( backoff );
                }
                catch ( InterruptedException e )
                {
                    throw new TransactionFailureException( "Interrupted", e );
                }

                monitor.retrying();
            }
        }

        if ( txEx instanceof TransactionFailureException )
        {
            throw ((TransactionFailureException) txEx);
        }
        else if ( txEx instanceof Error )
        {
            throw ((Error) txEx);
        }
        else if ( txEx instanceof RuntimeException )
        {
            throw ((RuntimeException) txEx);
        }
        else
        {
            throw new TransactionFailureException( "Failed", txEx );
        }
    }
}
