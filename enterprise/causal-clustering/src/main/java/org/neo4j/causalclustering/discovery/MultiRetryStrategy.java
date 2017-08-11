/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * Implementation of the RetryStrategy that repeats the retriable function until the correct result has been retrieved or the limit of retries has been
 * encountered.
 * There is a fixed delay between each retry.
 *
 * @param <I> the type of input of the retriable function
 * @param <E> the type of output of the retriable function
 */
public class MultiRetryStrategy<I, E> implements RetryStrategy<I,E>
{
    private final long delayInMillis;
    private final long retries;
    private final LogProvider logProvider;

    /**
     * @param delayInMillis number of milliseconds between each attempt at getting the desired result
     * @param retries the number of attempts to perform before giving up
     */
    public MultiRetryStrategy( long delayInMillis, long retries )
    {
        this( delayInMillis, retries, NullLogProvider.getInstance() );
    }

    /**
     * @param delayInMillis number of milliseconds between each attempt at getting the desired result
     * @param retries the number of attempts to perform before giving up
     * @param logProvider {@see LogProvider}
     */
    public MultiRetryStrategy( long delayInMillis, long retries, LogProvider logProvider )
    {
        this.delayInMillis = delayInMillis;
        this.retries = retries;
        this.logProvider = logProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E apply( I input, Function<I,E> retriable, Predicate<E> wasRetrySuccessful )
    {
        Log log = logProvider.getLog( MultiRetryStrategy.class );
        E result = retriable.apply( input );
        int currentIteration = 0;
        while ( !wasRetrySuccessful.test( result ) && currentIteration++ < retries )
        {
            log.debug( "Try attempt was unsuccessful for input: %s\n", input );
            try
            {
                Thread.sleep( delayInMillis );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            result = retriable.apply( input );
        }
        return result;
    }
}
