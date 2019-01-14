/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Implementation of the RetryStrategy that repeats the retriable function until the correct result has been retrieved or the limit of retries has been
 * encountered.
 * There is a fixed delay between each retry.
 *
 * @param <INPUT> the type of input of the retriable function
 * @param <OUTPUT> the type of output of the retriable function
 */
public class MultiRetryStrategy<INPUT, OUTPUT> implements RetryStrategy<INPUT,OUTPUT>
{
    private final long delayInMillis;
    private final long retries;
    private final LogProvider logProvider;
    private final LongConsumer sleeper;

    /**
     * @param delayInMillis number of milliseconds between each attempt at getting the desired result
     * @param retries the number of attempts to perform before giving up
     * @param logProvider {@see LogProvider}
     */
    public MultiRetryStrategy( long delayInMillis, long retries, LogProvider logProvider, LongConsumer sleeper )
    {
        this.delayInMillis = delayInMillis;
        this.retries = retries;
        this.logProvider = logProvider;
        this.sleeper = sleeper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OUTPUT apply( INPUT retriableInput, Function<INPUT,OUTPUT> retriable, Predicate<OUTPUT> wasRetrySuccessful )
    {
        Log log = logProvider.getLog( MultiRetryStrategy.class );
        OUTPUT result = retriable.apply( retriableInput );
        int currentIteration = 0;
        while ( !wasRetrySuccessful.test( result ) && currentIteration++ < retries )
        {
            log.debug( "Try attempt was unsuccessful for input: %s\n", retriableInput );
            sleeper.accept( delayInMillis );
            result = retriable.apply( retriableInput );
        }
        return result;
    }
}
