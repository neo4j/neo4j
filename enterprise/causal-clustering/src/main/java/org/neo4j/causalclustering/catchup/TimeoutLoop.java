/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.Log;

class TimeoutLoop
{
    private TimeoutLoop()
    {
    }

    static <T> T waitForCompletion( Future<T> future, String operation, Supplier<Optional<Long>> millisSinceLastResponseSupplier,
                                    long inactivityTimeoutMillis, Log log ) throws CatchUpClientException
    {
        long remainingTimeoutMillis = inactivityTimeoutMillis;
        while ( true )
        {
            try
            {
                return future.get( remainingTimeoutMillis, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw exception( future, operation, e );
            }
            catch ( ExecutionException e )
            {
                throw exception( future, operation, e );
            }
            catch ( TimeoutException e )
            {
                if ( !millisSinceLastResponseSupplier.get().isPresent() )
                {
                    log.info( "Request timed out with no responses after " + inactivityTimeoutMillis + " ms." );
                    throw exception( future, operation, e );
                }
                else
                {
                    long millisSinceLastResponse = millisSinceLastResponseSupplier.get().get();
                    if ( millisSinceLastResponse < inactivityTimeoutMillis )
                    {
                        remainingTimeoutMillis = inactivityTimeoutMillis - millisSinceLastResponse;
                    }
                    else
                    {
                        log.info( "Request timed out after period of inactivity. Time since last response: " +
                                millisSinceLastResponse + " ms." );
                        throw exception( future, operation, e );
                    }
                }
            }
        }
    }

    private static CatchUpClientException exception( Future<?> future, String operation, Exception e )
    {
        future.cancel( true );
        return new CatchUpClientException( operation, e );
    }
}
