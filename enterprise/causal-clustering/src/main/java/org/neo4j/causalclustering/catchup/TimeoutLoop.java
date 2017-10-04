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
