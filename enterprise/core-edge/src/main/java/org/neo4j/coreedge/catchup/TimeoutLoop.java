/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

class TimeoutLoop
{
    static <T> T waitForCompletion( Future<T> future, Supplier<Long> millisSinceLastResponseSupplier,
                                    long inactivityTimeout, TimeUnit timeUnit ) throws CatchUpClientException
    {
        long remainingTimeoutMillis = timeUnit.toMillis( inactivityTimeout );
        while ( true )
        {
            try
            {
                return future.get( remainingTimeoutMillis, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new CatchUpClientException( e );
            }
            catch ( ExecutionException e )
            {
                throw new CatchUpClientException( e );
            }
            catch ( TimeoutException e )
            {
                long millisSinceLastResponse = millisSinceLastResponseSupplier.get();
                if ( millisSinceLastResponse < timeUnit.toMillis( inactivityTimeout ) )
                {
                    remainingTimeoutMillis = timeUnit.toMillis( inactivityTimeout ) - millisSinceLastResponse;
                }
                else
                {
                    throw new CatchUpClientException( e );
                }
            }
        }
    }
}
