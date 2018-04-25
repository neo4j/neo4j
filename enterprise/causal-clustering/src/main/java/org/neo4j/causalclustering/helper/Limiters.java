/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.helper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.time.Clocks;

@SuppressWarnings( "WeakerAccess" )
public class Limiters
{
    private final ConcurrentHashMap<Object,Consumer<Runnable>> caps = new ConcurrentHashMap<>();
    private final Clock clock;

    public Limiters()
    {
        this.clock = Clocks.systemClock();
    }

    public Limiters( Clock clock )
    {
        this.clock = clock;
    }

    /**
     * Rate limits calls under the specified handle.
     *
     * @param handle A unique handle.
     * @param minInterval The minimum interval between invocations.
     * @return A rate limited consumer of {@link Runnable}s.
     */
    public Consumer<Runnable> rateLimiter( Object handle, Duration minInterval )
    {
        return caps.computeIfAbsent( handle, ignored -> Limiters.rateLimiter( minInterval, clock ) );
    }

    public static Consumer<Runnable> rateLimiter( Duration minInterval )
    {
        return rateLimiter( minInterval, Clocks.systemClock() );
    }

    /**
     * Rate limits calls.
     *
     * @param minInterval The minimum interval between invocations.
     * @return A rate limited consumer of {@link Runnable}s.
     */
    public static Consumer<Runnable> rateLimiter( Duration minInterval, Clock clock )
    {
        return new Consumer<Runnable>()
        {
            AtomicReference<Instant> lastRunRef = new AtomicReference<>();

            @Override
            public void accept( Runnable operation )
            {
                Instant now = clock.instant();
                Instant lastRun = lastRunRef.get();

                if ( lastRun == null )
                {
                    if ( lastRunRef.compareAndSet( null, now ) )
                    {
                        operation.run();
                    }
                    return;
                }

                if ( lastRun.plus( minInterval ).isAfter( now ) )
                {
                    return;
                }

                if ( lastRunRef.compareAndSet( lastRun, now ) )
                {
                    operation.run();
                }
            }
        };
    }
}
