/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Constructors for basic {@link Future} types
 */
public class Futures
{
    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    @SafeVarargs
    public static <V> Future<List<V>> combine( final Future<? extends V>... futures )
    {
        return combine( Arrays.asList( futures ) );
    }

    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    public static <V> Future<List<V>> combine( final Iterable<? extends Future<? extends V>> futures )
    {
        return new Future<List<V>>()
        {
            @Override
            public boolean cancel( boolean mayInterruptIfRunning )
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.cancel( mayInterruptIfRunning );
                }
                return result;
            }

            @Override
            public boolean isCancelled()
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.isCancelled();
                }
                return result;
            }

            @Override
            public boolean isDone()
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.isDone();
                }
                return result;
            }

            @Override
            public List<V> get() throws InterruptedException, ExecutionException
            {
                List<V> result = new ArrayList<>();
                for ( Future<? extends V> future : futures )
                {
                    result.add( future.get() );
                }
                return result;
            }

            @Override
            public List<V> get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
            {
                List<V> result = new ArrayList<>();
                for ( Future<? extends V> future : futures )
                {
                    long before = System.nanoTime();
                    result.add( future.get( timeout, unit ) );
                    timeout -= unit.convert( System.nanoTime() - before, TimeUnit.NANOSECONDS );
                }
                return result;
            }
        };
    }
}
