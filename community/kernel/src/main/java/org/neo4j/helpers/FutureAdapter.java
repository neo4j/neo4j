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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Deprecated
public abstract class FutureAdapter<V> implements Future<V>
{
    public static final Future<Void> VOID = CompletableFuture.completedFuture( null );

    @Override
    public boolean cancel( boolean mayInterruptIfRunning )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * This class will be deleted as part of next major release. Please use {@link CompletableFuture#complete(Object)}
     * instead.
     */
    @Deprecated
    public static class Present<V> extends FutureAdapter<V>
    {
        private final V value;

        public Present( V value )
        {
            this.value = value;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public V get()
        {
            return value;
        }
        @Override
        public V get( long timeout, TimeUnit unit )
        {
            return value;
        }
    }

    /**
     * @param <T> type of values that this {@link Future} have.
     * @param value result value.
     * @return {@link Present} future with already specified result
     *
     * This method will be deleted as part of next major release. Please use {@link CompletableFuture#complete(Object)}
     * instead.
     */
    @Deprecated
    public static <T> Present<T> present( T value )
    {
        return new Present<>( value );
    }

    @Deprecated
    public static <T> Future<T> future( final Callable<T> task )
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit( task );
        executor.shutdown();
        return future;
    }
}
