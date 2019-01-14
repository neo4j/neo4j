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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

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

    public static <T> Future<T> latchGuardedValue( final Supplier<T> supplier, final CountDownLatch guardedByLatch,
                                                   final String jobDescription )
    {
        return new FutureAdapter<T>()
        {
            @Override
            public boolean isDone()
            {
                return guardedByLatch.getCount() == 0;
            }

            @Override
            public T get() throws InterruptedException
            {
                guardedByLatch.await();
                return supplier.get();
            }

            @Override
            public T get( long timeout, TimeUnit unit ) throws InterruptedException, TimeoutException
            {
                if ( !guardedByLatch.await( timeout, unit ) )
                {
                    throw new TimeoutException( jobDescription + " didn't complete within " +
                            timeout + " " + unit );
                }
                return supplier.get();
            }
        };
    }

    public static Future<Integer> processFuture( final Process process )
    {
        return new FutureAdapter<Integer>()
        {
            @Override
            public boolean isDone()
            {
                return tryGetExitValue( process ) != null;
            }

            private Integer tryGetExitValue( final Process process )
            {
                try
                {
                    return process.exitValue();
                }
                catch ( IllegalThreadStateException e )
                {   // Thrown if this process hasn't exited yet.
                    return null;
                }
            }

            @Override
            public Integer get() throws InterruptedException
            {
                return process.waitFor();
            }

            @Override
            public Integer get( long timeout, TimeUnit unit ) throws InterruptedException, TimeoutException
            {
                long end = System.currentTimeMillis() + unit.toMillis( timeout );
                while ( System.currentTimeMillis() < end )
                {
                    Integer result = tryGetExitValue( process );
                    if ( result != null )
                    {
                        return result;
                    }
                    Thread.sleep( 10 );
                }
                throw new TimeoutException( "Process '" + process + "' didn't exit within " + timeout + " " + unit );
            }
        };
    }

    public static <T> Future<T> future( final Callable<T> task )
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit( task );
        executor.shutdown();
        return future;
    }
}
