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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.ThrowingSupplier;

/**
 * A {@link Future} that may be explicitly completed (setting its value and status)
 *
 * @param <T> The result type returned by this Future's get method
 */
public class CompletableFuture<T> implements Future<T>
{
    private interface State<T> extends ThrowingSupplier<T,ExecutionException>
    {
    }

    private static final State<?> UNRESOLVED = new State()
    {
        @Override
        public Object get()
        {
            throw new IllegalStateException();
        }
    };
    private static final State<?> CANCELLED = new State()
    {
        @Override
        public Object get()
        {
            throw new CancellationException();
        }
    };

    @SuppressWarnings( "unchecked" )
    private final AtomicReference<State<T>> state = new AtomicReference<>( (State<T>) UNRESOLVED );
    private final CountDownLatch latch = new CountDownLatch( 1 );

    /**
     * If not already completed, sets the value returned by {@link #get()} and related methods to the given value.
     *
     * @param value the result value
     * @return true if this invocation caused this CompletableFuture to transition to a completed state, else false
     */
    public boolean complete( final T value )
    {
        return this.state.get() == UNRESOLVED && resolve( new State<T>()
        {
            @Override
            public T get()
            {
                return value;
            }
        } );
    }

    /**
     * If not already completed, causes invocations of {@link #get()} and related methods to throw the given exception.
     *
     * @param ex the exception
     * @return true if this invocation caused this CompletableFuture to transition to a completed state, else false
     */
    public boolean completeExceptionally( final Throwable ex )
    {
        return this.state.get() == UNRESOLVED && resolve( new State<T>()
        {
            @Override
            public T get() throws ExecutionException
            {
                throw new ExecutionException( ex );
            }
        } );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public boolean cancel( boolean mayInterruptIfRunning )
    {
        return this.state.get() == UNRESOLVED && resolve( (State<T>) CANCELLED );
    }

    @SuppressWarnings( "unchecked" )
    private boolean resolve( State<T> newState )
    {
        if ( !this.state.compareAndSet( (State<T>) UNRESOLVED, newState ) )
        {
            return false;
        }
        latch.countDown();
        return true;
    }

    @Override
    public boolean isCancelled()
    {
        return state.get() == CANCELLED;
    }

    @Override
    public boolean isDone()
    {
        return state.get() != UNRESOLVED;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException
    {
        latch.await();
        return state.get().get();
    }

    @Override
    public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
    {
        if( !latch.await( timeout, unit ) )
        {
            throw new TimeoutException();
        }
        return state.get().get();
    }
}
