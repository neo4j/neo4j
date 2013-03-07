/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class FutureAdapter<V> implements Future<V>
{
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
    };
    
    public static final Future<Void> VOID = new Present<Void>( null );
    
    public static <T> Future<T> latchGuardedValue( final ValueGetter<T> value, final CountDownLatch guardedByLatch )
    {
        return new FutureAdapter<T>()
        {
            @Override
            public boolean isDone()
            {
                return guardedByLatch.getCount() == 0;
            }

            @Override
            public T get() throws InterruptedException, ExecutionException
            {
                guardedByLatch.await();
                return value.get();
            }

            @Override
            public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException,
                    TimeoutException
            {
                if ( !guardedByLatch.await( timeout, unit ) )
                    throw new TimeoutException( "Index population job cancel didn't complete within " +
                            timeout + " " + unit );
                return value.get();
            }
        };
    }
}
