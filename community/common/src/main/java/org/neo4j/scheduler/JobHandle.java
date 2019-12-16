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
package org.neo4j.scheduler;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface JobHandle<T>
{
    void cancel();

    /**
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws CancellationException
     */
    void waitTermination() throws InterruptedException, ExecutionException;

    void waitTermination( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Waits if necessary for the computation to complete, and then
     * retrieves its result, similar to {@link Future#get()}.
     *
     * @return the computed result
     * @throws ExecutionException if the computation threw an exception
     * @throws InterruptedException if the current thread was interrupted while waiting
     */
    T get() throws ExecutionException, InterruptedException;

    default void registerCancelListener( CancelListener listener )
    {
        throw new UnsupportedOperationException( "Unsupported in this implementation" );
    }

    JobHandle<?> nullInstance = new NullJobHandle<>();

    class NullJobHandle<T> implements JobHandle<T>
    {
        @Override
        public void cancel()
        {   // no-op
        }

        @Override
        public void waitTermination()
        {   // no-op
        }

        @Override
        public void waitTermination( long timeout, TimeUnit unit )
        {   // no-op
        }

        @Override
        public T get()
        {
            return null;
        }
    }
}
