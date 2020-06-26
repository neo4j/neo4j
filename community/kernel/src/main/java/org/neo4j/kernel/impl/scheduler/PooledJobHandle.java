/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.scheduler;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.scheduler.CancelListener;
import org.neo4j.scheduler.JobHandle;

final class PooledJobHandle<T> implements JobHandle<T>
{
    private final Future<T> future;
    private final Object registryKey;
    private final ConcurrentHashMap<Object,?> registry;
    private final List<CancelListener> cancelListeners = new CopyOnWriteArrayList<>();

    PooledJobHandle( Future<T> future, Object registryKey, ConcurrentHashMap<Object,?> registry )
    {
        this.future = future;
        this.registryKey = registryKey;
        this.registry = registry;
    }

    @Override
    public void cancel()
    {
        future.cancel( false );
        for ( CancelListener cancelListener : cancelListeners )
        {
            cancelListener.cancelled();
        }
        registry.remove( registryKey );
    }

    @Override
    public void waitTermination() throws InterruptedException, ExecutionException
    {
        future.get();
    }

    @Override
    public void waitTermination( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
    {
        future.get( timeout, unit );
    }

    @Override
    public T get() throws ExecutionException, InterruptedException
    {
        return future.get();
    }

    @Override
    public void registerCancelListener( CancelListener listener )
    {
        cancelListeners.add( listener );
    }
}
