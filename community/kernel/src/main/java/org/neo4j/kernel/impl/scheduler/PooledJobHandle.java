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
package org.neo4j.kernel.impl.scheduler;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.scheduler.JobScheduler;

final class PooledJobHandle implements JobScheduler.JobHandle
{
    private final Future<?> future;
    private final Object registryKey;
    private final ConcurrentHashMap<Object,Future<?>> registry;
    private final List<JobScheduler.CancelListener> cancelListeners = new CopyOnWriteArrayList<>();

    PooledJobHandle( Future<?> future, Object registryKey, ConcurrentHashMap<Object,Future<?>> registry )
    {
        this.future = future;
        this.registryKey = registryKey;
        this.registry = registry;
    }

    @Override
    public void cancel( boolean mayInterruptIfRunning )
    {
        future.cancel( mayInterruptIfRunning );
        for ( JobScheduler.CancelListener cancelListener : cancelListeners )
        {
            cancelListener.cancelled( mayInterruptIfRunning );
        }
        registry.remove( registryKey );
    }

    @Override
    public void waitTermination() throws InterruptedException, ExecutionException
    {
        future.get();
    }

    @Override
    public void registerCancelListener( JobScheduler.CancelListener listener )
    {
        cancelListeners.add( listener );
    }
}
