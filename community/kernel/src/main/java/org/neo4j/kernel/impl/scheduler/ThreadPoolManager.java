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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.neo4j.helpers.Exceptions;
import org.neo4j.scheduler.JobScheduler.Group;
import org.neo4j.scheduler.JobScheduler.JobHandle;

final class ThreadPoolManager
{
    private final ConcurrentHashMap<Group,ThreadPool> pools;
    private final Function<Group,ThreadPool> poolBuilder;

    ThreadPoolManager( ThreadGroup topLevelGroup )
    {
        pools = new ConcurrentHashMap<>();
        poolBuilder = group -> new ThreadPool( group, topLevelGroup );
    }

    ThreadPool getThreadPool( Group group )
    {
        return pools.computeIfAbsent( group, poolBuilder );
    }

    JobHandle submit( Group group, Runnable job )
    {
        ThreadPool threadPool = getThreadPool( group );
        return threadPool.submit( job );
    }

    public InterruptedException shutDownAll()
    {
        pools.forEach( ( group, pool ) -> pool.cancelAllJobs() );
        pools.forEach( ( group, pool ) -> pool.shutDown() );
        return pools.values().stream()
                    .map( ThreadPool::getShutdownException )
                    .reduce( null, Exceptions::chain );
    }
}
