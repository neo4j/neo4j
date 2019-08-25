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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.scheduler.Group;

import static java.util.Objects.requireNonNullElseGet;

final class ThreadPoolManager
{
    private final ConcurrentHashMap<Group,ThreadPool> pools;
    private final ThreadGroup topLevelGroup;
    private boolean shutdown;

    ThreadPoolManager( ThreadGroup topLevelGroup )
    {
        this.topLevelGroup = topLevelGroup;
        pools = new ConcurrentHashMap<>();
    }

    ThreadPool getThreadPool( Group group )
    {
        return getThreadPool( group, null );
    }

    ThreadPool getThreadPool( Group group, ThreadPool.ThreadPoolParameters parameters )
    {
        return pools.computeIfAbsent( group, g -> createThreadPool( g, parameters ) );
    }

    boolean isStarted( Group group )
    {
        return pools.containsKey( group );
    }

    void assumeNotStarted( Group group )
    {
        if ( isStarted( group ) )
        {
            throw new IllegalStateException( group.groupName() + " is already been started. " );
        }
    }

    synchronized void forEachStarted( BiConsumer<Group, ThreadPool> consumer )
    {
        assertNotShutDown();
        pools.forEach( consumer );
    }

    private synchronized ThreadPool createThreadPool( Group group, ThreadPool.ThreadPoolParameters parameters )
    {
        assertNotShutDown();
        return new ThreadPool( group, topLevelGroup, requireNonNullElseGet( parameters, ThreadPool.ThreadPoolParameters::new ) );
    }

    private void assertNotShutDown()
    {
        if ( shutdown )
        {
            throw new IllegalStateException( "ThreadPoolManager is shutdown." );
        }
    }

    synchronized InterruptedException shutDownAll()
    {
        shutdown = true;
        pools.forEach( ( group, pool ) -> pool.cancelAllJobs() );
        pools.forEach( ( group, pool ) -> pool.shutDown() );
        return pools.values().stream()
                    .map( ThreadPool::getShutdownException )
                    .reduce( null, Exceptions::chain );
    }
}
