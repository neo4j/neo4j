/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

interface ExecutorServiceFactory
{
    ExecutorService build( SchedulerThreadFactory factory, OptionalInt threadCount );

    static ExecutorServiceFactory singleThread()
    {
        return ( factory, threadCount ) -> newSingleThreadExecutor( factory );
    }

    static ExecutorServiceFactory cached()
    {
        return ( factory, threadCount ) ->
        {
            if ( threadCount.isPresent() )
            {
                int threadCountAsInt = threadCount.getAsInt();
                if ( threadCountAsInt == 1 )
                {
                    return newSingleThreadExecutor( factory );
                }
                return newFixedThreadPool( threadCountAsInt, factory );
            }
            return newCachedThreadPool( factory );
        };
    }

    static ExecutorServiceFactory workStealing()
    {
        return ( factory, threadCount ) -> new ForkJoinPool( threadCount.orElse( getRuntime().availableProcessors() ), factory, null, false );
    }
}
