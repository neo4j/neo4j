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
package org.neo4j.collection.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;

public class PoolPerformance
{
    public static void main(String ... args) throws InterruptedException, ExecutionException
    {
        final Pool<Object> pool = new MarshlandPool<>( objectFactory() );
//        final Pool<Object> pool = new LinkedQueuePool<>( 16, objectFactory() );

        int iterations = 100000000;

        bench( pool, (long) iterations, 1 );
        bench( pool, (long) iterations / 2, 2 );
        bench( pool, (long) iterations / 4, 4 );
        bench( pool, (long) iterations / 8, 8 );
    }

    private static Factory<Object> objectFactory()
    {
        return new Factory<Object>()
        {
            @Override
            public Object newInstance()
            {
                return new Object();
            }
        };
    }

    private static void bench( Pool<Object> pool, long iterations, int concurrency ) throws InterruptedException, ExecutionException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

        long start = System.nanoTime();
        List<Future<Object>> futures = executorService.invokeAll( workers( pool, iterations, concurrency ) );
        awaitAll( futures );
        long delta = System.nanoTime() - start;

        System.out.println("With "+ concurrency +" threads: " + (iterations * concurrency) / (delta / 1000_000) + " iterations/ms");

        executorService.shutdownNow();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private static void awaitAll( List<Future<Object>> futures ) throws InterruptedException, ExecutionException
    {
        for ( Future<Object> future : futures )
        {
            future.get();
        }
    }

    private static List<Callable<Object>> workers( final Pool<Object> pool, final long iterations, final int numWorkers )
    {
        List<Callable<Object>> workers = new ArrayList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            workers.add( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    for ( int i = 0; i < iterations; i++ )
                    {
                        pool.release( pool.acquire() );
                    }
                    return null;
                }
            });
        }
        return workers;
    }
}
