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
package org.neo4j.bolt.runtime;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachedThreadPoolExecutorFactory implements ExecutorFactory
{
    private final Log log;
    private final RejectedExecutionHandler rejectionHandler;

    public CachedThreadPoolExecutorFactory( Log log )
    {
        this( log, new ThreadPoolExecutor.AbortPolicy() );
    }

    public CachedThreadPoolExecutorFactory( Log log, RejectedExecutionHandler rejectionHandler )
    {
        this.log = log;
        this.rejectionHandler = rejectionHandler;
    }

    @Override
    public ExecutorService create( int corePoolSize, int maxPoolSize, Duration keepLive, int queueSize, ThreadFactory threadFactory )
    {
        ThreadPool result = new ThreadPool( corePoolSize, maxPoolSize, keepLive, createTaskQueue( queueSize ), threadFactory, rejectionHandler );

        return result;
    }

    @Override
    public void destroy( ExecutorService executor )
    {
        if ( !( executor instanceof ThreadPool ) )
        {
            throw new IllegalArgumentException(
                    String.format( "The passed executor should already be created by '%s#create()'.", CachedThreadPoolExecutorFactory.class.getName() ) );
        }

        executor.shutdown();
        try
        {
            if ( !executor.awaitTermination( 10, TimeUnit.SECONDS ) )
            {
                executor.shutdownNow();

                if ( !executor.awaitTermination( 10, TimeUnit.SECONDS ) )
                {
                    log.error( "Thread pool did not terminate gracefully despite all efforts" );
                }
            }
        }
        catch ( InterruptedException ex )
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static BlockingQueue createTaskQueue( int queueSize )
    {
        if ( queueSize == -1 )
        {
            return new LinkedBlockingQueue();
        }
        else if ( queueSize == 0 )
        {
            return new SynchronousQueue();
        }
        else if ( queueSize > 0 )
        {
            return new ArrayBlockingQueue( queueSize );
        }

        throw new IllegalArgumentException( String.format( "Unsupported queue size %d for thread pool creation.", queueSize ) );
    }

    private class ThreadPool extends ThreadPoolExecutor
    {

        private ThreadPool( int corePoolSize, int maxPoolSize, Duration keepLive, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                RejectedExecutionHandler rejectionHandler )
        {
            super( corePoolSize, maxPoolSize, keepLive.toMillis(), MILLISECONDS, workQueue, threadFactory, rejectionHandler );
        }

    }

}
