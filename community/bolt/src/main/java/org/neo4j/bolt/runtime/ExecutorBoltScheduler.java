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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

public class ExecutorBoltScheduler implements BoltScheduler, BoltConnectionListener, BoltConnectionQueueMonitor
{
    private final ExecutorFactory executorFactory;
    private final JobScheduler scheduler;
    private final Log log;
    private final ConcurrentHashMap<String, BoltConnection> activeConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> activeWorkItems = new ConcurrentHashMap<>();
    private final int corePoolSize;
    private final int maxPoolSize;
    private final Duration keepAlive;
    private final int queueSize;

    private ExecutorService threadPool;

    public ExecutorBoltScheduler( ExecutorFactory executorFactory, JobScheduler scheduler, LogService logService, int corePoolSize, int maxPoolSize,
            Duration keepAlive, int queueSize )
    {
        this.executorFactory = executorFactory;
        this.scheduler = scheduler;
        this.log = logService.getInternalLog( getClass() );
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAlive = keepAlive;
        this.queueSize = queueSize;
    }

    boolean isRegistered( BoltConnection connection )
    {
        synchronized ( connection )
        {
            return activeConnections.containsKey( connection.id() );
        }
    }

    boolean isActive( BoltConnection connection )
    {
        synchronized ( connection )
        {
            return activeWorkItems.containsKey( connection.id() );
        }
    }

    public void start()
    {
        threadPool = executorFactory.create( corePoolSize, maxPoolSize, keepAlive, queueSize, scheduler.threadFactory( JobScheduler.Groups.boltWorker ) );
    }

    public void stop()
    {
        executorFactory.destroy( threadPool );
    }

    @Override
    public void created( BoltConnection connection )
    {
        activeConnections.put( connection.id(), connection );
    }

    @Override
    public void destroyed( BoltConnection connection )
    {
        String id = connection.id();

        try
        {
            CompletableFuture currentFuture = activeWorkItems.remove( id );
            if ( currentFuture != null )
            {
                currentFuture.cancel( true );
            }
        }
        finally
        {
            activeConnections.remove( id );
        }
    }

    @Override
    public void enqueued( BoltConnection to, Job job )
    {
        handleSubmission( to );
    }

    @Override
    public void drained( BoltConnection from, Collection<Job> batch )
    {

    }

    private void handleSubmission( BoltConnection connection )
    {
        String id = connection.id();

        if ( !activeWorkItems.containsKey( id ) )
        {
            synchronized ( connection )
            {
                if ( !activeWorkItems.containsKey( id ) )
                {
                    activeWorkItems.put( id, CompletableFuture.runAsync( () -> connection.processNextBatch(), threadPool ).whenCompleteAsync(
                            ( result, error ) -> handleCompletion( connection, result, error ), threadPool ) );
                }
            }
        }
    }

    private void handleCompletion( BoltConnection connection, Object result, Throwable error )
    {
        synchronized ( connection )
        {
            CompletableFuture<Void> previousFuture = activeWorkItems.remove( connection.id() );

            if ( error != null )
            {
                log.error( String.format( "Unexpected error during job scheduling for session '%s'.", connection.id() ), error );
                connection.stop();
            }
            else
            {
                if ( connection.hasPendingJobs() )
                {
                    previousFuture.thenAcceptAsync( ignore -> handleSubmission( connection ), threadPool );
                }
            }
        }
    }

}
