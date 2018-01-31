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
package org.neo4j.bolt.v1.runtime.concurrent;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.v1.runtime.BoltConnectionDescriptor;
import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * A {@link WorkerFactory} implementation that creates one thread for every session started, requests are then executed
 * in the session-specific thread.
 *
 * This resolves a tricky issue where sharing threads for multiple sessions can cause complex deadlocks. It does so
 * at the expense of creating, potentially, many threads. However, this approach is much less complex than using
 * a thread pool, and is the preferred approach of several highly scalable relational databases.
 *
 * If we find ourselves with tens of thousands of concurrent sessions per neo4j instance, we may want to introduce an
 * alternate strategy.
 */
public class ThreadedWorkerFactory extends LifecycleAdapter implements WorkerFactory
{
    private final BoltFactory connector;
    private final LogService logging;
    private final Clock clock;
    private final ExecutorService threadPool;

    public ThreadedWorkerFactory( BoltFactory connector, JobScheduler scheduler, LogService logging, Clock clock )
    {
        this.connector = connector;
        this.logging = logging;
        this.clock = clock;
        this.threadPool = new ThreadPoolExecutor( 200, 1000, 20, TimeUnit.MINUTES,
                new SynchronousQueue<>(), new BoltNamedThreadFactory( "neo4j.Session" ) );
    }

    @Override
    public BoltWorker newWorker( BoltConnectionDescriptor connectionDescriptor, Runnable onClose )
    {
        BoltStateMachine machine = connector.newMachine( connectionDescriptor, onClose, clock );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logging );

        threadPool.execute( worker );

        return worker;
    }

    @Override
    public void shutdown()
    {
        threadPool.shutdownNow();
    }

    private static final class BoltNamedThreadFactory extends NamedThreadFactory
    {
        BoltNamedThreadFactory( String threadNamePrefix )
        {
            super( threadNamePrefix );
        }

        @Override
        public Thread newThread( Runnable runnable )
        {
            Thread superThread = super.newThread( runnable );
            if ( runnable instanceof RunnableBoltWorker )
            {
                String oldName = superThread.getName();
                RunnableBoltWorker worker = (RunnableBoltWorker) runnable;
                superThread.setName( String.format( "%s - %s", oldName, worker.getConnectionDescriptor().toString() ) );
            }
            return superThread;
        }
    }
}
