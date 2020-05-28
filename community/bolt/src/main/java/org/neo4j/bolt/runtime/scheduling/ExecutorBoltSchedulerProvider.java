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
package org.neo4j.bolt.runtime.scheduling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.util.Preconditions.checkState;

public class ExecutorBoltSchedulerProvider extends LifecycleAdapter implements BoltSchedulerProvider
{
    private final Config config;
    private final ExecutorFactory executorFactory;
    private final JobScheduler scheduler;
    private final LogService logService;
    private final Log internalLog;
    private volatile BoltScheduler boltScheduler;

    private ExecutorService forkJoinThreadPool;

    public ExecutorBoltSchedulerProvider( Config config, ExecutorFactory executorFactory, JobScheduler scheduler,
            LogService logService )
    {
        this.config = config;
        this.executorFactory = executorFactory;
        this.scheduler = scheduler;
        this.logService = logService;
        this.internalLog = logService.getInternalLog( getClass() );
    }

    @Override
    public void init()
    {
        scheduler.setThreadFactory( Group.BOLT_WORKER, NettyThreadFactory::new );
        if ( config.get( BoltConnector.enabled ) )
        {
            checkState( forkJoinThreadPool == null, "ForkJoinPool already initialized, this should only be done once." );
            forkJoinThreadPool = new ForkJoinPool();
            this.boltScheduler =
                    new ExecutorBoltScheduler( BoltConnector.NAME, executorFactory, scheduler, logService, config.get( BoltConnector.thread_pool_min_size ),
                            config.get( BoltConnector.thread_pool_max_size ), config.get( BoltConnector.thread_pool_keep_alive ),
                            config.get( BoltConnectorInternalSettings.unsupported_thread_pool_queue_size ), forkJoinThreadPool,
                            config.get( BoltConnector.thread_pool_shutdown_wait_time ),
                            config.get( BoltConnectorInternalSettings.connection_keep_alive_scheduling_interval ) );
            this.boltScheduler.init();
        }
    }

    @Override
    public void start()
    {
        if ( boltScheduler != null )
        {
            boltScheduler.start();
        }
    }

    @Override
    public void stop()
    {
        if ( boltScheduler != null )
        {
            boltScheduler.stop();
        }
    }

    @Override
    public void shutdown()
    {
        if ( boltScheduler != null )
        {
            try
            {
                boltScheduler.shutdown();
            }
            catch ( Throwable t )
            {
                internalLog.warn( String.format( "An unexpected error occurred while shutting down BoltScheduler [%s]", boltScheduler.connector() ), t );
            }
            boltScheduler = null;
        }

        if ( forkJoinThreadPool != null )
        {
            forkJoinThreadPool.shutdown();
            forkJoinThreadPool = null;
        }
    }

    @Override
    public BoltScheduler get( BoltChannel channel )
    {
        if ( boltScheduler == null )
        {
            throw new IllegalArgumentException(
                    String.format( "Provided channel instance [local: %s, remote: %s] is not bound to any known bolt listen addresses.",
                            channel.serverAddress(), channel.clientAddress() ) );
        }

        return boltScheduler;
    }
}
