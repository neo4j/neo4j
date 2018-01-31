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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class ExecutorBoltSchedulerProvider extends LifecycleAdapter implements BoltSchedulerProvider
{
    private final Config config;
    private final ExecutorFactory executorFactory;
    private final JobScheduler scheduler;
    private final LogService logService;
    private final ConcurrentHashMap<String, BoltScheduler> boltSchedulers;

    private ExecutorService forkJoinThreadPool;

    public ExecutorBoltSchedulerProvider( Config config, ExecutorFactory executorFactory, JobScheduler scheduler, LogService logService )
    {
        this.config = config;
        this.executorFactory = executorFactory;
        this.scheduler = scheduler;
        this.logService = logService;
        this.boltSchedulers = new ConcurrentHashMap<>();
    }

    @Override
    public void start()
    {
        forkJoinThreadPool = new ForkJoinPool();
        config.enabledBoltConnectors().forEach( connector ->
        {
            BoltScheduler boltScheduler =
                    new ExecutorBoltScheduler( connector.key(), executorFactory, scheduler, logService, config.get( connector.thread_pool_core_size ),
                            config.get( connector.thread_pool_max_size ), config.get( connector.thread_pool_keep_alive ),
                            config.get( connector.thread_pool_queue_size ), forkJoinThreadPool );
            try
            {
                boltScheduler.start();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
            boltSchedulers.put( connector.key(), boltScheduler );
        } );
    }

    @Override
    public void stop()
    {
        boltSchedulers.values().forEach( s -> {
            try
            {
                s.stop();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        } );
        boltSchedulers.clear();

        forkJoinThreadPool.shutdown();
        forkJoinThreadPool = null;
    }

    @Override
    public BoltScheduler get( BoltChannel channel )
    {
        BoltScheduler boltScheduler = boltSchedulers.get( channel.connector() );
        if ( boltScheduler == null )
        {
            throw new IllegalArgumentException(
                    String.format( "Provided channel instance [local: %s, remote: %s] is not bound to any known bolt listen addresses.",
                            channel.serverAddress(), channel.clientAddress() ) );
        }

        return boltScheduler;
    }

}
