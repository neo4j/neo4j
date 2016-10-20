/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.state;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Supplies an executor service suitable for the core state.
 * Running tasks can be cancelled using a soft abort mechanism.
 */
public class CoreStateApplier
{
    public class Status
    {
        private volatile boolean cancelled;

        boolean isCancelled()
        {
            return cancelled;
        }
    }

    private final Log log;
    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "core-state-applier" );

    private final Status status = new Status();
    private ExecutorService applier;
    private boolean isPanic;

    public CoreStateApplier( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        spawnExecutor();
    }

    private void spawnExecutor()
    {
        status.cancelled = false;
        log.info( "Spawning new applier" );
        applier = newSingleThreadExecutor( threadFactory );
    }

    /**
     * Submit a task that can check the shutdown flag periodically to
     * abort its operation.
     *
     * @param abortableTask A function that creates a runnable that can use
     *                      the status flag to decide when to abort.
     */
    public boolean submit( Function<Status,Runnable> abortableTask )
    {
        if ( isPanic )
        {
            log.warn( "Task submitted while panicked" );
            return false;
        }

        if ( status.cancelled )
        {
            log.warn( "Task submitted while cancelled" );
        }

        applier.submit( wrapWithCatchAll( abortableTask.apply( status ) ) );
        return true;
    }

    private Runnable wrapWithCatchAll( Runnable task )
    {
        return () ->
        {
            try
            {
                task.run();
            }
            catch ( Throwable e )
            {
                log.error( "Task had an exception", e );
            }
        };
    }

    /**
     * Used for synchronizing with the internal executor.
     *
     * @param cancelTasks Whether or not to flag for cancelling.
     */
    public void sync( boolean cancelTasks )
    {
        if ( cancelTasks )
        {
            log.info( "Syncing with cancel" );
            status.cancelled = true;
        }
        else
        {
            log.info( "Syncing" );
        }

        applier.shutdown();

        do
        {
            try
            {
                applier.awaitTermination( 1, MINUTES );
            }
            catch ( InterruptedException ignored )
            {
                log.warn( "Unexpected interrupt", ignored );
            }

            if ( !applier.isTerminated() )
            {
                log.warn( "Applier is taking an unusually long time to sync" );
            }
        }
        while ( !applier.isTerminated() );

        spawnExecutor();
    }

    public void panic()
    {
        log.error( "Applier panicked" );
        applier.shutdown();
        isPanic = true;
    }
}
