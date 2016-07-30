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
package org.neo4j.coreedge.core.state;

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

        public boolean isCancelled()
        {
            return cancelled;
        }
    }

    private final Log log;
    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "core-state-applier" );

    private final Status status = new Status();
    private ExecutorService applier;

    public CoreStateApplier( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        spawnExecutor();
    }

    private void spawnExecutor()
    {
        status.cancelled = false;
        applier = newSingleThreadExecutor( threadFactory );
    }

    /**
     * Submit a task that can check the shutdown flag periodically to
     * abort its operation.
     *
     * @param abortableTask A function that creates a runnable that can use
     *                      the status flag to decide when to abort.
     */
    public void submit( Function<Status,Runnable> abortableTask )
    {
        if ( status.cancelled )
        {
            log.warn( "Task submitted while cancelled" );
        }

        applier.submit( abortableTask.apply( status ) );
    }

    /**
     * Used for synchronizing with the internal executor.
     *
     * @param cancelTasks Whether or not to flag for cancelling.
     *
     * @throws InterruptedException
     */
    public void sync( boolean cancelTasks ) throws InterruptedException
    {
        if ( applier != null )
        {
            if ( cancelTasks )
            {
                status.cancelled = true;
            }

            applier.shutdown();

            while ( !applier.awaitTermination( 1, MINUTES ) )
            {
                log.warn( "Applier is taking an unusually long time to sync" );
            }
        }

        spawnExecutor();
    }
}
