/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import java.util.concurrent.ThreadFactory;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Invokes the supplied task continuously when started. The supplied task
 * should be short since the abort flag is checked in between invocations.
 */
public class ContinuousJob extends LifecycleAdapter
{
    private final AbortableJob abortableJob;
    private final Log log;
    private final Thread thread;

    public ContinuousJob( ThreadFactory threadFactory, Runnable task, LogProvider logProvider )
    {
        this.abortableJob = new AbortableJob( task );
        this.thread = threadFactory.newThread( abortableJob );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        abortableJob.keepRunning = true;
        thread.start();
    }

    @Override
    public void stop() throws Throwable
    {
        log.info( "ContinuousJob " + thread.getName() + " stopping" );
        abortableJob.keepRunning = false;
        thread.join();
    }

    private static class AbortableJob implements Runnable
    {
        private final Runnable task;
        private volatile boolean keepRunning;

        AbortableJob( Runnable task )
        {
            this.task = task;
        }

        @Override
        public void run()
        {
            while ( keepRunning )
            {
                task.run();
            }
        }
    }
}
