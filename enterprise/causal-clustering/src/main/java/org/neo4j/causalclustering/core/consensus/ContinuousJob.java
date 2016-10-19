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
package org.neo4j.causalclustering.core.consensus;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Invokes the supplied task continuously when started. The supplied task
 * should be short since the abort flag is checked in between invocations.
 */
public class ContinuousJob extends LifecycleAdapter
{
    private final AbortableJob abortableJob = new AbortableJob();
    private final JobScheduler scheduler;
    private final JobScheduler.Group group;
    private final Runnable task;
    private final Log log;

    private JobScheduler.JobHandle jobHandle;

    public ContinuousJob( JobScheduler scheduler, JobScheduler.Group group, Runnable task, LogProvider logProvider )
    {
        this.scheduler = scheduler;
        this.group = group;
        this.task = task;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        abortableJob.keepRunning = true;
        jobHandle = scheduler.schedule( group, abortableJob );
    }

    @Override
    public void stop() throws Throwable
    {
        log.info( "ContinuousJob " + group.name() + " stopping" );
        abortableJob.keepRunning = false;
        jobHandle.waitTermination();
    }

    private class AbortableJob implements Runnable
    {
        private volatile boolean keepRunning;

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
