/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    public void start()
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
