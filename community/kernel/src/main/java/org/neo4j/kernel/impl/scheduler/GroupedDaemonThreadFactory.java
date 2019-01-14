/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.scheduler.JobScheduler;

final class GroupedDaemonThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory
{
    private final JobScheduler.Group group;
    private final ThreadGroup threadGroup;

    GroupedDaemonThreadFactory( JobScheduler.Group group, ThreadGroup parentThreadGroup )
    {
        this.group = group;
        threadGroup = new ThreadGroup( parentThreadGroup, group.name() );
    }

    @Override
    public Thread newThread( @SuppressWarnings( "NullableProblems" ) Runnable job )
    {
        Thread thread = new Thread( threadGroup, job, group.threadName() )
        {
            @Override
            public String toString()
            {
                StringBuilder sb = new StringBuilder( "Thread[" ).append( getName() );
                ThreadGroup group = getThreadGroup();
                String sep = ", in ";
                while ( group != null )
                {
                    sb.append( sep ).append( group.getName() );
                    group = group.getParent();
                    sep = "/";
                }
                return sb.append( ']' ).toString();
            }
        };
        thread.setDaemon( true );
        return thread;
    }

    @Override
    public ForkJoinWorkerThread newThread( ForkJoinPool pool )
    {
        // We do this complicated dance of allocating the ForkJoinThread in a separate thread,
        // because there is no way to give it a specific ThreadGroup, other than through inheritance
        // from the allocating thread.
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
        AtomicReference<ForkJoinWorkerThread> reference = new AtomicReference<>();
        Thread allocator = newThread( () -> reference.set( factory.newThread( pool ) ) );
        allocator.start();
        do
        {
            try
            {
                allocator.join();
            }
            catch ( InterruptedException ignore )
            {
            }
        }
        while ( reference.get() == null );
        ForkJoinWorkerThread worker = reference.get();
        worker.setName( group.threadName() );
        return worker;
    }
}
