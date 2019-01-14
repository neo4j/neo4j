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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.Group;

final class InterruptableThreadFactory implements ThreadFactory
{
    private final Group group;
    private final ThreadGroup threadGroup;
    private final ConcurrentLinkedQueue<Thread> threads;

    InterruptableThreadFactory( Group group, ThreadGroup parentThreadGroup )
    {
        this.group = group;
        this.threadGroup = new ThreadGroup( parentThreadGroup, group.groupName() );
        this.threads = new ConcurrentLinkedQueue<>();
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
        threads.add( thread );
        return thread;
    }

    void shutDown()
    {
        for ( Thread thread : threads )
        {
            thread.interrupt();
        }
    }

    public boolean awaitTermination( long timeout, TimeUnit unit )
    {
        long waitTime = unit.toMillis( timeout );
        long end = System.currentTimeMillis() + waitTime;
        for ( Thread thread : threads )
        {
            try
            {
                long now = System.currentTimeMillis();
                long waitTimeLeft = end - now;
                if ( waitTimeLeft > 0 )
                {
                    thread.join( waitTimeLeft );
                }
                else
                {
                    return false;
                }
            }
            catch ( InterruptedException e )
            {
                // We don't know if tasks are still running.
                // Someone rudely interrupted us.
                return false;
            }
        }
        return true;
    }
}
