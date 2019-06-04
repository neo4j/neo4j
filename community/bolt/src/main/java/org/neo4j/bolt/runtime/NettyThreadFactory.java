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
package org.neo4j.bolt.runtime;

import io.netty.util.concurrent.FastThreadLocalThread;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.SchedulerThreadFactory;

/**
 * Factory for providing {@link FastThreadLocalThread}s for netty to allow faster access and cleanup.
 */
public class NettyThreadFactory implements SchedulerThreadFactory
{
    private final Group group;
    private final ThreadGroup threadGroup;

    public NettyThreadFactory( Group group, ThreadGroup parentThreadGroup )
    {
        this.group = group;
        threadGroup = new ThreadGroup( parentThreadGroup, group.groupName() );
    }

    @Override
    public ForkJoinWorkerThread newThread( ForkJoinPool pool )
    {
        throw new UnsupportedOperationException( "ForkJoinWorkerThread are not supported by netty" );
    }

    @Override
    public Thread newThread( @SuppressWarnings( "NullableProblems" ) Runnable r )
    {
        return new FastThreadLocalThread( threadGroup, r, group.threadName() )
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
    }
}
