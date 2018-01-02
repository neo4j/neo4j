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
package org.neo4j.helpers;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
    public interface Monitor
    {
        void threadCreated( String threadNamePrefix );

        void threadFinished( String threadNamePrefix );
    }

    public static final Monitor NO_OP_MONITOR = new Monitor()
    {
        @Override
        public void threadCreated( String threadNamePrefix )
        {
        }

        @Override
        public void threadFinished( String threadNamePrefix )
        {
        }
    };

    private static final int DEFAULT_THREAD_PRIORITY = Thread.NORM_PRIORITY;

    private final ThreadGroup group;
    private final AtomicInteger threadCounter = new AtomicInteger( 1 );
    private String threadNamePrefix;
    private final int priority;
    private final boolean daemon;
    private final Monitor monitor;

    public NamedThreadFactory( String threadNamePrefix )
    {
        this( threadNamePrefix, DEFAULT_THREAD_PRIORITY );
    }

    public NamedThreadFactory( String threadNamePrefix, int priority )
    {
        this( threadNamePrefix, priority, NO_OP_MONITOR );
    }

    public NamedThreadFactory( String threadNamePrefix, Monitor monitor )
    {
        this( threadNamePrefix, DEFAULT_THREAD_PRIORITY, monitor );
    }

    public NamedThreadFactory( String threadNamePrefix, int priority, Monitor monitor )
    {
        this( threadNamePrefix, priority, monitor, false );
    }

    public NamedThreadFactory( String threadNamePrefix, int priority, Monitor monitor, boolean daemon )
    {
        this.threadNamePrefix = threadNamePrefix;
        SecurityManager securityManager = System.getSecurityManager();
        group = (securityManager != null) ?
                securityManager.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        this.priority = priority;
        this.daemon = daemon;
        this.monitor = monitor;
    }

    public Thread newThread( Runnable runnable )
    {
        int id = threadCounter.getAndIncrement();

        Thread result = new Thread( group, runnable, threadNamePrefix + "-" + id )
        {
            @Override
            public void run()
            {
                try
                {
                    super.run();
                }
                finally
                {
                    monitor.threadFinished( threadNamePrefix );
                }
            }
        };

        result.setDaemon( daemon );
        result.setPriority( priority );
        monitor.threadCreated( threadNamePrefix );
        return result;
    }

    public static NamedThreadFactory named( String threadNamePrefix )
    {
        return new NamedThreadFactory( threadNamePrefix );
    }

    public static NamedThreadFactory named( String threadNamePrefix, int priority )
    {
        return new NamedThreadFactory( threadNamePrefix, priority );
    }

    public static NamedThreadFactory daemon( String threadNamePrefix )
    {
        return daemon( threadNamePrefix, NO_OP_MONITOR );
    }

    public static NamedThreadFactory daemon( String threadNamePrefix, Monitor monitor )
    {
        return new NamedThreadFactory( threadNamePrefix, DEFAULT_THREAD_PRIORITY, monitor, true );
    }
}
