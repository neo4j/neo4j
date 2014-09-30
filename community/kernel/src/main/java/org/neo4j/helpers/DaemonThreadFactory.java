/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

/**
 * Yes, it's yet another daemon thread factory since Executors doesn't provide one. Sigh.
 */
public class DaemonThreadFactory implements ThreadFactory
{
    private static final AtomicInteger poolNumber = new AtomicInteger( 1 );

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger( 1 );
    private final String namePrefix;

    public DaemonThreadFactory( String name )
    {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = name + "-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public static DaemonThreadFactory daemon( String name )
    {
        return new DaemonThreadFactory( name );
    }

    public Thread newThread( Runnable runnable )
    {
        Thread t = new Thread( group, runnable, namePrefix + threadNumber.getAndIncrement() );
        t.setDaemon( true );
        t.setPriority( Thread.NORM_PRIORITY );
        return t;
    }
}
