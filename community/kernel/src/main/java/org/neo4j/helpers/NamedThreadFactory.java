/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
    private final ThreadGroup group;
    private final AtomicInteger threadCounter = new AtomicInteger( 1 );
    private String threadNamePrefix;

    public NamedThreadFactory( String threadNamePrefix )
    {
        this.threadNamePrefix = threadNamePrefix;
        SecurityManager securityManager = System.getSecurityManager();
        group = (securityManager != null) ?
                securityManager.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
    }

    public Thread newThread( Runnable runnable )
    {
        Thread result = new Thread( group, runnable, threadNamePrefix + "-" + threadCounter.getAndIncrement() );
        result.setDaemon( false );
        result.setPriority( Thread.NORM_PRIORITY );
        return result;
    }
}
