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
package org.neo4j.io.pagecache.impl.muninn;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * An executor for the background threads for the page caches.
 *
 * This is similar to an unbounded cached thread pool, except it uses daemon threads.
 *
 * There are only one of these (it's a singleton) to facilitate reusing the threads of closed page caches.
 * This is useful for making tests run faster.
 */
final class BackgroundThreadExecutor implements Executor
{
    static final BackgroundThreadExecutor INSTANCE = new BackgroundThreadExecutor();

    private final Executor executor;

    private BackgroundThreadExecutor()
    {
        executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
    }

    @Override
    public void execute( Runnable command )
    {
        executor.execute( command );
    }

    private static final class DaemonThreadFactory implements ThreadFactory
    {
        @Override
        public Thread newThread( Runnable r )
        {
            ThreadFactory def = Executors.defaultThreadFactory();
            Thread thread = def.newThread( r );
            thread.setDaemon( true );
            return thread;
        }
    }
}
