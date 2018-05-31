/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.test;

import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class ThreadTestUtils
{
    private ThreadTestUtils()
    {
    }

    public static Thread fork( Runnable runnable )
    {
        String name = "Forked-from-" + Thread.currentThread().getName();
        Thread thread = new Thread( runnable, name );
        thread.setDaemon( true );
        thread.start();
        return thread;
    }

    public static <T> Future<T> forkFuture( Callable<T> callable )
    {
        FutureTask<T> task = new FutureTask<>( callable );
        fork( task );
        return task;
    }

    public static void awaitThreadState( Thread thread, long maxWaitMillis, Thread.State first, Thread.State... rest )
    {
        EnumSet<Thread.State> set = EnumSet.of( first, rest );
        long deadline = maxWaitMillis + System.currentTimeMillis();
        Thread.State currentState;
        do
        {
            currentState = thread.getState();
            if ( System.currentTimeMillis() > deadline )
            {
                throw new AssertionError(
                        "Timed out waiting for thread state of <" +
                                set + ">: " + thread + " (state = " +
                                thread.getState() + ")" );
            }
        }
        while ( !set.contains( currentState ) );
    }
}
